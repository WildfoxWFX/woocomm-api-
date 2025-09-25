import requests
import pandas as pd
from sqlalchemy import create_engine
from requests.auth import HTTPBasicAuth
import os
import time

# ----------------- CONFIG -----------------
DB_SERVER = "SWETHA\\SQLEXPRESS"
DB_DATABASE = "woocomm"
DB_USERNAME = "wildfox"
DB_PASSWORD = "wfx@123"
TABLE_NAME = "wooorderline"

WC_API_URL = "https://samandmarshalleyewear.in/wp-json/wc/v3/orders"
CONSUMER_KEY = "ck_9707ba22a218ccb2b0070be2014bed6f2329406b"
CONSUMER_SECRET = "cs_c3d8d8afd56d31226ec33d03bf8fc39492758fb8"

CHECKPOINT_FILE = "woo_line_checkpoint.txt"  # stores last completed page
BATCH_SIZE = 3000                             # pages per batch
PER_PAGE = 100                                # WooCommerce API max per_page

# ----------------- FUNCTION TO FETCH ORDERS -----------------
def fetch_orders(page=1, per_page=100):
    params = {"page": page, "per_page": per_page}
    r = requests.get(WC_API_URL, params=params,
                     auth=HTTPBasicAuth(CONSUMER_KEY, CONSUMER_SECRET))
    r.raise_for_status()
    return r.json(), r.headers

# ----------------- FUNCTION TO PROCESS LINE ITEMS -----------------
def process_line_items(orders):
    rows = []
    for order in orders:
        order_id = order.get("id")
        for li in order.get("line_items", []):
            rows.append({
                "order_id": order_id,
                "line_item_id": li.get("id"),
                "product_id": li.get("product_id"),
                "variation_id": li.get("variation_id"),
                "product_name": li.get("name"),
                "quantity": li.get("quantity"),
                "subtotal": li.get("subtotal"),
                "subtotal_tax": li.get("subtotal_tax"),
                "total": li.get("total"),
                "total_tax": li.get("total_tax")
            })
    return pd.DataFrame(rows)

# ----------------- CONNECT TO MSSQL -----------------
connection_string = (
    "mssql+pyodbc:///?odbc_connect="
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={DB_SERVER};"
    f"DATABASE={DB_DATABASE};"
    f"UID={DB_USERNAME};"
    f"PWD={DB_PASSWORD};"
    "TrustServerCertificate=yes;"
)
engine = create_engine(connection_string)

# ----------------- MAIN SCRIPT -----------------
# Get total pages from API
_, headers = fetch_orders(1)
total_pages = int(headers.get("x-wp-totalpages", 1))
print(f"Total pages to fetch: {total_pages}")

# Resume from checkpoint if available
if os.path.exists(CHECKPOINT_FILE):
    with open(CHECKPOINT_FILE, "r") as f:
        start_page = int(f.read().strip())
else:
    start_page = 1

print(f"Resuming from page {start_page}")

# Loop in batches
for batch_start in range(start_page, total_pages + 1, BATCH_SIZE):
    batch_end = min(batch_start + BATCH_SIZE - 1, total_pages)
    print(f"\n🚀 Processing batch {batch_start} → {batch_end}")

    for page in range(batch_start, batch_end + 1):
        try:
            orders, _ = fetch_orders(page, PER_PAGE)
            df = process_line_items(orders)

            if not df.empty:
                df.to_sql(TABLE_NAME, engine, if_exists="append", index=False)
                print(f"✅ Inserted page {page} with {len(df)} line items")

            # Update checkpoint
            with open(CHECKPOINT_FILE, "w") as f:
                f.write(str(page))

            time.sleep(0.5)  # avoid overloading server

        except Exception as e:
            print(f"❌ Error on page {page}: {e}")
            print("Stopping here. You can rerun to resume.")
            exit(1)

print("\n🎉 All line items inserted successfully into SQL Server")
