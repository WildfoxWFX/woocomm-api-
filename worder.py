import requests
import pandas as pd
from sqlalchemy import create_engine
from requests.auth import HTTPBasicAuth
import os

# ----------------- CONFIG -----------------
DB_SERVER = "SWETHA\\SQLEXPRESS"
DB_DATABASE = "woocomm"
DB_USERNAME = "wildfox"
DB_PASSWORD = "wfx@123"
TABLE_NAME = "wooorder"

WC_API_URL = "https://samandmarshalleyewear.in/wp-json/wc/v3/orders"
CONSUMER_KEY = "ck_9707ba22a218ccb2b0070be2014bed6f2329406b"
CONSUMER_SECRET = "cs_c3d8d8afd56d31226ec33d03bf8fc39492758fb8"

CHECKPOINT_FILE = "woo_checkpoint.txt"   # stores last completed page
BATCH_SIZE = 3000                        # number of pages per batch
PER_PAGE = 100                           # WooCommerce API max

# ----------------- FUNCTION TO FETCH ORDERS -----------------
def fetch_orders(page=1, per_page=100):
    params = {"page": page, "per_page": per_page}
    r = requests.get(WC_API_URL, params=params,
                     auth=HTTPBasicAuth(CONSUMER_KEY, CONSUMER_SECRET))
    r.raise_for_status()
    return r.json(), r.headers

# ----------------- FUNCTION TO PROCESS ORDERS -----------------
def process_orders(orders):
    rows = []
    for order in orders:
        order_id = order.get("id")
        date_created = order.get("date_created")
        date_modified = order.get("date_modified")
        billing = order.get("billing", {})
        first_name = billing.get("first_name")
        last_name = billing.get("last_name")

        is_editable = order.get("is_editable")
        needs_payment = order.get("needs_payment")
        needs_processing = order.get("needs_processing")
        date_created_gmt = order.get("date_created_gmt")
        date_modified_gmt = order.get("date_modified_gmt")
        date_completed_gmt = order.get("date_completed_gmt")

        # Line items (one row per product)
        for li in order.get("line_items", []):
            rows.append({
                "order_id": order_id,
                "date_created": date_created,
                "date_modified": date_modified,
                "billing_first_name": first_name,
                "billing_last_name": last_name,
                "product_id": li.get("product_id"),
                "quantity": li.get("quantity"),
                "price": li.get("price"),
                "line_total": li.get("total"),
                "is_editable": is_editable,
                "needs_payment": needs_payment,
                "needs_processing": needs_processing,
                "date_created_gmt": date_created_gmt,
                "date_modified_gmt": date_modified_gmt,
                "date_completed_gmt": date_completed_gmt
            })
    return pd.DataFrame(rows)

# ----------------- CONNECT TO MSSQL -----------------
# Use ODBC connection string (avoids issues with backslashes)
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
            df = process_orders(orders)

            if not df.empty:
                df.to_sql(TABLE_NAME, engine, if_exists="append", index=False)
                print(f"✅ Inserted page {page} with {len(df)} rows")

            # Update checkpoint after successful page insert
            with open(CHECKPOINT_FILE, "w") as f:
                f.write(str(page))

        except Exception as e:
            print(f"❌ Error on page {page}: {e}")
            print("Stopping here. You can rerun to resume.")
            exit(1)

print("\n🎉 All orders inserted successfully into SQL Server")
