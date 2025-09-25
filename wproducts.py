import requests
import pandas as pd
from sqlalchemy import create_engine
from requests.auth import HTTPBasicAuth
import os
from datetime import datetime

# ----------------- CONFIG -----------------
DB_SERVER = "SWETHA\\SQLEXPRESS"
DB_DATABASE = "woocomm"
DB_USERNAME = "wildfox"
DB_PASSWORD = "wfx@123"
TABLE_NAME = "wooproduct"

WC_API_URL = "https://samandmarshalleyewear.in/wp-json/wc/v3/products"
CONSUMER_KEY = "ck_9707ba22a218ccb2b0070be2014bed6f2329406b"
CONSUMER_SECRET = "cs_c3d8d8afd56d31226ec33d03bf8fc39492758fb8"

CHECKPOINT_FILE = "product_checkpoint.txt"  # stores last completed page
BATCH_SIZE = 5        # number of pages per batch
PER_PAGE = 100        # WooCommerce API max

# ----------------- HELPER FUNCTIONS -----------------
def parse_datetime(date_string):
    if not date_string:
        return None
    try:
        return datetime.fromisoformat(date_string.replace('T', ' ').replace('Z', ''))
    except:
        return None

def safe_decimal(value):
    if not value or value == "":
        return None
    try:
        return float(value)
    except:
        return None

# ----------------- FETCH PRODUCTS -----------------
def fetch_products(page=1, per_page=100):
    params = {"page": page, "per_page": per_page}
    r = requests.get(WC_API_URL, params=params,
                     auth=HTTPBasicAuth(CONSUMER_KEY, CONSUMER_SECRET), timeout=30)
    r.raise_for_status()
    return r.json(), r.headers

# ----------------- PROCESS PRODUCTS -----------------
def process_products(products):
    rows = []
    for p in products:
        rows.append({
            "id": p.get("id"),
            "name": p.get("name"),
            "date_created": parse_datetime(p.get("date_created")),
            "date_created_gmt": parse_datetime(p.get("date_created_gmt")),
            "date_modified": parse_datetime(p.get("date_modified")),
            "date_modified_gmt": parse_datetime(p.get("date_modified_gmt")),
            "type": p.get("type"),
            "sku": p.get("sku"),
            "price": safe_decimal(p.get("price")),
            "regular_price": safe_decimal(p.get("regular_price")),
            "sale_price": safe_decimal(p.get("sale_price")),
            "shipping_required": p.get("shipping_required"),
            "weight": p.get("weight", ""),
            "shipping_taxable": p.get("shipping_taxable"),
            "shipping_class": p.get("shipping_class", ""),
            "shipping_class_id": p.get("shipping_class_id"),
            "parent_id": p.get("parent_id")
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
_, headers = fetch_products(1)
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
            products, _ = fetch_products(page, PER_PAGE)
            df = process_products(products)

            if not df.empty:
                df.to_sql(TABLE_NAME, engine, if_exists="append", index=False)
                print(f"✅ Inserted page {page} with {len(df)} rows")

            # Update checkpoint
            with open(CHECKPOINT_FILE, "w") as f:
                f.write(str(page))

        except Exception as e:
            print(f"❌ Error on page {page}: {e}")
            print("Stopping here. You can rerun to resume.")
            exit(1)

print("\n🎉 All products inserted successfully into SQL Server")
