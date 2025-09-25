import requests
import pandas as pd
from sqlalchemy import create_engine, text
import urllib

# ========== CONFIG ==========
DB_SERVER = "SWETHA\\SQLEXPRESS"
DB_DATABASE = "woocomm"
DB_USERNAME = "wildfox"
DB_PASSWORD = "wfx@123"
TABLE_NAME = "wooproductreview"

# WooCommerce API Credentials
WC_API_URL = "https://samandmarshalleyewear.in/wp-json/wc/v3/products/reviews"
WC_CONSUMER_KEY = "ck_9707ba22a218ccb2b0070be2014bed6f2329406b"
WC_CONSUMER_SECRET = "cs_c3d8d8afd56d31226ec33d03bf8fc39492758fb8"


# ---------- Fetch all reviews ----------
def fetch_reviews():
    reviews = []
    page = 1

    while True:
        response = requests.get(
            WC_API_URL,
            params={
                "consumer_key": WC_CONSUMER_KEY,
                "consumer_secret": WC_CONSUMER_SECRET,
                "per_page": 100,  # Woo max = 100
                "page": page
            }
        )

        if response.status_code != 200:
            print("❌ Error:", response.text)
            break

        data = response.json()

        if not data:  # No more pages
            break

        for review in data:
            reviews.append({
                "id": review["id"],
                "date_created": review["date_created"],
                "date_created_gmt": review["date_created_gmt"],
                "product_id": review["product_id"],
                "product_name": review.get("product_name", None),
                "reviewer": review["reviewer"],
                "rating": review["rating"],
                "verified": review["verified"]
            })

        print(f"✅ Fetched page {page}, got {len(data)} reviews")
        page += 1

    return reviews


# ---------- Store into SQL Server ----------
def save_to_sql(reviews):
    df = pd.DataFrame(reviews)

    if df.empty:
        print("⚡ No reviews fetched, skipping insert.")
        return

    # SQLAlchemy connection string
    params = urllib.parse.quote_plus(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={DB_SERVER};"
        f"DATABASE={DB_DATABASE};"
        f"UID={DB_USERNAME};"
        f"PWD={DB_PASSWORD}"
    )
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

    with engine.begin() as conn:
        # ✅ Create table if not exists
        conn.execute(text(f"""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{TABLE_NAME}' AND xtype='U')
            CREATE TABLE {TABLE_NAME} (
                id BIGINT PRIMARY KEY,
                date_created NVARCHAR(50),
                date_created_gmt NVARCHAR(50),
                product_id BIGINT,
                product_name NVARCHAR(255),
                reviewer NVARCHAR(255),
                rating INT,
                verified BIT
            )
        """))

        # ✅ Get all existing IDs
        existing_ids = pd.read_sql(text(f"SELECT id FROM {TABLE_NAME}"), conn)
        existing_ids_set = set(existing_ids["id"].tolist())

        # ✅ Keep only new reviews
        df_new = df[~df["id"].isin(existing_ids_set)]

        # ✅ Insert only new data
        if not df_new.empty:
            df_new.to_sql(TABLE_NAME, con=conn, if_exists="append", index=False)
            print(f"✅ Inserted {len(df_new)} new reviews into {TABLE_NAME}")
        else:
            print("⚡ No new reviews to insert.")


# ---------- MAIN ----------
if __name__ == "__main__":
    all_reviews = fetch_reviews()
    save_to_sql(all_reviews)
