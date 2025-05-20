import os
import pandas as pd
import psycopg2
from glob import glob

conn = psycopg2.connect(
    dbname="bigdataspark",
    user="user",
    password="password",
    host="localhost",
    port=5432
)
cursor = conn.cursor()

data_dir = "data/"  
csv_files = sorted(glob(os.path.join(data_dir, "MOCK_DATA*.csv")))

columns = [
    "id", "customer_first_name", "customer_last_name", "customer_age", "customer_email",
    "customer_country", "customer_postal_code", "customer_pet_type", "customer_pet_name",
    "customer_pet_breed", "seller_first_name", "seller_last_name", "seller_email",
    "seller_country", "seller_postal_code", "product_name", "product_category",
    "product_price", "product_quantity", "sale_date", "sale_customer_id",
    "sale_seller_id", "sale_product_id", "sale_quantity", "sale_total_price",
    "store_name", "store_location", "store_city", "store_state", "store_country",
    "store_phone", "store_email", "pet_category", "product_weight", "product_color",
    "product_size", "product_brand", "product_material", "product_description",
    "product_rating", "product_reviews", "product_release_date", "product_expiry_date",
    "supplier_name", "supplier_contact", "supplier_email", "supplier_phone",
    "supplier_address", "supplier_city", "supplier_country"
]
print(csv_files)
for file in csv_files:
    print(f"Загружается: {file}")
    df = pd.read_csv(file)
    df = df[columns]

    for _, row in df.iterrows():
        cursor.execute(f"""
            INSERT INTO data_raw ({', '.join(columns)})
            VALUES ({', '.join(['%s'] * len(columns))})
        """, tuple(row))

    conn.commit()

cursor.close()
conn.close()
print("Данные загружены в таблицу data_raw.")

