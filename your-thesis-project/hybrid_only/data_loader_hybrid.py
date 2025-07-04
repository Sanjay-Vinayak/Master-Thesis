import psycopg2
from pymongo import MongoClient
import pandas as pd
import numpy as np
import time
import os

POSTGRES_HOST = 'localhost'
POSTGRES_PORT = '5433'
POSTGRES_DB = 'ecom_hybrid_db'
POSTGRES_USER = 'user'
POSTGRES_PASSWORD = 'password'

MONGO_HOST = 'localhost'
MONGO_PORT = 27017

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OLIST_DATA_PATH = os.path.join(BASE_DIR, '..', 'data')

def get_postgres_connection():
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST, port=POSTGRES_PORT,
            database=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD
        )
        return conn
    except Exception as e:
        print(f"PostgreSQL connection error: {e}")
        return None

def get_mongo_client():
    try:
        client = MongoClient(f'mongodb://{MONGO_HOST}:{MONGO_PORT}/')
        client.admin.command('ismaster')
        return client
    except Exception as e:
        print(f"MongoDB connection error: {e}")
        return None

def create_postgres_tables(cursor):
    cursor.execute("DROP TABLE IF EXISTS order_items CASCADE; DROP TABLE IF EXISTS orders CASCADE; DROP TABLE IF EXISTS customers CASCADE;")
    cursor.execute("""
        CREATE TABLE customers (
            customer_id VARCHAR(50) PRIMARY KEY, customer_unique_id VARCHAR(50) NOT NULL,
            customer_zip_code_prefix VARCHAR(10), customer_city VARCHAR(100), customer_state VARCHAR(50)
        );
    """)
    cursor.execute("""
        CREATE TABLE orders (
            order_id VARCHAR(50) PRIMARY KEY, customer_id VARCHAR(50) REFERENCES customers(customer_id),
            order_status VARCHAR(50), order_purchase_timestamp TIMESTAMP, order_approved_at TIMESTAMP,
            order_delivered_carrier_date TIMESTAMP, order_delivered_customer_date TIMESTAMP,
            order_estimated_delivery_date TIMESTAMP
        );
    """)
    cursor.execute("""
        CREATE TABLE order_items (
            order_item_id SERIAL PRIMARY KEY, order_id VARCHAR(50) REFERENCES orders(order_id),
            product_id VARCHAR(50), seller_id VARCHAR(50), shipping_limit_date TIMESTAMP,
            price DECIMAL(10, 2), freight_value DECIMAL(10, 2)
        );
    """)
    print("PostgreSQL tables recreated.")

def load_postgres_data(conn):
    cursor = conn.cursor()
    try:
        customers_df = pd.read_csv(os.path.join(OLIST_DATA_PATH, 'olist_customers_dataset.csv')).replace({np.nan: None})
        print(f"Loading {len(customers_df)} customers...")
        for index, row in customers_df.iterrows():
            cursor.execute(
                "INSERT INTO customers (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (customer_id) DO NOTHING;",
                (row['customer_id'], row['customer_unique_id'], row['customer_zip_code_prefix'], row['customer_city'], row['customer_state'])
            )
        print("Customers loaded.")

        orders_df = pd.read_csv(os.path.join(OLIST_DATA_PATH, 'olist_orders_dataset.csv'))
        timestamp_cols = ['order_purchase_timestamp', 'order_approved_at', 'order_delivered_carrier_date', 'order_delivered_customer_date', 'order_estimated_delivery_date']
        for col in timestamp_cols: orders_df[col] = pd.to_datetime(orders_df[col], errors='coerce')
        orders_df = orders_df.replace({np.nan: None})
        print(f"Loading {len(orders_df)} orders...")
        for index, row in orders_df.iterrows():
            cursor.execute(
                """INSERT INTO orders (order_id, customer_id, order_status, order_purchase_timestamp, 
                                    order_approved_at, order_delivered_carrier_date, 
                                    order_delivered_customer_date, order_estimated_delivery_date) 
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (order_id) DO NOTHING;""",
                (row['order_id'], row['customer_id'], row['order_status'], row['order_purchase_timestamp'],
                 row['order_approved_at'], row['order_delivered_carrier_date'],
                 row['order_delivered_customer_date'], row['order_estimated_delivery_date'])
            )
        print("Orders loaded.")

        order_items_df = pd.read_csv(os.path.join(OLIST_DATA_PATH, 'olist_order_items_dataset.csv'))
        order_items_df['shipping_limit_date'] = pd.to_datetime(order_items_df['shipping_limit_date'], errors='coerce')
        order_items_df = order_items_df.replace({np.nan: None})
        print(f"Loading {len(order_items_df)} order items...")
        items_to_insert = [(row['order_id'], row['product_id'], row['seller_id'], row['shipping_limit_date'], row['price'], row['freight_value']) for index, row in order_items_df.iterrows()]
        if items_to_insert: cursor.executemany("""INSERT INTO order_items (order_id, product_id, seller_id, shipping_limit_date, price, freight_value) VALUES (%s, %s, %s, %s, %s, %s);""", items_to_insert)
        print("Order items loaded.")
        conn.commit()
    except FileNotFoundError as e: print(f"Error: Olist CSV file not found. Ensure CSVs are in '{OLIST_DATA_PATH}'. {e}"); conn.rollback()
    except Exception as e: print(f"Error loading PostgreSQL data: {e}"); conn.rollback()
    finally: cursor.close()

def load_mongodb_data(mongo_client):
    db = mongo_client.ecom_hybrid_db
    db.products.drop(); db.reviews.drop(); db.user_profiles.drop()
    print("MongoDB collections dropped.")

    try:
        products_df = pd.read_csv(os.path.join(OLIST_DATA_PATH, 'olist_products_dataset.csv')).replace({np.nan: None})
        products_data = []
        for index, row in products_df.iterrows():
            product_doc = {
                "product_id": row['product_id'],
                "product_category_name": row['product_category_name'],
                # CORRECTED: Changed 'length' to 'lenght'
                "product_name_length": row['product_name_lenght'],
                "product_description_length": row['product_description_lenght'],
                "product_photos_qty": row['product_photos_qty'],
                "product_weight_g": row['product_weight_g'],
                "product_length_cm": row['product_length_cm'],
                "product_height_cm": row['product_height_cm'],
                "product_width_cm": row['product_width_cm'],
                "specs": {
                    "weight_g": row['product_weight_g'],
                    "dimensions_cm": {
                        "length": row['product_length_cm'],
                        "height": row['product_height_cm'],
                        "width": row['product_width_cm']
                    }
                },
                "tags": [row['product_category_name']] if row['product_category_name'] else ["unknown"]
            }
            products_data.append(product_doc)
        if products_data: print(f"Loading {len(products_data)} products..."); db.products.insert_many(products_data); print("Products loaded.")
        else: print("No product data.")

        reviews_df = pd.read_csv(os.path.join(OLIST_DATA_PATH, 'olist_order_reviews_dataset.csv'))
        reviews_df['review_creation_date'] = pd.to_datetime(reviews_df['review_creation_date'], errors='coerce')
        reviews_df['review_answer_timestamp'] = pd.to_datetime(reviews_df['review_answer_timestamp'], errors='coerce')
        reviews_df = reviews_df.replace({np.nan: None})
        reviews_data = reviews_df.to_dict(orient='records')
        if reviews_data: print(f"Loading {len(reviews_data)} reviews..."); db.reviews.insert_many(reviews_data); print("Reviews loaded.")
        else: print("No review data.")

        customers_df = pd.read_csv(os.path.join(OLIST_DATA_PATH, 'olist_customers_dataset.csv'))
        unique_customer_ids = customers_df['customer_id'].unique()
        user_profiles_data = [{"customer_id": cust_id, "preferences": {"newsletter": False, "notifications": True}, "last_activity": None} for cust_id in unique_customer_ids]
        if user_profiles_data: print(f"Loading {len(user_profiles_data)} user profiles..."); db.user_profiles.insert_many(user_profiles_data); print("User profiles loaded.")
        else: print("No user profiles.")

    except FileNotFoundError as e: print(f"Error: Olist CSV file not found. Ensure CSVs are in '{OLIST_DATA_PATH}'. {e}")
    except Exception as e: print(f"Error loading MongoDB data: {e}")

if __name__ == "__main__":
    postgres_conn = None
    mongo_client = None
    max_retries = 15 
    retry_delay = 5 

    for i in range(max_retries):
        print(f"Attempting to connect to PostgreSQL (attempt {i+1}/{max_retries})...")
        postgres_conn = get_postgres_connection()
        if postgres_conn:
            print("PostgreSQL connection successful!")
            break
        time.sleep(retry_delay)
    
    for i in range(max_retries):
        print(f"Attempting to connect to MongoDB (attempt {i+1}/{max_retries})...")
        mongo_client = get_mongo_client()
        if mongo_client:
            print("MongoDB connection successful!")
            break
        time.sleep(retry_delay)

    if postgres_conn and mongo_client:
        try:
            print("--- Starting Olist Data Loading ---")
            cursor = postgres_conn.cursor()
            create_postgres_tables(cursor)
            load_postgres_data(postgres_conn)
            load_mongodb_data(mongo_client)
            print("--- Olist Data Loading Completed ---")
        except Exception as e: print(f"An error occurred during Olist data loading: {e}")
        finally:
            if postgres_conn: postgres_conn.close(); print("PostgreSQL connection closed.")
            if mongo_client: mongo_client.close(); print("MongoDB connection closed.")
    else: print("Failed to connect to one or both databases. Data loading aborted.")
