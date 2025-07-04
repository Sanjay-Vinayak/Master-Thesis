import psycopg2
import pandas as pd
import numpy as np
import time
import os

POSTGRES_HOST = 'localhost'
POSTGRES_PORT = '5433'
POSTGRES_DB = 'ecom_only_db' # Matches new database name in docker-compose
POSTGRES_USER = 'user'
POSTGRES_PASSWORD = 'password'

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OLIST_DATA_PATH = os.path.join(BASE_DIR, '..', 'data') # Same Olist data path

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

def create_postgres_tables(cursor):
    cursor.execute("""
        DROP TABLE IF EXISTS order_items CASCADE;
        DROP TABLE IF EXISTS orders CASCADE;
        DROP TABLE IF EXISTS customers CASCADE;
    """) # Drop existing tables to ensure clean reload of Olist data

    cursor.execute("""
        CREATE TABLE customers (
            customer_id VARCHAR(50) PRIMARY KEY,
            customer_unique_id VARCHAR(50) NOT NULL,
            customer_zip_code_prefix VARCHAR(10),
            customer_city VARCHAR(100),
            customer_state VARCHAR(50)
        );
    """)
    cursor.execute("""
        CREATE TABLE orders (
            order_id VARCHAR(50) PRIMARY KEY,
            customer_id VARCHAR(50) REFERENCES customers(customer_id),
            order_status VARCHAR(50),
            order_purchase_timestamp TIMESTAMP,
            order_approved_at TIMESTAMP,
            order_delivered_carrier_date TIMESTAMP,
            order_delivered_customer_date TIMESTAMP,
            order_estimated_delivery_date TIMESTAMP
        );
    """)
    cursor.execute("""
        CREATE TABLE order_items (
            order_item_id SERIAL PRIMARY KEY,
            order_id VARCHAR(50) REFERENCES orders(order_id),
            product_id VARCHAR(50), # Storing product_id in PG for joins, even if products aren't detailed here
            seller_id VARCHAR(50),
            shipping_limit_date TIMESTAMP,
            price DECIMAL(10, 2),
            freight_value DECIMAL(10, 2)
        );
    """)
    print("PostgreSQL tables dropped and recreated for Olist data.")

def load_postgres_data(conn):
    cursor = conn.cursor()

    try:
        customers_df = pd.read_csv(os.path.join(OLIST_DATA_PATH, 'olist_customers_dataset.csv')).replace({np.nan: None})
        print(f"Loading {len(customers_df)} customers into PostgreSQL...")
        for index, row in customers_df.iterrows():
            try:
                cursor.execute(
                    "INSERT INTO customers (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (customer_id) DO NOTHING;",
                    (row['customer_id'], row['customer_unique_id'], row['customer_zip_code_prefix'], row['customer_city'], row['customer_state'])
                )
            except Exception as e:
                print(f"Error inserting customer {row['customer_id']}: {e}")
        print("Customers loaded into PostgreSQL.")

        orders_df = pd.read_csv(os.path.join(OLIST_DATA_PATH, 'olist_orders_dataset.csv'))
        timestamp_cols = ['order_purchase_timestamp', 'order_approved_at', 
                          'order_delivered_carrier_date', 'order_delivered_customer_date', 
                          'order_estimated_delivery_date']
        for col in timestamp_cols:
            orders_df[col] = pd.to_datetime(orders_df[col], errors='coerce')
        orders_df = orders_df.replace({np.nan: None})

        print(f"Loading {len(orders_df)} orders into PostgreSQL...")
        for index, row in orders_df.iterrows():
            try:
                cursor.execute(
                    """INSERT INTO orders (order_id, customer_id, order_status, order_purchase_timestamp, 
                                        order_approved_at, order_delivered_carrier_date, 
                                        order_delivered_customer_date, order_estimated_delivery_date) 
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (order_id) DO NOTHING;""",
                    (row['order_id'], row['customer_id'], row['order_status'], row['order_purchase_timestamp'],
                     row['order_approved_at'], row['order_delivered_carrier_date'],
                     row['order_delivered_customer_date'], row['order_estimated_delivery_date'])
                )
            except Exception as e:
                print(f"Error inserting order {row['order_id']}: {e}")
        print("Orders loaded into PostgreSQL.")

        order_items_df = pd.read_csv(os.path.join(OLIST_DATA_PATH, 'olist_order_items_dataset.csv'))
        order_items_df['shipping_limit_date'] = pd.to_datetime(order_items_df['shipping_limit_date'], errors='coerce')
        order_items_df = order_items_df.replace({np.nan: None})

        print(f"Loading {len(order_items_df)} order items into PostgreSQL...")
        items_to_insert = [
            (row['order_id'], row['product_id'], row['seller_id'], row['shipping_limit_date'], row['price'], row['freight_value'])
            for index, row in order_items_df.iterrows()
        ]
        if items_to_insert:
            try:
                cursor.executemany(
                    """INSERT INTO order_items (order_id, product_id, seller_id, shipping_limit_date, price, freight_value) 
                       VALUES (%s, %s, %s, %s, %s, %s);""",
                    items_to_insert
                )
            except Exception as e:
                print(f"Error inserting order items: {e}")
        print("Order items loaded into PostgreSQL.")

        conn.commit()
    except FileNotFoundError as e:
        print(f"Error: Olist CSV file not found. Please ensure all Olist CSVs are in '{OLIST_DATA_PATH}'. Error: {e}")
        conn.rollback()
    except Exception as e:
        conn.rollback()
        print(f"Error loading PostgreSQL data: {e}")
    finally:
        cursor.close()

if __name__ == "__main__":
    postgres_conn = None
    max_retries = 15
    retry_delay = 5

    for i in range(max_retries):
        print(f"Attempting to connect to PostgreSQL (attempt {i+1}/{max_retries})...")
        postgres_conn = get_postgres_connection()
        if postgres_conn:
            print("PostgreSQL connection successful!")
            break
        time.sleep(retry_delay)

    if postgres_conn:
        try:
            print("\n--- Starting Olist Data Loading (PostgreSQL Only) ---")
            cursor = postgres_conn.cursor()
            create_postgres_tables(cursor)
            load_postgres_data(postgres_conn)
            print("\n--- Olist Data Loading Completed (PostgreSQL Only) ---")
        except Exception as e:
            print(f"An error occurred during Olist data loading: {e}")
        finally:
            if postgres_conn:
                postgres_conn.close()
                print("PostgreSQL connection closed.")
    else:
        print("Failed to connect to PostgreSQL. Data loading aborted.")
