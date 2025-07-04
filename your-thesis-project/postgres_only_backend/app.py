from flask import Flask, jsonify
import psycopg2
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

POSTGRES_HOST = 'localhost'
POSTGRES_PORT = '5433'
POSTGRES_DB = 'ecom_only_db' # Matches new database name
POSTGRES_USER = 'user'
POSTGRES_PASSWORD = 'password'

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

@app.route('/')
def home():
    return "E-commerce PostgreSQL Only Backend is running!"

@app.route('/customers', methods=['GET'])
def get_customers():
    conn = get_postgres_connection()
    if not conn: return jsonify({"error": "Failed to connect to PostgreSQL"}), 500
    try:
        cur = conn.cursor()
        cur.execute("SELECT customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state FROM customers;")
        customers = cur.fetchall()
        cur.close()
        return jsonify([{"customer_id": c[0], "customer_unique_id": c[1], "customer_zip_code_prefix": c[2], "customer_city": c[3], "customer_state": c[4]} for c in customers])
    except Exception as e:
        print(f"Error fetching customers: {e}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if conn: conn.close()

@app.route('/orders', methods=['GET'])
def get_orders():
    conn = get_postgres_connection()
    if not conn: return jsonify({"error": "Failed to connect to PostgreSQL"}), 500
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT 
                order_id, customer_id, order_status, 
                order_purchase_timestamp, order_approved_at, 
                order_delivered_carrier_date, order_delivered_customer_date, 
                order_estimated_delivery_date 
            FROM orders;
        """)
        orders = cur.fetchall()
        cur.close()
        
        return jsonify([
            {
                "order_id": o[0], 
                "customer_id": o[1], 
                "order_status": o[2], 
                "order_purchase_timestamp": o[3].isoformat() if o[3] else None, 
                "order_approved_at": o[4].isoformat() if o[4] else None,
                "order_delivered_carrier_date": o[5].isoformat() if o[5] else None,
                "order_delivered_customer_date": o[6].isoformat() if o[6] else None,
                "order_estimated_delivery_date": o[7].isoformat() if o[7] else None
            } 
            for o in orders
        ])
    except Exception as e:
        print(f"Error fetching orders: {e}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if conn: conn.close()

@app.route('/order_items', methods=['GET'])
def get_order_items():
    conn = get_postgres_connection()
    if not conn: return jsonify({"error": "Failed to connect to PostgreSQL"}), 500
    try:
        cur = conn.cursor()
        cur.execute("SELECT order_item_id, order_id, product_id, seller_id, shipping_limit_date, price, freight_value FROM order_items;")
        order_items = cur.fetchall()
        cur.close()
        return jsonify([
            {
                "order_item_id": oi[0], "order_id": oi[1], "product_id": oi[2],
                "seller_id": oi[3], "shipping_limit_date": oi[4].isoformat() if oi[4] else None,
                "price": str(oi[5]), "freight_value": str(oi[6])
            }
            for oi in order_items
        ])
    except Exception as e:
        print(f"Error fetching order items: {e}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if conn: conn.close()


if __name__ == '__main__':
    app.run(debug=True, port=5000, host='0.0.0.0')
