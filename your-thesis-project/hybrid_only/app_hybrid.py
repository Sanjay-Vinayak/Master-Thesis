from flask import Flask, jsonify
import psycopg2
from pymongo import MongoClient
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

POSTGRES_HOST = 'localhost'
POSTGRES_PORT = '5433'
POSTGRES_DB = 'ecom_hybrid_db'
POSTGRES_USER = 'user'
POSTGRES_PASSWORD = 'password'

MONGO_HOST = 'localhost'
MONGO_PORT = 27017

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

@app.route('/')
def home():
    return "E-commerce Hybrid Backend is running!"

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

@app.route('/products', methods=['GET'])
def get_products():
    client = get_mongo_client()
    if not client: return jsonify({"error": "Failed to connect to MongoDB"}), 500
    try:
        db = client.ecom_hybrid_db
        products = list(db.products.find({}, {"_id": 0}))
        return jsonify(products)
    except Exception as e:
        print(f"Error fetching products: {e}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if client: client.close()

@app.route('/reviews', methods=['GET'])
def get_reviews():
    client = get_mongo_client()
    if not client: return jsonify({"error": "Failed to connect to MongoDB"}), 500
    try:
        db = client.ecom_hybrid_db
        reviews = list(db.reviews.find({}, {"_id": 0}))
        return jsonify(reviews)
    except Exception as e:
        print(f"Error fetching reviews: {e}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if client: client.close()

@app.route('/user_profiles', methods=['GET'])
def get_user_profiles():
    client = get_mongo_client()
    if not client: return jsonify({"error": "Failed to connect to MongoDB"}), 500
    try:
        db = client.ecom_hybrid_db
        user_profiles = list(db.user_profiles.find({}, {"_id": 0}))
        return jsonify(user_profiles)
    except Exception as e:
        print(f"Error fetching user profiles: {e}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if client: client.close()

if __name__ == '__main__':
    app.run(debug=True, port=5000, host='0.0.0.0')
