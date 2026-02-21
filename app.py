import os
from flask import Flask, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")

def get_connection():
    return psycopg2.connect(DATABASE_URL)

def init_db():
    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            source TEXT,
            url TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        conn.commit()
        cur.close()
        conn.close()
        print("Database initialized successfully.")
    except Exception as e:
        print("Database initialization failed:", e)

@app.route("/")
def home():
    return "<h1>WebinX Intelligence Engine Running</h1>"

@app.route("/health")
def health():
    return jsonify({"status": "ok"})

@app.route("/db-test")
def db_test():
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        result = cur.fetchone()
        cur.close()
        conn.close()
        return jsonify({"db_connection": "success", "result": result})
    except Exception as e:
        return jsonify({"db_connection": "failed", "error": str(e)})

@app.route("/events")
def list_events():
    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT * FROM events ORDER BY created_at DESC;")
        events = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify(events)
    except Exception as e:
        return jsonify({"error": str(e)})

# Initialize DB when service starts
init_db()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
