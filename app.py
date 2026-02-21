import os
from flask import Flask
import psycopg

app = Flask(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")

def get_connection():
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL is not set")
    return psycopg.connect(DATABASE_URL)

@app.route("/")
def home():
    return "<h1>WebinX Intelligence Engine Running</h1>"

@app.route("/health")
def health():
    return {"status": "ok"}

@app.route("/db-check")
def db_check():
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                result = cur.fetchone()
        return {"database": "connected", "result": result[0]}
    except Exception as e:
        return {"database": "error", "message": str(e)}, 500

if __name__ == "__main__":
    app.run(debug=True)
