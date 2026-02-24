from flask import Flask, jsonify
from db import get_connection

app = Flask(__name__)


@app.route("/")
def home():
    return "<h1>WebinX Backend Running</h1>"


@app.route("/health")
def health():
    return {"status": "ok"}


@app.route("/health/db")
def db_health():
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        cur.fetchone()
        cur.close()
        conn.close()
        return {"database": "connected"}
    except Exception as e:
        return {"database": "error", "message": str(e)}, 500


@app.route("/api/events")
def api_events():
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, title, start_time, slug
            FROM events
            ORDER BY start_time DESC
            LIMIT 20
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify(rows)
    except Exception as e:
        return {"error": str(e)}, 500


if __name__ == "__main__":
    app.run(debug=True)