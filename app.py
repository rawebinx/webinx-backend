<<<<<<< HEAD
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
=======
from flask import Flask, jsonify, request, render_template
import sqlite3
import math

app = Flask(__name__)

DB_PATH = "webinx.db"
PER_PAGE = 20


def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


@app.route("/")
def home():
    page = int(request.args.get("page", 1))
    search = request.args.get("search", "").strip()
    source = request.args.get("source", "").strip()

    offset = (page - 1) * PER_PAGE

    conn = get_db_connection()
    cursor = conn.cursor()

    base_query = """
        FROM events
        WHERE 1=1
    """

    params = []

    if search:
        base_query += " AND title LIKE ?"
        params.append(f"%{search}%")

    if source:
        base_query += " AND source = ?"
        params.append(source)

    # Total count
    cursor.execute("SELECT COUNT(*) " + base_query, params)
    total_events = cursor.fetchone()[0]

    # Fetch paginated
    query = """
        SELECT id, title, event_date, source, event_url, final_rank
    """ + base_query + """
        ORDER BY final_rank DESC
        LIMIT ? OFFSET ?
    """

    cursor.execute(query, params + [PER_PAGE, offset])
    events = cursor.fetchall()
    conn.close()

    total_pages = math.ceil(total_events / PER_PAGE)

    return render_template(
        "home.html",
        events=events,
        current_page=page,
        total_pages=total_pages,
        total_events=total_events,
        search=search,
        source=source
    )


@app.route("/api/events")
def api_events():
    limit = int(request.args.get("limit", 10))
    page = int(request.args.get("page", 1))
    offset = (page - 1) * limit

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, title, event_date, source, event_url, final_rank
        FROM events
        ORDER BY final_rank DESC
        LIMIT ? OFFSET ?
    """, (limit, offset))

    rows = cursor.fetchall()
    conn.close()

    return jsonify([dict(row) for row in rows])

>>>>>>> 7eeba7d (Clean repo, remove cache, align with PostgreSQL production setup)

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
@app.route("/health/db")
def db_health():
    import os
    import psycopg

    try:
        conn = psycopg.connect(os.environ["DATABASE_URL"])
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        cur.fetchone()
        cur.close()
        conn.close()
        return "DB Connected ✅"
    except Exception as e:
        return f"DB Error ❌ {str(e)}"
        
if __name__ == "__main__":

