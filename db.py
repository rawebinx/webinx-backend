import sqlite3

DB_NAME = "webinx.db"

def get_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            event_date TEXT NOT NULL,
            country TEXT NOT NULL,
            mode TEXT NOT NULL,
            event_url TEXT NOT NULL,
            source TEXT,
            final_rank REAL,
            UNIQUE(title, event_date)
        )
    """)

    conn.commit()
    conn.close()

def get_events():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT * FROM events
        WHERE country='India'
        AND event_date >= DATE('now')
        ORDER BY final_rank DESC
        LIMIT 100
    """)

    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]
