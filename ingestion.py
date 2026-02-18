from db import get_connection
from intelligence import calculate_rank
from auto_fetch import fetch

def ingest():
    events = fetch()
    conn = get_connection()
    cursor = conn.cursor()

    for event in events:
        rank = calculate_rank(event)

        cursor.execute("""
            INSERT OR IGNORE INTO events
            (title,event_date,country,mode,event_url,source,final_rank)
            VALUES(?,?,?,?,?,?,?)
        """,(event["title"],
             event["event_date"],
             event["country"],
             event["mode"],
             event["event_url"],
             event["source"],
             rank))

    conn.commit()
    conn.close()

if __name__ == "__main__":
    ingest()
