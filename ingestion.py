from db import get_connection
from intelligence import calculate_rank
from auto_fetch import fetch
from datetime import datetime


def ingest():
    events = fetch()

    if not events:
        print("No events fetched.")
        return

    conn = get_connection()
    cursor = conn.cursor()

    inserted = 0
    updated = 0
    skipped = 0

    try:
        for event in events:

            # Validation guard
            required_fields = ["title", "event_date", "country", "mode", "event_url"]
            if not all(event.get(field) for field in required_fields):
                skipped += 1
                continue

            rank = calculate_rank(event)

            cursor.execute(
                "SELECT id FROM events WHERE event_url = ?",
                (event["event_url"],)
            )
            existing = cursor.fetchone()

            if existing:
                cursor.execute("""
                    UPDATE events
                    SET title = ?,
                        event_date = ?,
                        country = ?,
                        mode = ?,
                        source = ?,
                        final_rank = ?,
                        updated_at = ?
                    WHERE event_url = ?
                """, (
                    event["title"],
                    event["event_date"],
                    event["country"],
                    event["mode"],
                    event["source"],
                    rank,
                    datetime.now(),
                    event["event_url"]
                ))
                updated += 1
            else:
                cursor.execute("""
                    INSERT INTO events
                    (title, event_date, country, mode, event_url, source, final_rank, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    event["title"],
                    event["event_date"],
                    event["country"],
                    event["mode"],
                    event["event_url"],
                    event["source"],
                    rank,
                    datetime.now()
                ))
                inserted += 1

        conn.commit()

    except Exception as e:
        conn.rollback()
        print("Critical ingestion failure:", e)

    finally:
        conn.close()

    print(f"Ingestion completed.")
    print(f"Inserted: {inserted}")
    print(f"Updated: {updated}")
    print(f"Skipped: {skipped}")


if __name__ == "__main__":
    ingest()
