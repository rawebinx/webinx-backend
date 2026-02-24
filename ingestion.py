from db import get_connection
from auto_fetch import fetch
from intelligence import calculate_rank
from datetime import datetime
import uuid
import re


def generate_slug(title):
    slug = re.sub(r'[^a-zA-Z0-9]+', '-', title.lower()).strip('-')
    return slug


def ingest():
    events = fetch()

    if not events:
        print("No events fetched.")
        return

    conn = get_connection()
    cursor = conn.cursor()

    inserted = 0
    updated = 0

    try:
        for event in events:

            if not event.get("title") or not event.get("start_time"):
                continue

            slug = generate_slug(event["title"])
            now = datetime.utcnow()

            cursor.execute("""
                INSERT INTO events (
                    id,
                    title,
                    slug,
                    description,
                    event_url,
                    start_time,
                    end_time,
                    category_id,
                    sector_id,
                    source_id,
                    host_id,
                    is_active,
                    created_at,
                    updated_at
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (slug)
                DO UPDATE SET
                    title = EXCLUDED.title,
                    description = EXCLUDED.description,
                    event_url = EXCLUDED.event_url,
                    start_time = EXCLUDED.start_time,
                    end_time = EXCLUDED.end_time,
                    updated_at = EXCLUDED.updated_at
            """, (
                str(uuid.uuid4()),
                event["title"],
                slug,
                event.get("description"),
                event.get("event_url"),
                event["start_time"],
                event.get("end_time"),
                event["category_id"],
                event["sector_id"],
                event["source_id"],
                event.get("host_id"),
                True,
                now,
                now
            ))

            if cursor.rowcount == 1:
                inserted += 1
            else:
                updated += 1

        conn.commit()

    except Exception as e:
        conn.rollback()
        print("Ingestion failed:", e)

    finally:
        conn.close()

    print("Ingestion complete")
    print("Inserted:", inserted)
    print("Updated:", updated)


if __name__ == "__main__":
    ingest()