from db import get_connection
from auto_fetch import fetch
from datetime import datetime, timezone
import uuid
import re


# -------------------------------------------------
# UUID MAPPING (MATCHES FETCH SOURCE NAMES)
# -------------------------------------------------

SOURCE_MAP = {
    "Eventbrite India": "15c5ac4f-9ad0-412f-bfa1-035f6e2b9765",
    "Meetup India": "6e955bf1-be14-4d86-9930-43596ace17fa",
    "Airmeet Events": "43634822-404b-4168-abec-aa48e590686a",
}

DEFAULT_CATEGORY_ID = "8a55bb21-9a1e-467a-bd0f-4cd2751054ab"
DEFAULT_SECTOR_ID = "ee0936e5-7100-487a-8e6e-48ae2d64fad3"


# -------------------------------------------------
# SLUG GENERATOR
# -------------------------------------------------

def generate_slug(title: str) -> str:
    return re.sub(r'[^a-zA-Z0-9]+', '-', title.lower()).strip('-')


# -------------------------------------------------
# SAFE DATE PARSER
# -------------------------------------------------

def parse_event_date(event):
    raw_date = event.get("event_date") or event.get("start_time")

    if not raw_date:
        return None

    try:
        # If format is YYYY-MM-DD
        return datetime.strptime(raw_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except:
        try:
            # If ISO format like 2026-03-05T12:54:55Z
            return datetime.fromisoformat(
                raw_date.replace("Z", "+00:00")
            )
        except:
            return None


# -------------------------------------------------
# INGESTION ENGINE
# -------------------------------------------------

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

            print("FULL EVENT:", event)

            title = event.get("title")
            event_url = event.get("event_url")
            source_name = event.get("source")

            start_time = parse_event_date(event)

            if not title or not start_time:
                print("Skipping (missing title or valid date)")
                skipped += 1
                continue

            source_id = SOURCE_MAP.get(source_name)

            if not source_id:
                print(f"Skipping unknown source: {source_name}")
                skipped += 1
                continue

            slug = generate_slug(title)
            now = datetime.now(timezone.utc)

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
                    updated_at = EXCLUDED.updated_at
                RETURNING xmax;
            """, (
                str(uuid.uuid4()),
                title,
                slug,
                None,
                event_url,
                start_time,
                None,
                DEFAULT_CATEGORY_ID,
                DEFAULT_SECTOR_ID,
                source_id,
                None,
                True,
                now,
                now
            ))

            result = cursor.fetchone()

            if result and result[0] == 0:
                inserted += 1
            else:
                updated += 1

        conn.commit()

    except Exception as e:
        conn.rollback()
        print("❌ Ingestion failed with error:")
        print(str(e))
        raise

    finally:
        cursor.close()
        conn.close()

    print("Ingestion complete")
    print("Inserted:", inserted)
    print("Updated:", updated)
    print("Skipped:", skipped)


if __name__ == "__main__":
    ingest()