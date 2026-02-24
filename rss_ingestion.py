import feedparser
import sqlite3
from datetime import datetime
from rss_sources import RSS_SOURCES

DB_PATH = "webinx.db"


def event_exists(cursor, url):
    cursor.execute("SELECT 1 FROM events WHERE event_url = ?", (url,))
    return cursor.fetchone() is not None


def extract_date(entry):
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        return datetime(*entry.published_parsed[:6]).date()
    return datetime.utcnow().date()


def looks_like_event(title):
    keywords = [
        "event", "summit", "conference", "webinar",
        "bootcamp", "workshop", "meetup", "hackathon",
        "live", "masterclass"
    ]
    title_lower = title.lower()
    return any(word in title_lower for word in keywords)


def insert_event(cursor, event):
    cursor.execute("""
        INSERT INTO events 
        (title, event_date, event_url, source, country, mode, final_rank, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        event["title"],
        event["event_date"],
        event["event_url"],
        event["source"],
        "India",
        "online",
        30.0,
        datetime.utcnow(),
        datetime.utcnow()
    ))


def fetch_rss_events():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    inserted = 0
    skipped = 0

    for source in RSS_SOURCES:
        print(f"Fetching from: {source['name']}")
        feed = feedparser.parse(source["url"])

        for entry in feed.entries[:30]:
            title = entry.get("title", "").strip()
            link = entry.get("link", "")

            if not title or not link:
                skipped += 1
                continue

            if not looks_like_event(title):
                skipped += 1
                continue

            if event_exists(cursor, link):
                skipped += 1
                continue

            event_date = extract_date(entry)

            try:
                insert_event(cursor, {
                    "title": title,
                    "event_date": event_date,
                    "event_url": link,
                    "source": source["name"]
                })
                inserted += 1
            except:
                skipped += 1

    conn.commit()
    conn.close()

    print(f"RSS Done → Inserted: {inserted}, Skipped: {skipped}")


if __name__ == "__main__":
    fetch_rss_events()