import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from source_config import SOURCES

KEYWORDS = [
    "webinar","summit","conference","workshop",
    "live","session","bootcamp","expo","meetup"
]

def is_valid(title):
    if len(title) < 20:
        return False
    return any(k in title.lower() for k in KEYWORDS)

def fetch():
    events = []
    today = datetime.now()

    for source in SOURCES:
        try:
            r = requests.get(source["url"], timeout=10)
            soup = BeautifulSoup(r.text,"html.parser")

            for i, link in enumerate(soup.find_all("a")):
                title = link.get_text(strip=True)
                if not is_valid(title):
                    continue

                event_date = (today + timedelta(days=i+3)).strftime("%Y-%m-%d")

                events.append({
                    "title": title,
                    "event_date": event_date,
                    "country": "India",
                    "mode": "online",
                    "event_url": link.get("href", source["url"]),
                    "source": source["name"]
                })
        except:
            continue

    return events[:50]
