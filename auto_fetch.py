import requests
import json
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urljoin
from source_config import SOURCES


HEADERS = {
    "User-Agent": "Mozilla/5.0"
}


# -------------------------------------------------
# Common Date Extractor (JSON-LD based)
# -------------------------------------------------

def extract_event_date_from_page(event_url):
    try:
        r = requests.get(event_url, headers=HEADERS, timeout=10)
        if r.status_code != 200:
            return None

        soup = BeautifulSoup(r.text, "html.parser")

        scripts = soup.find_all("script", type="application/ld+json")

        for script in scripts:
            try:
                data = json.loads(script.string)
                if isinstance(data, dict) and data.get("@type") == "Event":
                    start = data.get("startDate")
                    if start:
                        return datetime.fromisoformat(
                            start.replace("Z", "+00:00")
                        ).date()
            except:
                continue

    except:
        return None

    return None


# -------------------------------------------------
# EVENTBRITE
# -------------------------------------------------

def fetch_eventbrite(source):
    events = []
    today = datetime.now().date()

    try:
        response = requests.get(source["url"], headers=HEADERS, timeout=10)
        if response.status_code != 200:
            return events

        soup = BeautifulSoup(response.text, "html.parser")
        links = soup.find_all("a", href=True)

        seen = set()
        count = 0

        for link in links:
            href = link["href"]

            if "/e/" not in href:
                continue

            title = link.get_text(strip=True)
            if not title or len(title) < 10:
                continue

            event_url = urljoin(source["url"], href)

            if event_url in seen:
                continue
            seen.add(event_url)

            event_date = extract_event_date_from_page(event_url)
            if not event_date or event_date < today:
                continue

            events.append({
                "title": title,
                "event_date": event_date.strftime("%Y-%m-%d"),
                "country": "India",
                "mode": "online",
                "event_url": event_url,
                "source": source["name"]
            })

            count += 1
            if count >= 20:
                break

    except:
        pass

    return events


# -------------------------------------------------
# MEETUP
# -------------------------------------------------

def fetch_meetup(source):
    events = []
    today = datetime.now().date()

    try:
        response = requests.get(source["url"], headers=HEADERS, timeout=10)
        if response.status_code != 200:
            return events

        soup = BeautifulSoup(response.text, "html.parser")
        links = soup.find_all("a", href=True)

        seen = set()
        count = 0

        for link in links:
            href = link["href"]

            if "/events/" not in href:
                continue

            title = link.get_text(strip=True)
            if not title or len(title) < 10:
                continue

            event_url = urljoin(source["url"], href)

            if event_url in seen:
                continue
            seen.add(event_url)

            event_date = extract_event_date_from_page(event_url)
            if not event_date or event_date < today:
                continue

            events.append({
                "title": title,
                "event_date": event_date.strftime("%Y-%m-%d"),
                "country": "India",
                "mode": "online",
                "event_url": event_url,
                "source": source["name"]
            })

            count += 1
            if count >= 20:
                break

    except:
        pass

    return events


# -------------------------------------------------
# AIRMEET
# -------------------------------------------------

def fetch_airmeet(source):
    events = []
    today = datetime.now().date()

    try:
        response = requests.get(source["url"], headers=HEADERS, timeout=10)
        if response.status_code != 200:
            return events

        soup = BeautifulSoup(response.text, "html.parser")
        links = soup.find_all("a", href=True)

        seen = set()
        count = 0

        for link in links:
            href = link["href"]

            if "/event/" not in href:
                continue

            title = link.get_text(strip=True)
            if not title or len(title) < 8:
                continue

            event_url = urljoin(source["url"], href)

            if event_url in seen:
                continue
            seen.add(event_url)

            event_date = extract_event_date_from_page(event_url)
            if not event_date or event_date < today:
                continue

            events.append({
                "title": title,
                "event_date": event_date.strftime("%Y-%m-%d"),
                "country": "India",
                "mode": "online",
                "event_url": event_url,
                "source": source["name"]
            })

            count += 1
            if count >= 20:
                break

    except:
        pass

    return events


# -------------------------------------------------
# DISPATCHER
# -------------------------------------------------

def fetch():
    all_events = []

    for source in SOURCES:
        print(f"Fetching from: {source['name']}")

        if source["type"] == "eventbrite":
            events = fetch_eventbrite(source)
        elif source["type"] == "meetup":
            events = fetch_meetup(source)
        elif source["type"] == "airmeet":
            events = fetch_airmeet(source)
        else:
            events = []

        print(f"Valid events from {source['name']}: {len(events)}")
        all_events.extend(events)

    print("Total valid events:", len(all_events))
    return all_events
