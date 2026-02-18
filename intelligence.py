from datetime import datetime

def calculate_rank(event):
    score = 10
    title = event.get("title","").lower()

    keywords = {
        "ai":15,
        "tender":12,
        "government":10,
        "startup":8,
        "conference":6,
        "webinar":6,
        "summit":8
    }

    for word, value in keywords.items():
        if word in title:
            score += value

    try:
        event_date = datetime.strptime(event["event_date"], "%Y-%m-%d")
        days_left = (event_date - datetime.now()).days
        if 0 <= days_left <= 7:
            score += 10
    except:
        pass

    return float(score)
