from flask import Flask, jsonify
from db import get_events, init_db

app = Flask(__name__)
init_db()

@app.route("/")
def home():
    return "WebinX Intelligence Engine Running"

@app.route("/api/events")
def api_events():
    events = get_events()
    return jsonify(events)

if __name__ == "__main__":
    app.run()
