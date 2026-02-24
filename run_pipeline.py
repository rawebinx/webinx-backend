from ingestion import ingest
from datetime import datetime

LOG_FILE = "logs/ingestion.log"
ERROR_FILE = "logs/error.log"

import os

# Ensure logs folder exists
os.makedirs("logs", exist_ok=True)

def log(message):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now()}] {message}\n")

def log_error(message):
    with open(ERROR_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now()}] {message}\n")

if __name__ == "__main__":
    try:
        log("Pipeline started")
        ingest()
        log("Pipeline completed successfully")
    except Exception as e:
        log_error(f"Pipeline failed: {str(e)}")
