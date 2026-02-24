import os
import sys
from datetime import datetime
import psycopg2

from ingestion import ingest

DATABASE_URL = os.getenv("DATABASE_URL")

def main():
    conn = None
    cursor = None
    log_id = None

    try:
        # Connect to database
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()

        started_at = datetime.utcnow()

        # Insert start log
        cursor.execute("""
            INSERT INTO ingestion_logs (started_at, status)
            VALUES (%s, %s)
            RETURNING id;
        """, (started_at, 'success'))

        log_id = cursor.fetchone()[0]
        conn.commit()

        # Run ingestion
        ingest()

        finished_at = datetime.utcnow()

        # Update success
        cursor.execute("""
            UPDATE ingestion_logs
            SET finished_at = %s
            WHERE id = %s;
        """, (finished_at, log_id))

        conn.commit()

        print("Ingestion completed successfully.")
        sys.exit(0)

    except Exception as e:
        print("Ingestion failed:", str(e))

        if conn and log_id:
            cursor.execute("""
                UPDATE ingestion_logs
                SET finished_at = %s,
                    status = 'failed',
                    error_message = %s
                WHERE id = %s;
            """, (datetime.utcnow(), str(e), log_id))
            conn.commit()

        sys.exit(1)

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    main()