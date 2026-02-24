import os
import psycopg2

DATABASE_URL = os.getenv("DATABASE_URL")

conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS ingestion_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    total_fetched INTEGER DEFAULT 0,
    processed INTEGER DEFAULT 0,
    skipped INTEGER DEFAULT 0,
    status VARCHAR(20) CHECK (status IN ('success','failed')),
    error_message TEXT
);
""")

cursor.execute("""
CREATE INDEX IF NOT EXISTS idx_ingestion_started_at 
ON ingestion_logs(started_at DESC);
""")

conn.commit()
cursor.close()
conn.close()

print("ingestion_logs table created successfully.")