import pyodbc
import json
from kafka import KafkaProducer
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
)

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=RAVI\\SQLEXPRESS;"
    "DATABASE=Healthcare_Claims_DB;"
    "Trusted_Connection=yes;"
)

cursor = conn.cursor()

# Get watermark
cursor.execute("""
SELECT last_processed_timestamp
FROM ETL_Watermark
WHERE table_name='Claims'
""")

watermark = cursor.fetchone()[0]

print(f"Watermark : {watermark}")

# Read only changed rows
cursor.execute("""
SELECT
    claim_id,
    member_id,
    provider_id,
    claim_amount,
    claim_status,
    diagnosis_code,
    service_date,
    modified_date
FROM Claims
WHERE modified_date > ?
""", watermark)

columns = [column[0] for column in cursor.description]

max_timestamp = watermark

for row in cursor.fetchall():

    message = dict(zip(columns, row))

    producer.send("claims_topic", value=message)

    print(f"Published Claim {message['claim_id']}")
    if message['modified_date'] > max_timestamp:
        max_timestamp = message['modified_date']

producer.flush()

# Update watermark
cursor.execute("""
UPDATE ETL_Watermark
SET last_processed_timestamp = ?
WHERE table_name='Claims'
""", max_timestamp)

conn.commit()
conn.close()

print("Incremental load completed.")