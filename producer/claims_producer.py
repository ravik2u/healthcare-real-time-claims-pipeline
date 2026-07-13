import pyodbc
import json
from kafka import KafkaProducer
from datetime import datetime

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
)

# SQL Server Connection
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=RAVI\\SQLEXPRESS;"
    "DATABASE=Healthcare_Claims_DB;"
    "Trusted_Connection=yes;"
)

cursor = conn.cursor()

query = """
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
"""

cursor.execute(query)

columns = [column[0] for column in cursor.description]

for row in cursor.fetchall():

    message = dict(zip(columns, row))

    producer.send(
        'claims_topic',
        value=message
    )

    print(f"Published Claim: {message['claim_id']}")

producer.flush()
conn.close()

print("All claim records published successfully.")