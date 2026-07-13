import json
import os
from kafka import KafkaConsumer
from datetime import datetime

consumer = KafkaConsumer(
    'claims_topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

landing_folder = r"C:\healthcare-realtime-project\landing\claims"

os.makedirs(landing_folder, exist_ok=True)

print("Listening for claim events...")

for message in consumer:
    claim = message.value

    filename = f"claim_{claim['claim_id']}_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"

    filepath = os.path.join(landing_folder, filename)

    with open(filepath, "w") as f:
        json.dump(claim, f, indent=4)

    print(f"Saved {filepath}")