from azure.eventhub import EventHubProducerClient, EventData
from dotenv import load_dotenv
import time
import random
import json
import os

load_dotenv()

connection_str = os.getenv("EVENTHUB_CONNECTION_STRING")
eventhub_name = os.getenv("EVENTHUB_NAME")

if not connection_str or not eventhub_name:
    raise ValueError("❌ Missing EVENTHUB_CONNECTION_STRING or EVENTHUB_NAME in .env")

producer = EventHubProducerClient.from_connection_string(
    conn_str=connection_str, eventhub_name=eventhub_name
)

def generate_sensor_data():
    return {
        "sensor_id": f"S{random.randint(1,5)}",
        "timestamp": time.time(),
        "vehicle_count": random.randint(0, 50),
        "pollution_index": round(random.uniform(10, 100), 2),
        "city_area": random.choice(["North", "South", "East", "West"])
    }

while True:
    event_data_batch = producer.create_batch()
    for _ in range(10):
        data = generate_sensor_data()
        event_data_batch.add(EventData(json.dumps(data)))
    producer.send_batch(event_data_batch)
    print("✅ Batch sent!")
    time.sleep(5) 


