from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'building_sensors_anton'

while True:
    message = {
        "sensor_id": random.randint(1, 5),
        "temperature": round(random.uniform(20, 80), 2),
        "humidity": round(random.uniform(30, 90), 2),
        "timestamp": datetime.utcnow().isoformat()
    }
    producer.send(topic_name, message)
    print("Sent:", message)
    time.sleep(1)
