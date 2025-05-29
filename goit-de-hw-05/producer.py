from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

buildings = ["Building A", "Building B", "Building C"]

while True:
    data = {
        "building": random.choice(buildings),
        "temperature": round(random.uniform(18.0, 35.0), 2),
        "humidity": round(random.uniform(30.0, 70.0), 2),
    }

    # Відправляємо в основний топік
    producer.send("building_sensors_anton", value=data)
    print(f"📤 Надіслано: {data}")

    time.sleep(1)
