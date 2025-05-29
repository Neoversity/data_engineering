from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "building_sensors_anton",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

for message in consumer:
    data = message.value
    if data["temperature"] > 30.0:
        print(f"🌡️ Температура ВИСОКА: {data}")
