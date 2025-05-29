from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "temperature_alerts_anton",
    "humidity_alerts_anton",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("🎯 Очікуємо на алерти...")

for message in consumer:
    data = message.value
    print(f"🚨 Отримано алерт: {data}")
