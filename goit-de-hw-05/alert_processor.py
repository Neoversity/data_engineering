from kafka import KafkaConsumer, KafkaProducer
import json

# Споживач читає з топіка з даними від сенсорів
consumer = KafkaConsumer(
    "building_sensors_anton",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Виробник для надсилання алертів
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda m: json.dumps(m).encode('utf-8')
)

for message in consumer:
    data = message.value
    alert = None

    if data["temperature"] > 40.0:
        alert = {
            "sensor_id": data.get("sensor_id", "unknown"),
            "timestamp": data.get("timestamp", ""),
            "temperature": data["temperature"],
            "message": "Температура перевищує 40°C"
        }
        producer.send("temperature_alerts_anton", alert)
        print("🚨 Температурний алерт надіслано:", alert)

    if data["temperature"] < 20.0:
        alert = {
            "sensor_id": data.get("sensor_id", "unknown"),
            "timestamp": data.get("timestamp", ""),
            "temperature": data["temperature"],
            "message": "Температура занадто низька"
        }
        producer.send("temperature_alerts_anton", alert)
        print("❄️ Температурний алерт (низька):", alert)

    if data["humidity"] > 80.0 or data["humidity"] < 20.0:
        alert = {
            "sensor_id": data.get("sensor_id", "unknown"),
            "timestamp": data.get("timestamp", ""),
            "humidity": data["humidity"],
            "message": "Вологість поза допустимими межами"
        }
        producer.send("humidity_alerts_anton", alert)
        print("💧 Алерт вологості надіслано:", alert)
