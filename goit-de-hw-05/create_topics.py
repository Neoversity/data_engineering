from kafka.admin import KafkaAdminClient, NewTopic

admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092",  # змінити, якщо у тебе інший порт/хост
    client_id='anton_admin'
)

topic_list = [
    NewTopic(name="building_sensors_anton", num_partitions=1, replication_factor=1),
    NewTopic(name="temperature_alerts_anton", num_partitions=1, replication_factor=1),
    NewTopic(name="humidity_alerts_anton", num_partitions=1, replication_factor=1)
]

try:
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
    print("✅ Топіки успішно створено!")
except Exception as e:
    print("⚠️ Помилка при створенні топіків:", e)
