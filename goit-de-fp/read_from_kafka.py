from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

# --- Kafka ---
kafka_bootstrap_servers = "kafka:9092"
kafka_topic = "athlete_event_results"

# --- Схема MySQL таблиці (вказуємо явно) ---
schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("athlete_id", IntegerType(), True),
        StructField("event_id", IntegerType(), True),
        StructField("medal", StringType(), True),
        # додай інші поля, якщо є
    ]
)

# --- Spark сесія ---
spark = SparkSession.builder.appName("Kafka to DataFrame").getOrCreate()

# --- Зчитування з Kafka ---
df_kafka = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
    .option("subscribe", kafka_topic)
    .load()
)

# --- Обробка JSON у DataFrame ---
df_parsed = (
    df_kafka.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
)

# --- Вивід у консоль (для перевірки) ---
query = (
    df_parsed.writeStream.format("console")
    .outputMode("append")
    .option("truncate", False)
    .start()
)

query.awaitTermination()
