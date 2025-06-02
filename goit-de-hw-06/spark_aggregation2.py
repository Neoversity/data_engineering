from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

# Spark сесія
spark = SparkSession.builder \
    .appName("SensorAggregation") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Схема повідомлення
schema = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("timestamp", StringType())
])

# Зчитування з Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "building_sensors_anton") \
    .option("startingOffsets", "latest") \
    .load()

# Парсинг значення
parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Перетворення рядка в timestamp
parsed = parsed.withColumn("event_time", col("timestamp").cast("timestamp"))

# Sliding window з watermark
aggregated = parsed \
    .withWatermark("event_time", "10 seconds") \
    .groupBy(
        window(col("event_time"), "1 minute", "30 seconds")
    ).agg(
        {"temperature": "avg", "humidity": "avg"}
    ) \
    .withColumnRenamed("avg(temperature)", "t_avg") \
    .withColumnRenamed("avg(humidity)", "h_avg")

# Вивід у консоль (для перевірки)
query = aggregated.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
