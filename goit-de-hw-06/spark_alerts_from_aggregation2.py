from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import *

spark = SparkSession.builder.appName("AlertDetection").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Схема вхідних даних
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

# Парсимо JSON
parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", col("timestamp").cast("timestamp"))

# Агрегація
aggregated = parsed.withWatermark("event_time", "10 seconds") \
    .groupBy(window(col("event_time"), "1 minute", "30 seconds")) \
    .agg(
        {"temperature": "avg", "humidity": "avg"}
    ).withColumnRenamed("avg(temperature)", "t_avg") \
     .withColumnRenamed("avg(humidity)", "h_avg")

# Завантаження CSV з алертами
alerts_df = spark.read.csv("alerts_conditions.csv", header=True, inferSchema=True)

# Cross join + фільтрація
joined = aggregated.crossJoin(alerts_df).filter(
    ((alerts_df.temperature_min == -999) | (aggregated.t_avg >= alerts_df.temperature_min)) &
    ((alerts_df.temperature_max == -999) | (aggregated.t_avg <= alerts_df.temperature_max)) &
    ((alerts_df.humidity_min == -999) | (aggregated.h_avg >= alerts_df.humidity_min)) &
    ((alerts_df.humidity_max == -999) | (aggregated.h_avg <= alerts_df.humidity_max))
)

# Результат — тільки потрібні поля
result = joined.select(
    "window",
    "t_avg",
    "h_avg",
    "code",
    "message"
)


# result.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start() \
#     .awaitTermination()

result.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .outputMode("append") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "alerts_output") \
    .option("checkpointLocation", "/tmp/spark_checkpoints/alerts") \
    .start() \
    .awaitTermination()
