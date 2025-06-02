from pyspark.sql import SparkSession

# 1. Створюємо SparkSession
spark = SparkSession.builder \
    .appName("LoadAlertConditions") \
    .getOrCreate()

# 2. Зчитуємо CSV
alerts_df = spark.read.csv("alerts_conditions.csv", header=True, inferSchema=True)
alerts_df.show()
