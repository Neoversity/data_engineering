from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, current_timestamp
from pyspark.sql.types import FloatType


def main():
    spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

    # Зчитуємо таблиці
    bio_df = spark.read.parquet("/opt/airflow/silver/athlete_bio")
    results_df = spark.read.parquet("/opt/airflow/silver/athlete_event_results")

    # Приводимо height/weight до числових типів
    bio_df = bio_df.withColumn("height", col("height").cast(FloatType())).withColumn(
        "weight", col("weight").cast(FloatType())
    )

    # Видаляємо дублікат колонки country_noc з results_df, щоб не було конфлікту
    results_df = results_df.drop("country_noc")

    # JOIN по athlete_id
    joined_df = results_df.join(bio_df, on="athlete_id", how="inner")

    # Агрегація
    agg_df = joined_df.groupBy("sport", "medal", "sex", "country_noc").agg(
        avg("height").alias("avg_height"), avg("weight").alias("avg_weight")
    )

    # Додаємо колонку з часовою міткою
    final_df = agg_df.withColumn("timestamp", current_timestamp())
    final_df.show()

    # Запис результату
    final_df.write.mode("overwrite").parquet("/opt/airflow/gold/avg_stats")

    print("🎯 Збережено у gold/avg_stats")
    spark.stop()


if __name__ == "__main__":
    main()
