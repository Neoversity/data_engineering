import re
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

BRONZE_PATH = os.getenv("BRONZE_PATH", "/opt/airflow/bronze")
SILVER_PATH = os.getenv("SILVER_PATH", "/opt/airflow/silver")


# UDF для очищення тексту
def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', "", str(text))


clean_text_udf = udf(clean_text, StringType())


def process_table(spark, table):
    print(f"🔄 Обробка таблиці: {table}")
    # input_path = f"bronze/{table}"
    input_path = f"{BRONZE_PATH}/{table}"
    output_path = f"{SILVER_PATH}/{table}"

    df = spark.read.parquet(input_path)
    df.show()

    # Очистити всі колонки типу string
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, clean_text_udf(col(field.name)))

    # Дедуплікація
    df_cleaned = df.dropDuplicates()
    df_cleaned.show()

    # Збереження
    df_cleaned.write.mode("overwrite").parquet(output_path)
    print(f"✅ Збережено в: {output_path}")


def main():
    spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

    tables = ["athlete_bio", "athlete_event_results"]

    for table in tables:
        process_table(spark, table)

    spark.stop()
    print("🎉 Готово!")


if __name__ == "__main__":
    main()
