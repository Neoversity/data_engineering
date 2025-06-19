from pyspark.sql import SparkSession


def main():
    spark = (
        SparkSession.builder.appName("LandingToBronzeFromMySQL")
        .config("spark.jars", "/opt/spark/jars/mysql-connector-java-8.0.33.jar")
        .getOrCreate()
    )

    jdbc_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
    db_properties = {
        "user": "neo_data_admin",
        "password": "Proyahaxuqithab9oplp",
        "driver": "com.mysql.cj.jdbc.Driver",
    }

    tables = ["athlete_bio", "athlete_event_results"]

    for table in tables:
        print(f"✅ Завантаження таблиці {table} з MySQL...")
        df = spark.read.jdbc(url=jdbc_url, table=table, properties=db_properties)
        df.show()
        output_path = f"bronze/{table}"
        df.write.mode("overwrite").parquet(output_path)
        print(f"📁 Збережено {table} у {output_path}")

    spark.stop()
    print("🚀 Готово!")


if __name__ == "__main__":
    main()
