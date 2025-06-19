from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Налаштування підключення до MySQL
jdbc_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
jdbc_table = "athlete_bio"
jdbc_user = "neo_data_admin"
jdbc_password = "Proyahaxuqithab9oplp"

# Ініціалізація SparkSession
spark = (
    SparkSession.builder.appName("AthleteBioReader")
    .config("spark.jars", "mysql-connector-j-8.0.33.jar")
    .getOrCreate()
)

# Завантаження таблиці athlete_bio
df = (
    spark.read.format("jdbc")
    .options(
        url=jdbc_url,
        driver="com.mysql.cj.jdbc.Driver",
        dbtable=jdbc_table,
        user=jdbc_user,
        password=jdbc_password,
    )
    .load()
)

# Фільтрація: тільки ті, де height і weight — це дійсні числа
filtered_df = df.filter(
    (col("height").cast("double").isNotNull())
    & (col("weight").cast("double").isNotNull())
)

# Вивід перших 10 рядків очищених даних
filtered_df.select("athlete_id", "name", "height", "weight").show(10)
