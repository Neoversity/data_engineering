from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, round

# Створення Spark сесії
spark = SparkSession.builder \
    .appName("HW3 Data Analysis") \
    .getOrCreate()

# Шляхи до файлів
base_path = "./"


# Крок 1 Завантаження CSV-файлів
users = spark.read.option("header", "true").option("inferSchema", "true").csv("users.csv")
products = spark.read.option("header", "true").option("inferSchema", "true").csv("products.csv")
purchases = spark.read.option("header", "true").option("inferSchema", "true").csv("purchases.csv")



# users.show(5)
# products.show(5)
# purchases.show(5)

#  Очистка даних 
users_clean = users.dropna()
products_clean = products.dropna()
purchases_clean = purchases.dropna()
# print("Users (очищено):")
# users_clean.show(5)
# print("Products (очищено):")
# products_clean.show(5)
# print("Purchases (очищено):")
# purchases_clean.show(5)


# Крок 3 Загальна сума покупок за категоріями

joined = purchases_clean.join(products_clean, "product_id")

# Обчислюємо вартість покупки: ціна × кількість
joined = joined.withColumn("total", col("price") * col("quantity"))

# Сума за категоріями
# total_by_category = joined.groupBy("category").agg(spark_sum("total").alias("total_sales"))
# total_by_category.show()

# Крок 4. Сума покупок за категоріями

# Фільтруємо користувачів віком 18–25 включно
users_18_25 = users_clean.filter((col("age") >= 18) & (col("age") <= 25))

# З'єднуємо purchases → users → products
joined_18_25 = purchases_clean.join(users_18_25, "user_id") \
                              .join(products_clean, "product_id")

# Розрахунок total
joined_18_25 = joined_18_25.withColumn("total", col("price") * col("quantity"))

# Сума покупок за категоріями
total_18_25_by_category = joined_18_25.groupBy("category") \
    .agg(spark_sum("total").alias("total_sales_18_25"))

# total_18_25_by_category.show()

# Крок 5. Частка покупок за кожною категорією

# Обчислюємо загальну суму всіх покупок цієї вікової групи
total_all_18_25 = total_18_25_by_category.agg(spark_sum("total_sales_18_25")).first()[0]

# Додаємо колонку з часткою (у відсотках)
share_18_25 = total_18_25_by_category.withColumn(
    "share_percent", round(col("total_sales_18_25") / total_all_18_25 * 100, 2)
)

# share_18_25.show()

# Крок 6. Топ-3 категорії

top3_categories = share_18_25.orderBy(col("share_percent").desc()).limit(3)
top3_categories.show()
