from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, round as spark_round

# -------------------------------------------------
# 1. Завантаження CSV-файлів як окремих DataFrame
# -------------------------------------------------

spark = SparkSession.builder \
    .appName("GoIT DE HW 03 - PySpark Data Analysis") \
    .getOrCreate()

users_df = spark.read.csv("users.csv", header=True, inferSchema=True)
purchases_df = spark.read.csv("purchases.csv", header=True, inferSchema=True)
products_df = spark.read.csv("products.csv", header=True, inferSchema=True)

print("\nP1 - Users DataFrame")
users_df.show()

print("\nP1 - Purchases DataFrame")
purchases_df.show()

print("\nP1 - Products DataFrame")
products_df.show()


# -------------------------------------------------
# 2. Очищення даних: видалення рядків з пропущеними значеннями
# -------------------------------------------------

users_clean_df = users_df.dropna()
purchases_clean_df = purchases_df.dropna()
products_clean_df = products_df.dropna()

print("\nP2 - Cleaned Users DataFrame")
users_clean_df.show()

print("\nP2 - Cleaned Purchases DataFrame")
purchases_clean_df.show()

print("\nP2 - Cleaned Products DataFrame")
products_clean_df.show()


# -------------------------------------------------
# Підготовка об'єднаного DataFrame для подальшого аналізу
# -------------------------------------------------

joined_df = purchases_clean_df \
    .join(users_clean_df, on="user_id", how="inner") \
    .join(products_clean_df, on="product_id", how="inner") \
    .withColumn("total_price", col("quantity") * col("price"))

print("\nJoined DataFrame with total_price")
joined_df.show()


# -------------------------------------------------
# 3. Загальна сума покупок за кожною категорією продуктів
# -------------------------------------------------

total_by_category_df = joined_df \
    .groupBy("category") \
    .agg(
        spark_round(
            spark_sum("total_price"), 2
        ).alias("total_spent")
    ) \
    .orderBy("category")

print("\nP3 - Total purchases by product category")
total_by_category_df.show()


# -------------------------------------------------
# 4. Сума покупок за кожною категорією для користувачів віком 18-25
# -------------------------------------------------

age_18_25_df = joined_df \
    .filter((col("age") >= 18) & (col("age") <= 25))

purchases_18_25_by_category_df = age_18_25_df \
    .groupBy("category") \
    .agg(
        spark_round(
            spark_sum("total_price"), 2
        ).alias("total_spent")
    ) \
    .orderBy("category")

print("\nP4 - Purchases by category for users aged 18-25")
purchases_18_25_by_category_df.show()


# -------------------------------------------------
# 5. Частка покупок за кожною категорією від сумарних витрат
#    для користувачів віком 18-25
# -------------------------------------------------

total_spent_18_25 = age_18_25_df \
    .agg(spark_sum("total_price").alias("total")) \
    .collect()[0]["total"]

percentage_18_25_by_category_df = purchases_18_25_by_category_df \
    .withColumn(
        "percentage",
        spark_round((col("total_spent") / total_spent_18_25) * 100, 2)
    ) \
    .orderBy(col("percentage").desc())

print("\nP5 - Percentage of purchases by category for users aged 18-25")
percentage_18_25_by_category_df.show()


# -------------------------------------------------
# 6. Три категорії з найвищим відсотком витрат
#    для користувачів віком 18-25
# -------------------------------------------------

top_3_categories_df = percentage_18_25_by_category_df \
    .orderBy(col("percentage").desc()) \
    .limit(3)

print("\nP6 - Top 3 categories by percentage for users aged 18-25")
top_3_categories_df.show()


# -------------------------------------------------
# Завершення Spark session
# -------------------------------------------------

spark.stop()