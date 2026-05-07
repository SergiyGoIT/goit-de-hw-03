from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, round

# 1. Створення Spark сесії
spark = SparkSession.builder \
    .appName("HW3") \
    .getOrCreate()

# 2. Завантаження даних
users = spark.read.csv("users.csv", header=True, inferSchema=True)
purchases = spark.read.csv("purchases.csv", header=True, inferSchema=True)
products = spark.read.csv("products.csv", header=True, inferSchema=True)

# 3. Очистка (видалення null)
users_clean = users.dropna()
purchases_clean = purchases.dropna()
products_clean = products.dropna()

# 4. JOIN таблиць
df = purchases_clean \
    .join(users_clean, "user_id") \
    .join(products_clean, "product_id")

# Додаткове поле: total_price
df = df.withColumn("total_price", col("quantity") * col("price"))

# -------------------------------------------
# 3. Загальна сума покупок по категоріях
# -------------------------------------------
total_by_category = df.groupBy("category") \
    .agg(_sum("total_price").alias("total_spent"))

total_by_category.show()

# -------------------------------------------
# 4. Сума для віку 18-25
# -------------------------------------------
df_young = df.filter((col("age") >= 18) & (col("age") <= 25))

total_young = df_young.groupBy("category") \
    .agg(_sum("total_price").alias("total_spent"))

total_young.show()

# -------------------------------------------
# 5. Частка (%)
# -------------------------------------------
total_sum_young = df_young.agg(_sum("total_price")).collect()[0][0]

share = total_young.withColumn(
    "percentage",
    round((col("total_spent") / total_sum_young) * 100, 2)
)

share.show()

# -------------------------------------------
# 6. TOP-3 категорії
# -------------------------------------------
top3 = share.orderBy(col("percentage").desc()).limit(3)

top3.show()