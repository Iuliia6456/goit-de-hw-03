from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, round


spark = SparkSession.builder.appName("task_1").getOrCreate()

# Load DataFrames
users_df = spark.read.csv('./users.csv', header=True)
products_df = spark.read.csv('./products.csv', header=True)
purchases_df = spark.read.csv('./purchases.csv', header=True)

products_df = products_df.withColumn("price", col("price").cast("float"))
purchases_df = purchases_df.withColumn("quantity", col("quantity").cast("int"))
users_df = users_df.withColumn("age", col("age").cast("int"))

# clean dfs
clean_products_df = products_df.na.drop(subset=["product_id", "category", "price"])
clean_purchases_df = purchases_df.na.drop(subset=["purchase_id", "user_id", "product_id", "quantity"])
clean_users_df = users_df.na.drop(subset=["user_id", "age"])

# Join purchases with products
purchases_with_products = clean_purchases_df.join(clean_products_df, on="product_id", how="inner")

# Join with users
purchases_with_users = purchases_with_products.join(clean_users_df, on="user_id", how="inner")

# Filter purchases between 18 and 25
purchases_age_group = purchases_with_users.filter((col("age") >= 18) & (col("age") <= 25))

# Calculate total purchase amount
total_amount_by_category = (
    purchases_age_group.withColumn("total_amount", col("price") * col("quantity"))
    .groupBy("category")
    .agg(round(sum("total_amount"), 2).alias("total_purchase_amount"))
    .orderBy("total_purchase_amount", ascending=False)
)

total_amount_by_category.show()