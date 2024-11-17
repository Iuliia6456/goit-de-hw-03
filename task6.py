from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, round

spark = SparkSession.builder.appName("CategoryShare").getOrCreate()

users_df = spark.read.csv('./users.csv', header=True, inferSchema=True)
products_df = spark.read.csv('./products.csv', header=True, inferSchema=True)
purchases_df = spark.read.csv('./purchases.csv', header=True, inferSchema=True)

products_df = products_df.withColumn("price", col("price").cast("float"))
purchases_df = purchases_df.withColumn("quantity", col("quantity").cast("int"))
users_df = users_df.withColumn("age", col("age").cast("int"))

# clean dfs
clean_products_df = products_df.na.drop(subset=["product_id", "category", "price"])
clean_purchases_df = purchases_df.na.drop(subset=["purchase_id", "user_id", "product_id", "quantity"])
clean_users_df = users_df.na.drop(subset=["user_id", "age"])

# Join purchases with products and users
purchases_with_products = clean_purchases_df.join(clean_products_df, on="product_id", how="inner")
purchases_with_users = purchases_with_products.join(clean_users_df, on="user_id", how="inner")

# Filter for age group 18 to 25
purchases_age_group = purchases_with_users.filter((col("age") >= 18) & (col("age") <= 25))

# Calculate total cost
purchases_age_group = purchases_age_group.withColumn("total_cost", col("price") * col("quantity"))

# Calculate total cost per category
category_cost = (
    purchases_age_group.groupBy("category")
    .agg(round(sum("total_cost"), 2).alias("category_total"))
)

# Calculate total cost across all categories
total_cost = purchases_age_group.agg(round(sum("total_cost"), 2).alias("total")).collect()[0]["total"]

# Calculate share of each category
category_share = (
    category_cost.withColumn("share_of_total", round(col("category_total") / total_cost * 100, 2))
)

# Select the top 3 categories
top_3_categories = category_share.orderBy(col("share_of_total").desc()).limit(3)
top_3_categories.show()
