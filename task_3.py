from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, round


spark = SparkSession.builder.appName("TotalAmountByCategory").getOrCreate()

# Load DataFrames
products_df = spark.read.csv('./products.csv', header=True)
purchases_df = spark.read.csv('./purchases.csv', header=True)

purchases_df = purchases_df.withColumn("quantity", col("quantity").cast("int"))
products_df = products_df.withColumn("price", col("price").cast("float"))

# clean dfs
clean_products_df = products_df.na.drop(subset=["product_id", "category", "price"])
clean_purchases_df = purchases_df.na.drop(subset=["purchase_id", "user_id", "product_id", "quantity"])

# Join purchases with products
purchases_with_products = clean_purchases_df.join(clean_products_df, on="product_id", how="inner")

# Calculate total amount
total_amount_by_category = (
    purchases_with_products
    .withColumn("total_amount", col("price") * col("quantity"))
    .groupBy("category")
    .agg(round(sum("total_amount"), 2).alias("total_purchase_amount"))
    .orderBy("total_purchase_amount", ascending=False)
)

total_amount_by_category.show()
