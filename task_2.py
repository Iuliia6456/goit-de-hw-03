from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("task_1").getOrCreate()

# Load DataFrames
users_df = spark.read.csv('./users.csv', header=True)
products_df = spark.read.csv('./products.csv', header=True)
purchases_df = spark.read.csv('./purchases.csv', header=True)

print("Row counts before cleaning:")
print(f"Users DataFrame: {users_df.count()} rows")
print(f"Products DataFrame: {products_df.count()} rows")
print(f"Purchases DataFrame: {purchases_df.count()} rows")

clean_users_df = users_df.na.drop()
clean_products_df = products_df.na.drop()
clean_purchases_df = purchases_df.na.drop()

# Row counts after cleaning
print("\nRow counts after cleaning:")
print(f"Cleaned Users DataFrame: {clean_users_df.count()} rows")
print(f"Cleaned Products DataFrame: {clean_products_df.count()} rows")
print(f"Cleaned Purchases DataFrame: {clean_purchases_df.count()} rows")

