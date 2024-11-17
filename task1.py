from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("task_1").getOrCreate()
users_df = spark.read.csv('./users.csv', header=True)
users_df.show(5)

products_df = spark.read.csv('./products.csv', header=True)
products_df.show(5)

purchases_df = spark.read.csv('./purchases.csv', header=True)
purchases_df.show(5)
