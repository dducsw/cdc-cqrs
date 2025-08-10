from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, concat_ws, collect_list, sum as spark_sum, round

"""
spark-submit \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:10.5.0,org.postgresql:postgresql:42.7.7 \
  first_load.py

"""

spark = SparkSession.builder \
    .appName("Denormalization and first load to MongoDB") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.5.0,org.postgresql:postgresql:42.7.7") \
    .config("spark.mongodb.write.connection.uri", "mongodb://localhost:27017/ecommerce") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

jdbc_url = "jdbc:postgresql://localhost:5432/ecommerce"
jdbc_connection = {
    "user": "debezium",
    "password": "6666",
    "driver": "org.postgresql.Driver"
}

# Read tables from Postgres
print("* Read tables from Postgres")
orders = spark.read.jdbc(jdbc_url, "public.orders", properties=jdbc_connection)
order_details = spark.read.jdbc(jdbc_url, "public.order_details", properties=jdbc_connection)
products = spark.read.jdbc(jdbc_url, "public.products", properties=jdbc_connection)



def write_to_mongo(df, collection_name):
    print(f"* Write {collection_name}")
    df.write \
        .format("mongodb") \
        .option("database", "ecommerce") \
        .option("collection", collection_name) \
        .mode("overwrite") \
        .save()

# Write to MongoDB
write_to_mongo(orders, "orders")
write_to_mongo(products, "products")
write_to_mongo(order_details, "order_details")


print("** Success **")
spark.stop()