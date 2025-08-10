from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, lit, to_timestamp
from pyspark.sql.types import StructType, StringType, IntegerType, LongType


spark = SparkSession.builder \
    .appName("CDC Kafka to Mongo") \
    .config("spark.mongodb.write.connection.uri", "mongodb://localhost:27017/ecommerce") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Define schema
orders_schema = StructType().add("order_id", IntegerType()) \
    .add("customer_name", StringType()) \
    .add("address", StringType()) \
    .add("phone", StringType()) \
    .add("order_date", LongType()) \
    .add("status", StringType())

cdc_schema = StructType().add("before", orders_schema) \
                         .add("after", orders_schema) \
                         .add("op", StringType()) \
                         .add("ts_ms", StringType())

# Read from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ecommerce.public.orders") \
    .option("startingOffsets", "earliest") \
    .load()


# Parse JSON
df_json = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json("json_str", cdc_schema).alias("data")) \
    .select("data.*")

# after op: insert, update
df_after = df_json.filter("op IN ('c','u')").select("after.*", "op", "ts_ms") \
    .withColumn(
        "order_date",
        (col("order_date") / 1000 / 1000).cast("timestamp")
    ) \
    .withColumn(
        "cdc_changed",
        when(col("op") == "c", lit("inserted")).otherwise(lit("updated"))
    ) \
    .withColumn(
        "cdc_time",
        to_timestamp((col("ts_ms").cast("long") / 1000))
    ).drop("op", "ts_ms")

# before op: delete
df_delete = df_json.filter("op = 'd'") \
    .selectExpr("before.order_id as order_id", "ts_ms") \
    .withColumn("cdc_changed", lit("deleted")) \
    .withColumn("cdc_time", to_timestamp((col("ts_ms").cast("long") / 1000))) \
    .drop("ts_ms")
                
# Debug & write to Mongo
def debug_batch(df, batch_id):
    print("Orders Topic")
    print(f"\n=== [Batch ID: {batch_id}] ===")
    df.show(truncate=False)

    df_with_id = df.withColumn("_id", col("order_id"))

    df_with_id.write \
      .format("mongodb") \
      .option("database", "ecommerce") \
      .option("collection", "orders") \
      .mode("append") \
      .save()

def mark_deleted_batch(df, _batch_id):
    from pymongo import MongoClient
    rows = df.select("order_id", "cdc_time").collect()
    if rows:
        client = MongoClient("mongodb://localhost:27017")
        collection = client["ecommerce"]["orders"]
        for row in rows:
            if row.order_id is not None:
                collection.update_one(
                    {"_id": row.order_id},
                    {"$set": {"cdc_changed": "deleted", "cdc_time": row.cdc_time}}
                )
        print(f"Marked deleted for order_id(s): {[row.order_id for row in rows]}")

# writeStream for insert/update
query_after = df_after.writeStream \
    .foreachBatch(debug_batch) \
    .option("checkpointLocation", "/tmp/ldduc/spark-checkpoint-orders") \
    .trigger(processingTime="10 seconds") \
    .outputMode("append") \
    .start()

query_delete = df_delete.writeStream \
    .foreachBatch(mark_deleted_batch) \
    .option("checkpointLocation", "/tmp/ldduc/spark-checkpoint-order-delete") \
    .trigger(processingTime="10 seconds") \
    .outputMode("append") \
    .start()

query_after.awaitTermination()
query_delete.awaitTermination()