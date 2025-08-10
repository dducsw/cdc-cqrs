from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, lit, to_timestamp
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType

spark = SparkSession.builder \
    .appName("CDC Kafka to Mongo - OrderDetails") \
    .config("spark.mongodb.write.connection.uri", "mongodb://localhost:27017/ecommerce") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Define schema for order_details
order_details_schema = StructType() \
    .add("order_detail_id", IntegerType()) \
    .add("order_id", IntegerType()) \
    .add("product_id", IntegerType()) \
    .add("unit_price", FloatType()) \
    .add("quantity", IntegerType()) \
    .add("discount", FloatType())

cdc_schema = StructType().add("before", order_details_schema) \
                         .add("after", order_details_schema) \
                         .add("op", StringType()) \
                         .add("ts_ms", StringType())

# Read from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ecommerce.public.order_details") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON
df_json = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json("json_str", cdc_schema).alias("data")) \
    .select("data.*")

# after op: insert, update
df_after = df_json.filter("op IN ('c','u')").select("after.*", "op", "ts_ms") \
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
    .selectExpr("before.order_detail_id as order_detail_id", "ts_ms") \
    .withColumn("cdc_changed", lit("deleted")) \
    .withColumn("cdc_time", to_timestamp((col("ts_ms").cast("long") / 1000))) \
    .drop("ts_ms")

def debug_batch(df, batch_id):
    print("Order Details Topic")
    print(f"\n=== [Batch ID: {batch_id}] ===")
    df.show(truncate=False)

    df_with_id = df.withColumn("_id", col("order_detail_id"))

    df_with_id.write \
      .format("mongodb") \
      .option("database", "ecommerce") \
      .option("collection", "order_details") \
      .mode("append") \
      .save()

def mark_deleted_batch(df, _batch_id):
    from pymongo import MongoClient
    rows = df.select("order_detail_id", "cdc_time").collect()
    if rows:
        client = MongoClient("mongodb://localhost:27017")
        collection = client["ecommerce"]["order_details"]
        for row in rows:
            if row.order_detail_id is not None:
                collection.update_one(
                    {"_id": row.order_detail_id},
                    {"$set": {"cdc_changed": "deleted", "cdc_time": row.cdc_time}}
                )
        print(f"Marked deleted for order_detail_id(s): {[row.order_detail_id for row in rows]}")

query_after = df_after.writeStream \
    .foreachBatch(debug_batch) \
    .option("checkpointLocation", "/tmp/ldduc/spark-checkpoint-order-details") \
    .trigger(processingTime="10 seconds") \
    .outputMode("append") \
    .start()

query_delete = df_delete.writeStream \
    .foreachBatch(mark_deleted_batch) \
    .option("checkpointLocation", "/tmp/ldduc/spark-checkpoint-order-details-delete") \
    .trigger(processingTime="10 seconds") \
    .outputMode("append") \
    .start()

query_after.awaitTermination()
query_delete.awaitTermination()