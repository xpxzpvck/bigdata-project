import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, year, month, dayofmonth
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

spark = SparkSession.builder \
    .appName("BronzeLayer") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "raw-page-creates"
MINIO_BUCKET_PATH = "s3a://wikipedia-batch-data/raw"
CHECKPOINT_PATH = "s3a://wikipedia-batch-data/checkpoints/raw_archive"

def main():
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    schema = StructType([
        StructField("page_title", StringType(), True),
        StructField("meta", StructType([
            StructField("domain", StringType(), True)
        ]), True),
        StructField("performer", StructType([
            StructField("user_text", StringType(), True),
            StructField("user_is_bot", BooleanType(), True)
        ]), True)
    ])

    parsed_stream = raw_stream \
        .select(
            from_json(col("value").cast("string"), schema).alias("data"),
            col("timestamp").alias("event_time")
        ) \
        .select(
            col("data.page_title").alias("page_title"),
            col("data.meta.domain").alias("domain"),
            col("data.performer.user_text").alias("user_name"),
            col("data.performer.user_is_bot").alias("user_is_bot"),
            col("event_time")
        ) \
        .withColumn("year", year("event_time")) \
        .withColumn("month", month("event_time")) \
        .withColumn("day", dayofmonth("event_time"))

    query = parsed_stream.coalesce(1) \
        .writeStream \
        .format("parquet") \
        .option("path", MINIO_BUCKET_PATH) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .partitionBy("year", "month", "day") \
        .trigger(processingTime="1 minute") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()