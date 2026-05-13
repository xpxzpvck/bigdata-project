import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth

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

    parsed_stream = raw_stream \
        .select(
            col("value").cast("string").alias("json_payload"),
            col("timestamp")
        ) \
        .withColumn("year", year("timestamp")) \
        .withColumn("month", month("timestamp")) \
        .withColumn("day", dayofmonth("timestamp"))

    query = parsed_stream.writeStream \
        .format("parquet") \
        .option("path", MINIO_BUCKET_PATH) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .partitionBy("year", "month", "day") \
        .trigger(processingTime="1 minute") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()