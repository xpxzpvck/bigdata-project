import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, coalesce
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

INPUT_TOPIC = "raw-page-creates"
CHECKPOINT_PATH = "s3a://wikipedia-batch-data/checkpoints/api_views"

def process_batch(df, epoch_id):
    df.persist()
    if df.isEmpty():
        df.unpersist()
        return

    # 1. Запис у таблицю C2 (Pages by User)
    df.select("user_id", "created_at", "page_id", "page_title", "domain") \
        .write.format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "wikipedia_analytics") \
        .option("table", "pages_by_user") \
        .mode("append").save()

    # 2. Запис у таблицю C3 (Page Details)
    df.select("page_id", "page_title", "domain", "user_name", "created_at") \
        .write.format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "wikipedia_analytics") \
        .option("table", "page_details") \
        .mode("append").save()

    # 3. Запис у таблицю C4 (Pages by Domain)
    df.select("domain", "created_at", "page_id", "page_title", "user_name") \
        .write.format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "wikipedia_analytics") \
        .option("table", "pages_by_domain") \
        .mode("append").save()

    df.unpersist()

def main():
    spark = SparkSession.builder \
        .appName("ApiViewsJob") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    schema = StructType([
        StructField("page_id", IntegerType(), True),
        StructField("page_title", StringType(), True),
        StructField("meta", StructType([
            StructField("domain", StringType(), True)
        ]), True),
        StructField("performer", StructType([
            StructField("user_id", IntegerType(), True),
            StructField("user_text", StringType(), True)
        ]), True)
    ])

    parsed_stream = raw_stream \
        .select(
            from_json(col("value").cast("string"), schema).alias("data"),
            col("timestamp").alias("created_at")
        )

    # Витягуємо поля. 
    # Захист: Анонімні автори (по IP) не мають user_id, тому беремо їхній IP з user_text
    final_stream = parsed_stream.select(
        col("data.page_id").cast("string").alias("page_id"),
        col("data.page_title").alias("page_title"),
        col("data.meta.domain").alias("domain"),
        col("data.performer.user_text").alias("user_name"),
        coalesce(col("data.performer.user_id").cast("string"), col("data.performer.user_text")).alias("user_id"),
        col("created_at")
    ).filter(
        col("page_id").isNotNull() & 
        col("domain").isNotNull() & 
        col("user_id").isNotNull()
    )

    query = final_stream.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .outputMode("append") \
        .trigger(processingTime="1 minute") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()