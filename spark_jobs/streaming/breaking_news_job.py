import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count, when, expr,
    collect_list, struct, to_json, lit, date_format, round
)
from pyspark.sql.types import StructType, StructField, StringType

# 1. Читаємо конфігурацію MinIO та Kafka
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
KAFKA_BROKER = "kafka:9092"

INPUT_TOPIC = "raw-page-creates"
OUTPUT_TOPIC = "breaking-news-alerts"
CHECKPOINT_PATH = "s3a://wikipedia-batch-data/checkpoints/breaking_news"

def main():
    # 2. Налаштовуємо Spark з підключенням до S3 (MinIO)
    spark = SparkSession.builder \
        .appName("BreakingNewsJob") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 3. Читаємо потік
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    schema = StructType([
        StructField("page_title", StringType(), True),
        StructField("meta", StructType([
            StructField("domain", StringType(), True)
        ]), True)
    ])

    # 4. Парсимо та чистимо від службових сторінок (File:, Category:, User:)
    parsed_stream = raw_stream \
        .select(
            from_json(col("value").cast("string"), schema).alias("data"),
            col("timestamp").alias("event_time")
        ) \
        .select(
            col("data.page_title").alias("page_title"),
            col("data.meta.domain").alias("domain"),
            col("event_time")
        ) \
        .filter(col("page_title").isNotNull()) \
        .filter(col("domain").isNotNull())

    # 5. Sliding Window 1 година (зсув 5 хвилин)
    # ФІКС: Обходимо заборону Spark. Збираємо всі події в масив і рахуємо їх відразу після agg()
    agg_stream = parsed_stream \
        .withWatermark("event_time", "5 minutes") \
        .groupBy(
            window(col("event_time"), "1 hour", "5 minutes").alias("time_window"),
            col("domain")
        ) \
        .agg(
            count("page_title").alias("pages_last_1hour"),
            collect_list(struct(col("event_time"), col("page_title"))).alias("all_events")
        ) \
        .withColumn("recent_events", expr("filter(all_events, e -> e.event_time >= time_window.end - interval 5 minutes)")) \
        .withColumn("pages_last_5min", expr("size(recent_events)")) \
        .withColumn("sample_pages", expr("slice(transform(recent_events, e -> e.page_title), 1, 5)")) \
        .drop("all_events", "recent_events")

    # 6. Математика: Рахуємо baseline та spike_ratio
    alert_stream = agg_stream \
        .withColumn("avg_pages_per_5min", round((col("pages_last_1hour") - col("pages_last_5min")) / 11, 2)) \
        .withColumn("avg_pages_per_5min", when(col("avg_pages_per_5min") <= 0, 1).otherwise(col("avg_pages_per_5min"))) \
        .withColumn("spike_ratio", round(col("pages_last_5min") / col("avg_pages_per_5min"), 2)) \
        .filter(col("spike_ratio") > 3.0)

    # 7. Форматування JSON
    output_stream = alert_stream \
        .select(
            to_json(struct(
                date_format(col("time_window.end"), "yyyy-MM-dd HH:mm:ss").alias("alert_time"),
                lit("activity_spike").alias("alert_type"),
                col("domain"),
                col("pages_last_5min").cast("integer").alias("pages_last_5min"),
                col("avg_pages_per_5min").cast("double").alias("avg_pages_per_5min"),
                col("spike_ratio").cast("double").alias("spike_ratio"),
                col("sample_pages")
            )).alias("value")
        )

    # 8. Запис у Kafka
    query = output_stream.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", OUTPUT_TOPIC) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .outputMode("update") \
        .trigger(processingTime="1 minute") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()