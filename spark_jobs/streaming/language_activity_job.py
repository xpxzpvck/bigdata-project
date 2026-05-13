import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count, count_distinct, avg, when, expr, round,
    length, struct, to_json, lit, date_format, round, expr
)
from pyspark.sql.types import StructType, StructField, StringType

# Конфігурація
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
KAFKA_BROKER = "kafka:9092"

INPUT_TOPIC = "raw-page-creates"
CHECKPOINT_PATH = "s3a://wikipedia-batch-data/checkpoints/language_activity"

def process_batch(df, epoch_id):
    """
    Обробка мікро-батчу: розрахунок метрик, отримання попереднього стану 
    з Cassandra для розрахунку тренду та запис фінального результату.
    """
    df.persist()
    if df.isEmpty():
        df.unpersist()
        return

    spark = df.sparkSession

    # Готуємо наш поточний датафрейм (без стовпця trend)
    current_df = df.select(
        col("domain"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("pages_count").cast("int"),
        col("unique_authors").cast("int"),
        col("avg_title_length")
    )

    try:
        # Читаємо історичні дані з Cassandra
        cassandra_df = spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", "wikipedia_analytics") \
            .option("table", "language_activity") \
            .load()

        # Дістаємо тільки попередню хвилину
        prev_df = cassandra_df.select(
            col("domain").alias("prev_domain"),
            col("window_start").alias("prev_window_start"),
            col("pages_count").alias("prev_count")
        )

        # Джоїнимо поточну хвилину з попередньою (по домену і зміщенню часу)
        joined_df = current_df.join(
            prev_df,
            (current_df.domain == prev_df.prev_domain) & 
            (current_df.window_start - expr("INTERVAL 1 MINUTE") == prev_df.prev_window_start),
            "left"
        )

        # Рахуємо тренд (з обробкою ділення на нуль та пустих значень при першому запуску)
        final_df = joined_df.withColumn(
            "trend_percent",
            when(col("prev_count").isNull() | (col("prev_count") == 0), 0.0)
            .otherwise(round(((col("pages_count") - col("prev_count")) / col("prev_count")) * 100, 2))
        ).drop("prev_domain", "prev_window_start", "prev_count")

    except Exception as e:
        # Якщо таблиці в Кассандрі ще немає або вона порожня (найперший батч)
        final_df = current_df.withColumn("trend_percent", lit(0.0))

    # 1. Пишемо історичні дані з трендом у language_activity
    final_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "wikipedia_analytics") \
        .option("table", "language_activity") \
        .mode("append") \
        .save()

    # 2. Оновлюємо актуальний статус доменів для API (запит C1)
    domain_stats_df = final_df.select(
        col("domain"),
        col("pages_count"),
        col("unique_authors"),
        col("avg_title_length"),
        col("window_end").alias("last_updated")
    )

    domain_stats_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "wikipedia_analytics") \
        .option("table", "domain_stats") \
        .mode("append") \
        .save()

    df.unpersist()

def main():
    spark = SparkSession.builder \
        .appName("LanguageActivityJob") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Схема вхідних даних
    schema = StructType([
        StructField("page_title", StringType(), True),
        StructField("meta", StructType([
            StructField("domain", StringType(), True)
        ]), True),
        StructField("performer", StructType([
            StructField("user_text", StringType(), True)
        ]), True)
    ])

    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", INPUT_TOPIC) \
        .load()

    # Парсинг та підготовка полів
    parsed_stream = raw_stream \
        .select(
            from_json(col("value").cast("string"), schema).alias("data"),
            col("timestamp").alias("event_time")
        ) \
        .select(
            col("data.meta.domain").alias("domain"),
            col("data.performer.user_text").alias("author"),
            col("data.page_title").alias("title"),
            col("event_time")
        ) \
        .filter(col("domain").isNotNull())

    # Агрегація за 1 хвилину
    # Використовуємо approx_count_distinct для швидкодії в унікальних авторах
    agg_stream = parsed_stream \
        .withWatermark("event_time", "1 minute") \
        .groupBy(
            window(col("event_time"), "1 minute"),
            col("domain")
        ) \
        .agg(
            count("*").alias("pages_count"),
            expr("approx_count_distinct(author)").alias("unique_authors"),
            round(avg(length(col("title"))), 2).alias("avg_title_length")
        )

    # Запуск стрімінгу
    query = agg_stream.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .outputMode("update") \
        .trigger(processingTime="1 minute") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()