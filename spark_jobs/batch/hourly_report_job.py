import os
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum, when, expr, date_trunc,
    collect_list, struct, to_json, round, row_number, lit
)
from pyspark.sql.window import Window

MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

# Шлях до збережених даних з save_raw_job.py
RAW_DATA_PATH = "s3a://wikipedia-batch-data/raw"

def main():
    spark = SparkSession.builder \
        .appName("HourlyActivityBatchJob") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 1. Визначаємо часові рамки: останні 6 повних годин
    now = datetime.now(timezone.utc)
    # Обрізаємо хвилини та секунди (напр. 14:32 -> 14:00)
    current_hour_start = now.replace(minute=0, second=0, microsecond=0)
    six_hours_ago = current_hour_start - timedelta(hours=6)

    print(f"Analyzing data from {six_hours_ago} to {current_hour_start}")

    # 2. Читаємо всі історичні дані з Data Lake
    try:
        df = spark.read.parquet(RAW_DATA_PATH)
    except Exception as e:
        print(f"No data found in {RAW_DATA_PATH} or MinIO unavailable. Exiting.")
        return

    # 3. Фільтруємо дані за наш 6-годинний проміжок
    df_filtered = df.filter(
        (col("event_time") >= lit(six_hours_ago)) &
        (col("event_time") < lit(current_hour_start))
    )

    if df_filtered.isEmpty():
        print("No records found for the specified 6-hour window.")
        return

    # Округлюємо час кожної події до початку її години (щоб згрупувати)
    df_hourly = df_filtered.withColumn("time_start", date_trunc("hour", col("event_time")))

    # 4. Рахуємо активність користувачів (основа для Топ-10 та унікальних авторів)
    user_counts = df_hourly.groupBy("domain", "time_start", "user_name", "user_is_bot").agg(
        count("*").alias("user_pages")
    )

    # 5. Загальна статистика по домену за годину
    domain_stats = user_counts.groupBy("domain", "time_start").agg(
        sum("user_pages").cast("integer").alias("pages_created"),
        count("user_name").cast("integer").alias("unique_authors"),
        sum(when(col("user_is_bot") == True, col("user_pages")).otherwise(0)).alias("bot_pages")
    ).withColumn(
        "bot_percent", round((col("bot_pages") / col("pages_created")) * 100, 2)
    ).withColumn(
        "time_end", col("time_start") + expr("INTERVAL 1 HOUR")
    ).drop("bot_pages")

    # 6. Знаходимо Топ-10 авторів (завдяки Batch-режиму ми можемо використати Window!)
    w = Window.partitionBy("domain", "time_start").orderBy(col("user_pages").desc())
    ranked_users = user_counts.withColumn("rn", row_number().over(w)).filter(col("rn") <= 10)

    # Збираємо Топ-10 у JSON-рядок для зручності REST API
    top_authors_df = ranked_users.withColumn(
        "author_struct",
        struct(
            col("user_name").alias("name"),
            col("user_pages").cast("integer").alias("pages"),
            col("user_is_bot").alias("is_bot")
        )
    ).groupBy("domain", "time_start").agg(
        to_json(collect_list("author_struct")).alias("top_authors")
    )

    # 7. Об'єднуємо загальну статистику і Топ-10
    final_df = domain_stats.join(top_authors_df, ["domain", "time_start"]) \
        .select(
            "domain",
            "time_start",
            "time_end",
            "pages_created",
            "unique_authors",
            "bot_percent",
            "top_authors"
        )

    # 8. Пишемо готовий звіт у Cassandra
    final_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "wikipedia_analytics") \
        .option("table", "hourly_activity_report") \
        .mode("append") \
        .save()

    print("Batch Job completed successfully!")

if __name__ == "__main__":
    main()