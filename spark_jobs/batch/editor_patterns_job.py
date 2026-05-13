import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, hour, collect_set, count_distinct, 
    lag, unix_timestamp, row_number, round
)
from pyspark.sql.window import Window

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
RAW_DATA_PATH = "s3a://wikipedia-batch-data/raw"

def main():
    spark = SparkSession.builder \
        .appName("EditorBehaviorBatchJob") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 1. Читаємо всі історичні дані
    try:
        df = spark.read.parquet(RAW_DATA_PATH)
    except Exception as e:
        print(f"No data found in {RAW_DATA_PATH}. Exiting.")
        return

    # Відкидаємо ботів (нас цікавить поведінка живих авторів)
    df = df.filter(col("user_is_bot") == False)

    # =========================================================================
    # Розрахунок часу між подіями (Gap)
    # =========================================================================
    # Сортуємо події кожного автора за часом
    w_time = Window.partitionBy("user_name").orderBy("event_time")
    
    # Беремо час попередньої події і віднімаємо від поточного (в секундах)
    df_with_gap = df.withColumn("prev_time", lag("event_time").over(w_time)) \
                    .withColumn("gap_seconds", unix_timestamp("event_time") - unix_timestamp("prev_time"))

    # =========================================================================
    # Базова статистика користувача
    # =========================================================================
    user_stats = df_with_gap.groupBy("user_name").agg(
        count("*").cast("integer").alias("total_pages"),
        round(avg("gap_seconds"), 2).alias("avg_gap_seconds"),
        collect_set(hour("event_time")).alias("active_hours"), # Збираємо унікальні години активності
        count_distinct("domain").cast("integer").alias("domains_count")
    )

    # =========================================================================
    # Спеціалізація (Домінуючий домен)
    # =========================================================================
    # Рахуємо сторінки по кожному домену для кожного автора
    domain_counts = df.groupBy("user_name", "domain").agg(count("*").alias("domain_pages"))
    
    # Ранжуємо домени і беремо той, де найбільше сторінок
    w_domain = Window.partitionBy("user_name").orderBy(col("domain_pages").desc())
    specialization_df = domain_counts.withColumn("rn", row_number().over(w_domain)) \
        .filter(col("rn") == 1) \
        .select("user_name", col("domain").alias("specialization"))

    # =========================================================================
    # Фінальна збірка та фільтрація
    # =========================================================================
    final_df = user_stats.join(specialization_df, "user_name") \
        .filter(col("total_pages") >= 5) # Залишаємо тільки тих, хто створив 5+ сторінок
        
    # Якщо автор створив всі 5 сторінок в одну секунду (буває і таке), gap буде null. Фіксимо це:
    final_df = final_df.fillna({"avg_gap_seconds": 0.0})

    # Запис у Cassandra
    final_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "wikipedia_analytics") \
        .option("table", "editor_patterns") \
        .mode("append") \
        .save()

    print("Editor Behavior Job completed successfully!")

if __name__ == "__main__":
    main()