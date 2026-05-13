import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, explode, split, lower, window,
    count, collect_set, struct, to_json, current_timestamp, lit, slice, length
)
from pyspark.sql.types import StructType, StructField, StringType

MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

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

KAFKA_BROKER = "kafka:9092"
INPUT_TOPIC = "raw-page-creates"
OUTPUT_TOPIC = "breaking-news-alerts"
CHECKPOINT_PATH = "s3a://wikipedia-batch-data/checkpoints/breaking_news"

STOP_WORDS = ["i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", 
              "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", 
              "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", 
              "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", 
              "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", 
              "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", 
              "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", 
              "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how",
              "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only",
              "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"]

def main():
    # 1. Читаємо сирий потік з Kafka
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # 2. Описуємо схему JSON, який ми очікуємо від EventStreams
    schema = StructType([
        StructField("page_title", StringType(), True),
        StructField("meta", StructType([
            StructField("domain", StringType(), True)
        ]), True)
    ])

    # 3. Парсимо JSON та дістаємо потрібні поля. 
    # Замість rev_timestamp використовуємо час потрапляння в Kafka для windowing
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
        .filter(col("page_title").isNotNull())

    # 4. Keyword Tokenization (розбиваємо назву на слова)
    # Використовуємо regex для поділу по пробілах, нижніх підкресленнях та пунктуації
    words_stream = parsed_stream \
        .withColumn("word", explode(split(lower(col("page_title")), r"[\s_\p{Punct}]+"))) \
        .filter(col("word") != "") \
        .filter(length(col("word")) > 2) \
        .filter(~col("word").isin(STOP_WORDS))

    # 5. Sliding Window Aggregation (вікно 10 хв, зсув 1 хв)
    agg_stream = words_stream \
        .withWatermark("event_time", "2 minutes") \
        .groupBy(
            window(col("event_time"), "10 minutes", "1 minute"),
            col("word")
        ) \
        .agg(
            count("page_title").alias("occurrences"),
            collect_set("domain").alias("domains"),
            # Збираємо унікальні назви сторінок, але беремо лише перші 5 для sample_pages
            slice(collect_set("page_title"), 1, 5).alias("sample_pages")
        ) \
        .filter(col("occurrences") >= 5)

    # 6. Форматуємо результат у JSON для відправки в Kafka
    alert_stream = agg_stream \
        .select(
            to_json(struct(
                current_timestamp().cast("string").alias("alert_time"),
                lit("keyword_burst").alias("alert_type"),
                col("word").alias("keyword"),
                col("occurrences"),
                col("domains"),
                col("sample_pages")
            )).alias("value")
        )

    # 7. Записуємо в цільовий топік
    query = alert_stream.writeStream \
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