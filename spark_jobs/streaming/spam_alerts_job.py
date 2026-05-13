import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count, expr, when,
    collect_set, collect_list, struct, to_json, date_format
)
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
KAFKA_BROKER = "kafka:9092"

INPUT_TOPIC = "raw-page-creates"
OUTPUT_TOPIC = "spam-alerts"
CHECKPOINT_PATH = "s3a://wikipedia-batch-data/checkpoints/spam_alerts"

def main():
    spark = SparkSession.builder \
        .appName("SpamAlertsJob") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 1. Читаємо з Kafka
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
        .filter(col("user_name").isNotNull()) \
        .filter(col("user_is_bot") == False) # Фокус тільки на людях

    # 2. Агрегація за 5 хвилин
    user_agg_stream = parsed_stream \
        .withWatermark("event_time", "2 minutes") \
        .groupBy(
            window(col("event_time"), "5 minutes"),
            col("user_name")
        ) \
        .agg(
            count("*").cast("integer").alias("page_count"),
            collect_set("domain").alias("domains"),
            collect_list("page_title").alias("titles")
        )

    # 3. Виявлення 4-х патернів
    rules_stream = user_agg_stream \
        .withColumn("is_high_velocity", col("page_count") > 10) \
        .withColumn("is_cross_wiki", expr("size(domains) > 1")) \
        .withColumn("has_suspicious_title", expr(r"exists(titles, t -> t rlike '(http|www\\.|\\d{6,})')")) \
        .withColumn("has_anomalous_length", expr(r"exists(titles, t -> length(t) < 3 OR length(t) > 80)")) \
        .withColumn("window_start", col("window.start"))

    # 4. Визначаємо Severity та виводимо в JSON
    alert_stream = rules_stream \
        .withColumn("severity",
            when(col("has_suspicious_title") | (col("is_high_velocity") & col("is_cross_wiki")), "high")
            .when(col("is_high_velocity") | col("is_cross_wiki"), "medium")
            .when(col("has_anomalous_length"), "low")
            .otherwise("none")
        ) \
        .filter(col("severity") != "none") \
        .withColumn("reasons", expr("""
            filter(array(
                case when is_high_velocity then 'High Velocity' else null end,
                case when is_cross_wiki then 'Cross-wiki activity' else null end,
                case when has_suspicious_title then 'Suspicious title' else null end,
                case when has_anomalous_length then 'Anomalous length' else null end
            ), x -> x is not null)
        """)) \
        .select(
            to_json(struct(
                date_format(col("window_start"), "yyyy-MM-dd HH:mm:ss").alias("alert_time"),
                col("user_name"),
                col("severity"),
                col("page_count"),
                col("domains"),
                col("reasons"),
                col("titles")
            )).alias("value")
        )

    # 5. Прямий запис у Kafka (без foreachBatch)
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