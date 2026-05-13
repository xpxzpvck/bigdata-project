import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count, sum, when, 
    collect_list, struct, to_json, lit, date_format, round, row_number
)
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from pyspark.sql.window import Window

MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
KAFKA_BROKER = "kafka:9092"

INPUT_TOPIC = "raw-page-creates"
OUTPUT_TOPIC = "bot-alerts"

# Нам потрібні два різні чекпоїнти для двох паралельних запитів
CHECKPOINT_PATH_STATS = "s3a://wikipedia-batch-data/checkpoints/bot_stats"
CHECKPOINT_PATH_SPAM = "s3a://wikipedia-batch-data/checkpoints/bot_spam"

def process_domain_stats(df, _):
    """
    Обробляє кожен завершений мікро-батч (1 хвилина).
    Тут ми можемо використовувати звичайні Batch-функції (включно з сортуванням).
    """
    df.persist()
    
    # 1. Рахуємо загальну статистику по домену
    domain_agg = df.groupBy("window", "domain").agg(
        sum(when(col("user_is_bot") == True, col("user_count")).otherwise(0)).alias("bot_count"),
        sum(when(col("user_is_bot") == False, col("user_count")).otherwise(0)).alias("human_count")
    ).withColumn(
        "total_count", col("bot_count") + col("human_count")
    ).withColumn(
        "bot_percentage", round((col("bot_count") / col("total_count")) * 100, 2)
    )
    
    # 2. Знаходимо Топ-5 для ботів і людей за допомогою Window function
    w = Window.partitionBy("window", "domain", "user_is_bot").orderBy(col("user_count").desc())
    ranked_df = df.withColumn("rn", row_number().over(w)).filter(col("rn") <= 5)
    
    top_bots = ranked_df.filter(col("user_is_bot") == True) \
        .groupBy("window", "domain").agg(collect_list("user_text").alias("top_bots"))
        
    top_humans = ranked_df.filter(col("user_is_bot") == False) \
        .groupBy("window", "domain").agg(collect_list("user_text").alias("top_humans"))
        
    # 3. Збираємо все в одну підсумкову таблицю
    final_df = domain_agg \
        .join(top_bots, ["window", "domain"], "left") \
        .join(top_humans, ["window", "domain"], "left") \
        .select(
            col("domain"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("human_count").cast("int"),
            col("bot_count").cast("int"),
            col("bot_percentage"),
            col("top_humans"),
            col("top_bots")
        )
        
    # --- ЗАПИС У CASSANDRA ---
    final_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "wikipedia_analytics") \
        .option("table", "bot_activity_metrics") \
        .mode("append") \
        .save()
        
    # --- ЗАПИС АЛЕРТІВ (>80%) У KAFKA ---
    # Захист: генеруємо алерт тільки якщо створено хоча б 5 сторінок (відсікаємо мікродомени)
    alerts_df = final_df.filter((col("bot_percentage") > 80.0) & (col("total_count") >= 5))
    
    if not alerts_df.isEmpty():
        alerts_df.select(
            to_json(struct(
                date_format(col("window_end"), "yyyy-MM-dd HH:mm:ss").alias("alert_time"),
                lit("high_bot_activity").alias("alert_type"),
                col("domain"),
                col("bot_percentage"),
                col("bot_count"),
                col("human_count")
            )).alias("value")
        ).write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("topic", OUTPUT_TOPIC) \
            .save()
            
    df.unpersist()


def main():
    spark = SparkSession.builder \
        .appName("BotActivityJob") \
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
            col("data.meta.domain").alias("domain"),
            col("data.performer.user_text").alias("user_text"),
            col("data.performer.user_is_bot").alias("user_is_bot"),
            col("event_time")
        ) \
        .filter(col("domain").isNotNull() & col("user_text").isNotNull())

    # =========================================================================
    # ЗАПИТ 1: Статистика по хвилинах та Топ-5 (Append Mode)
    # =========================================================================
    # Групуємо користувачів, щоб foreachBatch отримав датафрейм з їхніми каунтами
    user_counts_stream = parsed_stream \
        .withWatermark("event_time", "1 minute") \
        .groupBy(
            window(col("event_time"), "1 minute"),
            col("domain"),
            col("user_is_bot"),
            col("user_text")
        ) \
        .agg(count("*").alias("user_count"))
        
    query_stats = user_counts_stream.writeStream \
        .foreachBatch(process_domain_stats) \
        .outputMode("append") \
        .option("checkpointLocation", CHECKPOINT_PATH_STATS) \
        .trigger(processingTime="1 minute") \
        .start()

    # =========================================================================
    # ЗАПИТ 2: Алерт на спам-ботів (>50 сторінок за 10 хв) (Update Mode)
    # =========================================================================
    # Update Mode тут ідеальний, бо ми хочемо отримати алерт ОДРАЗУ, як тільки 
    # лічильник перетне 50, а не чекати 10 хвилин, поки вікно закриється.
    spammer_stream = parsed_stream \
        .filter(col("user_is_bot") == True) \
        .withWatermark("event_time", "2 minutes") \
        .groupBy(
            window(col("event_time"), "10 minutes", "1 minute"),
            col("domain"),
            col("user_text")
        ) \
        .agg(count("*").alias("pages_created")) \
        .filter(col("pages_created") > 50)
        
    query_spam = spammer_stream \
        .select(
            to_json(struct(
                date_format(col("window.end"), "yyyy-MM-dd HH:mm:ss").alias("alert_time"),
                lit("bot_spam").alias("alert_type"),
                col("domain"),
                col("user_text").alias("bot_name"),
                col("pages_created")
            )).alias("value")
        ).writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", OUTPUT_TOPIC) \
        .option("checkpointLocation", CHECKPOINT_PATH_SPAM) \
        .outputMode("update") \
        .trigger(processingTime="1 minute") \
        .start()

    # Чекаємо на завершення ОБОХ потоків
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()