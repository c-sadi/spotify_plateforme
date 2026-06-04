"""
Spark Job : streaming_trends_job
==================================
Consomme le topic Kafka `listening_events` et produit en continu
les tendances musicales temps réel.


Outputs :
    - PostgreSQL → table `realtime_top_tracks` (top 10 par fenêtre de 5 min)
    Redis       → clé `top_tracks:live` (top genres par sliding window)


Lancement :
    spark-submit \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\\
                   org.postgresql:postgresql:42.7.1 \\
        spark_jobs/streaming_trends_job.py


TODO :
    [x] Implémenter la lecture du topic Kafka avec readStream
    [x] Désérialiser les messages JSON avec le bon schéma
    [ ] Implémenter les fenêtres tumbling de 5 minutes
    [ ] Implémenter les sliding windows pour les genres (15 min / 5 min)
    [ ] Configurer le checkpoint sur MinIO
    [ ] Écrire les résultats dans PostgreSQL et Redis
"""


import os
import redis 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, BooleanType, TimestampType
)

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP",  "kafka-1:9092")
KAFKA_TOPIC      = "listening_events"
CHECKPOINT_PATH  = "s3a://spotify-checkpoints/streaming_trends"
POSTGRES_URL     = os.getenv("SPOTIFY_POSTGRES_URL", "jdbc:postgresql://postgres:5432/spotify")
POSTGRES_PROPS   = {
    "user":    "spotify",
    "password": "spotify",
    "driver": "org.postgresql.Driver",
}

LISTENING_EVENT_SCHEMA = StructType([
    StructField("event_id",    StringType(),    False),
    StructField("user_id",     StringType(),    False),
    StructField("track_id",    StringType(),    False),
    StructField("source_peer", StringType(),    True),
    StructField("timestamp",   StringType(),    False),
    StructField("duration_ms", IntegerType(),   True),
    StructField("device_type", StringType(),    True),
    StructField("geo_country", StringType(),    True),
    StructField("completed",   BooleanType(),   True),
    StructField("event_source",StringType(),    True),
])

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("SPOTIFY-streaming-trends")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.statefulOperator.allowMultiple", "true")
        .config("spark.hadoop.fs.s3a.endpoint",           "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key",         "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key",         "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access",   "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

def read_kafka_stream(spark: SparkSession):
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    return kafka_df.selectExpr("CAST(value AS STRING) as value") \
        .select(F.from_json(F.col("value"), LISTENING_EVENT_SCHEMA).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", F.to_timestamp(F.col("timestamp")))

def write_to_postgres(batch_df, batch_id):
    from pyspark.sql.window import Window
    window_spec = Window.partitionBy("window_start", "window_end").orderBy(F.col("stream_count").desc())
    top_10_df = batch_df.withColumn("rank", F.row_number().over(window_spec)).filter(F.col("rank") <= 10).drop("rank")
    print(f" [Batch {batch_id}] Top 10 Tumbling calculé.")
    top_10_df.show(truncate=False)

def compute_top_tracks_tumbling(events_df, spark):
    tracks_df = (spark.read.format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", "tracks")
        .option("user", POSTGRES_PROPS["user"])
        .option("password", POSTGRES_PROPS["password"])
        .option("driver", POSTGRES_PROPS["driver"])
        .load().select("id"))
    
    valid_events_df = events_df.join(tracks_df, events_df.track_id == tracks_df.id, "left").drop("id")
    return (
        valid_events_df
        .groupBy(F.window(F.col("event_time"), "5 minutes"), F.col("track_id"))
        .count()
        .select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"), F.col("track_id"), F.col("count").alias("stream_count"))
    )

def compute_genre_listeners_sliding(events_df, spark):
    tracks_df = spark.read.format("jdbc").option("url", POSTGRES_URL).option("dbtable", "tracks").option("user", POSTGRES_PROPS["user"]).option("password", POSTGRES_PROPS["password"]).option("driver", POSTGRES_PROPS["driver"]).load()
    artists_df = spark.read.format("jdbc").option("url", POSTGRES_URL).option("dbtable", "artists").option("user", POSTGRES_PROPS["user"]).option("password", POSTGRES_PROPS["password"]).option("driver", POSTGRES_PROPS["driver"]).load()
    
    full_data = events_df.join(tracks_df, events_df.track_id == tracks_df.id, "left").join(artists_df, tracks_df.artist_id == artists_df.id, "left")
    enriched_df = full_data.withColumn("single_genre", F.explode(F.col("genres")))
    
    return (
        enriched_df
        .groupBy(F.window(F.col("event_time"), "15 minutes", "5 minutes"), F.col("single_genre"))
        .count()
        .select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"), F.col("single_genre").alias("genre"), F.col("count").alias("genre_count"))
    )

def write_to_redis(batch_df, batch_id):
    r = redis.Redis(host='redis', port=6379, db=0)
    for row in batch_df.collect():
        r.set(f"genre_listeners:{row['genre']}", row['genre_count'])

def write_late_events(df):
    return df.select(F.to_json(F.struct("*")).alias("value")).writeStream \
        .format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP).option("topic", "late_listening_events") \
        .option("checkpointLocation", f"{CHECKPOINT_PATH}_late_v6").start()

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("🚀 Démarrage streaming_trends_job avec Watermarking...")
    events_df = read_kafka_stream(spark)
    watermarked_df = events_df.withWatermark("event_time", "10 minutes")

    late_events_df = watermarked_df.filter(F.col("event_time") < F.current_timestamp() - F.expr("INTERVAL 10 MINUTES"))
    valid_events_df = watermarked_df.filter(F.col("event_time") >= F.current_timestamp() - F.expr("INTERVAL 10 MINUTES"))

    write_late_events(late_events_df)
    
    compute_top_tracks_tumbling(valid_events_df, spark).writeStream \
        .outputMode("append").foreachBatch(write_to_postgres) \
        .option("checkpointLocation", f"{CHECKPOINT_PATH}_pg_v6").trigger(processingTime="10 seconds").start()

    compute_genre_listeners_sliding(valid_events_df, spark).writeStream \
        .outputMode("update").foreachBatch(write_to_redis) \
        .option("checkpointLocation", f"{CHECKPOINT_PATH}_redis_v6").trigger(processingTime="10 seconds").start()

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main() 


