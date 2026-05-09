# -*- coding: utf-8 -*-

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, BooleanType
)


KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "spotify_tracks"

BRONZE_PATH = "/home/jovyan/work/delta/bronze/spotify_tracks"
CHECKPOINT_PATH = "/home/jovyan/work/spark/checkpoints/bronze_spotify_tracks"
RUN_SECONDS = int(os.getenv("RUN_SECONDS", "0"))


spark = (
    SparkSession.builder
    .appName("Spotify Kafka To Bronze Delta")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.2.0"
    )
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


schema = StructType([
    StructField("event_time", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("track_id", StringType(), True),
    StructField("artists", StringType(), True),
    StructField("album_name", StringType(), True),
    StructField("track_name", StringType(), True),
    StructField("track_genre", StringType(), True),
    StructField("popularity", IntegerType(), True),
    StructField("duration_ms", IntegerType(), True),
    StructField("explicit", BooleanType(), True),
    StructField("danceability", DoubleType(), True),
    StructField("energy", DoubleType(), True),
    StructField("key", IntegerType(), True),
    StructField("loudness", DoubleType(), True),
    StructField("mode", IntegerType(), True),
    StructField("speechiness", DoubleType(), True),
    StructField("acousticness", DoubleType(), True),
    StructField("instrumentalness", DoubleType(), True),
    StructField("liveness", DoubleType(), True),
    StructField("valence", DoubleType(), True),
    StructField("tempo", DoubleType(), True),
    StructField("time_signature", IntegerType(), True)
])


raw_stream_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)


parsed_df = (
    raw_stream_df
    .selectExpr(
        "CAST(value AS STRING) AS raw_json",
        "timestamp AS kafka_timestamp",
        "partition",
        "offset"
    )
    .withColumn("data", from_json(col("raw_json"), schema))
)


bronze_df = (
    parsed_df
    .select(
        "raw_json",
        "kafka_timestamp",
        "partition",
        "offset",
        col("data.*")
    )
    .withColumn("ingestion_time", current_timestamp())
)


query = (
    bronze_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="10 seconds")
    .start(BRONZE_PATH)
)

print("Bronze streaming started.")
print(f"Kafka topic: {KAFKA_TOPIC}")
print(f"Bronze Delta path: {BRONZE_PATH}")
if RUN_SECONDS > 0:
    print(f"The stream will run for {RUN_SECONDS} seconds.")
    query.awaitTermination(RUN_SECONDS)
else:
    print("The stream will run continuously. Press Ctrl+C to stop.")
    query.awaitTermination()

print("Stopping Bronze stream.")
query.stop()
spark.stop()
