# -*- coding: utf-8 -*-

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    when,
    avg,
    count,
    round as spark_round,
)


SILVER_PATH = "/home/jovyan/work/delta/silver/spotify_tracks"
GOLD_PATH = "/home/jovyan/work/delta/gold/spotify_analytics"
CHECKPOINT_PATH = "/home/jovyan/work/spark/checkpoints/gold_spotify_analytics"
RUN_SECONDS = int(os.getenv("RUN_SECONDS", "0"))


spark = (
    SparkSession.builder
    .appName("Spotify Silver To Gold Delta")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


silver_df = (
    spark.readStream
    .format("delta")
    .load(SILVER_PATH)
)


gold_df = (
    silver_df
    .filter(col("track_genre").isNotNull())
    .filter(col("popularity").isNotNull())
    .filter(col("energy").isNotNull())
    .filter(col("danceability").isNotNull())
    .withColumn(
        "tempo_bucket",
        when(col("tempo") < 90, "slow")
        .when((col("tempo") >= 90) & (col("tempo") < 120), "medium")
        .otherwise("fast")
    )
    .groupBy("track_genre", "tempo_bucket")
    .agg(
        count("*").alias("track_count"),
        spark_round(avg("popularity"), 2).alias("avg_popularity"),
        spark_round(avg("energy"), 4).alias("avg_energy"),
        spark_round(avg("danceability"), 4).alias("avg_danceability"),
    )
    .withColumn("gold_ingestion_time", current_timestamp())
)


query = (
    gold_df.writeStream
    .format("delta")
    .outputMode("complete")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="10 seconds")
    .start(GOLD_PATH)
)

print("Gold streaming started.")
print(f"Silver Delta path: {SILVER_PATH}")
print(f"Gold Delta path: {GOLD_PATH}")
if RUN_SECONDS > 0:
    print(f"The stream will run for {RUN_SECONDS} seconds.")
    query.awaitTermination(RUN_SECONDS)
else:
    print("The stream will run continuously. Press Ctrl+C to stop.")
    query.awaitTermination()

print("Stopping Gold stream.")
query.stop()
spark.stop()
