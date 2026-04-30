# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, when


SILVER_PATH = "/home/jovyan/work/delta/silver/spotify_tracks"
GOLD_PATH = "/home/jovyan/work/delta/gold/spotify_tracks_features"
CHECKPOINT_PATH = "/home/jovyan/work/spark/checkpoints/gold_spotify_tracks_features"


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
    .withColumn("duration_min", col("duration_ms") / 60000)
    .withColumn("mood_score", (col("valence") + col("energy") + col("danceability")) / 3)
    .withColumn("audio_intensity", (col("energy") + col("loudness")) / 2)
    .withColumn("is_high_energy", when(col("energy") >= 0.7, 1).otherwise(0))
    .withColumn("is_danceable", when(col("danceability") >= 0.7, 1).otherwise(0))
    .withColumn(
        "tempo_bucket",
        when(col("tempo") < 90, "slow")
        .when((col("tempo") >= 90) & (col("tempo") < 120), "medium")
        .otherwise("fast")
    )
    .withColumn("gold_ingestion_time", current_timestamp())
)


query = (
    gold_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="10 seconds")
    .start(GOLD_PATH)
)

print("Gold streaming started.")
print(f"Silver Delta path: {SILVER_PATH}")
print(f"Gold Delta path: {GOLD_PATH}")
print("The stream will run for 60 seconds for this test.")

query.awaitTermination(60)

print("Stopping Gold streaming test.")
query.stop()
spark.stop()
