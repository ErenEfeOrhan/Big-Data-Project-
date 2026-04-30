# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_timestamp, when


BRONZE_PATH = "/home/jovyan/work/delta/bronze/spotify_tracks"
SILVER_PATH = "/home/jovyan/work/delta/silver/spotify_tracks"
CHECKPOINT_PATH = "/home/jovyan/work/spark/checkpoints/silver_spotify_tracks"


spark = (
    SparkSession.builder
    .appName("Spotify Bronze To Silver Delta")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


bronze_df = (
    spark.readStream
    .format("delta")
    .load(BRONZE_PATH)
)


silver_df = (
    bronze_df
    .withColumn("event_timestamp", to_timestamp(col("event_time")))
    .filter(col("track_id").isNotNull())
    .filter(col("track_name").isNotNull())
    .filter(col("artists").isNotNull())
    .filter(col("popularity").isNotNull())
    .filter((col("popularity") >= 0) & (col("popularity") <= 100))
    .filter((col("duration_ms").isNotNull()) & (col("duration_ms") > 0))
    .filter((col("danceability").isNotNull()) & (col("danceability") >= 0) & (col("danceability") <= 1))
    .filter((col("energy").isNotNull()) & (col("energy") >= 0) & (col("energy") <= 1))
    .filter((col("valence").isNotNull()) & (col("valence") >= 0) & (col("valence") <= 1))
    .filter((col("tempo").isNotNull()) & (col("tempo") > 0))
    .dropDuplicates(["track_id", "event_time"])
    .withColumn(
        "explicit_int",
        when(col("explicit") == True, 1).otherwise(0)
    )
    .withColumn("silver_ingestion_time", current_timestamp())
)


query = (
    silver_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="10 seconds")
    .start(SILVER_PATH)
)

print("Silver streaming started.")
print(f"Bronze Delta path: {BRONZE_PATH}")
print(f"Silver Delta path: {SILVER_PATH}")
print("The stream will run for 60 seconds for this test.")

query.awaitTermination(60)

print("Stopping Silver streaming test.")
query.stop()
spark.stop()
