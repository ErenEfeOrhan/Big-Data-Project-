# -*- coding: utf-8 -*-

import csv
import json
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "spotify_tracks")
DATA_FILE = os.getenv("DATA_FILE", "/app/data/spotify_tracks.csv")
MESSAGES_PER_SECOND = int(os.getenv("MESSAGES_PER_SECOND", "10"))
LOOP_DATASET = os.getenv("LOOP_DATASET", "true").lower() == "true"


def safe_float(value):
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def safe_int(value):
    try:
        if value is None or value == "":
            return None
        return int(float(value))
    except Exception:
        return None


def safe_bool(value):
    if value is None:
        return None
    return str(value).lower() == "true"


def create_event(row):
    return {
        "event_time": datetime.now(timezone.utc).isoformat(),
        "event_type": "track_stream_event",
        "user_id": f"user_{random.randint(1, 5000)}",
        "track_id": row.get("track_id"),
        "artists": row.get("artists"),
        "album_name": row.get("album_name"),
        "track_name": row.get("track_name"),
        "track_genre": row.get("track_genre"),
        "popularity": safe_int(row.get("popularity")),
        "duration_ms": safe_int(row.get("duration_ms")),
        "explicit": safe_bool(row.get("explicit")),
        "danceability": safe_float(row.get("danceability")),
        "energy": safe_float(row.get("energy")),
        "key": safe_int(row.get("key")),
        "loudness": safe_float(row.get("loudness")),
        "mode": safe_int(row.get("mode")),
        "speechiness": safe_float(row.get("speechiness")),
        "acousticness": safe_float(row.get("acousticness")),
        "instrumentalness": safe_float(row.get("instrumentalness")),
        "liveness": safe_float(row.get("liveness")),
        "valence": safe_float(row.get("valence")),
        "tempo": safe_float(row.get("tempo")),
        "time_signature": safe_int(row.get("time_signature"))
    }


def wait_for_kafka():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            print("Kafka connection successful.")
            return producer
        except Exception as error:
            print(f"Kafka is not ready yet. Retrying: {error}")
            time.sleep(5)


def stream_csv_to_kafka():
    producer = wait_for_kafka()
    sleep_time = 1 / MESSAGES_PER_SECOND
    total_sent = 0

    print(f"Data file: {DATA_FILE}")
    print(f"Kafka topic: {KAFKA_TOPIC}")
    print(f"Message speed: {MESSAGES_PER_SECOND} messages per second")

    while True:
        with open(DATA_FILE, mode="r", encoding="utf-8") as file:
            reader = csv.DictReader(file)

            for row in reader:
                event = create_event(row)
                producer.send(KAFKA_TOPIC, value=event)
                total_sent += 1

                if total_sent % MESSAGES_PER_SECOND == 0:
                    producer.flush()
                    print(f"Total sent messages: {total_sent}")

                time.sleep(sleep_time)

        producer.flush()

        if not LOOP_DATASET:
            break

        print("CSV finished. Restarting from the beginning for stream simulation.")


if __name__ == "__main__":
    stream_csv_to_kafka()
