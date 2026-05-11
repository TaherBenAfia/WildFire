import os
from dotenv import load_dotenv

load_dotenv()

_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "127.0.0.1:9092")
#session
NOAA_SESSION_CONFIG = {
    "app_name": "WildFire-NOAA-Stream",
    "master":   "local[*]",
    "driver_memory":      "1g",   # NOAA volume is lower than FIRMS
    "executor_memory":    "1g",
    "shuffle_partitions": "2",
}

# ── kafka connection (Spark side) ─────────────────────────────────────────────
NOAA_STREAM_CONFIG = {
    "kafka.bootstrap.servers":        _BOOTSTRAP,
    "subscribe":                      "climate.weather",
    "startingOffsets":                "earliest",
    "maxOffsetsPerTrigger":           "500",
    "kafka.allow.auto.create.topics": "true",
    "failOnDataLoss":                 "false",
}

# ── batch read ────────────────────────────────────────────────────────────────
NOAA_BATCH_CONFIG = {
    "kafka.bootstrap.servers": _BOOTSTRAP,
    "subscribe":               "climate.weather",
    "startingOffsets":         "earliest",
    "endingOffsets":           "latest",
}

# ── write stream (sink to Delta Lake) ────────────────────────────────────────
NOAA_SINK_CONFIG = {
    "format":           "delta",
    "output_path":      "data/delta/noaa",
    "checkpoint_path":  "data/checkpoints/noaa",
    "trigger_interval": "60 seconds",   # NOAA updates less frequently
}

# ── JAR packages ──────────────────────────────────────────────────────────────
NOAA_JARS = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
    "io.delta:delta-spark_2.12:3.2.0,"
    "org.apache.kafka:kafka-clients:3.5.0"
)