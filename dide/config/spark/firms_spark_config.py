import os
from dotenv import load_dotenv

load_dotenv()

_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "127.0.0.1:9092")
# ── session ───────────────────────────────────────────────────────────────────
FIRMS_SESSION_CONFIG = {
    "app_name": "WildFire-FIRMS-Stream",
    "master":   "local[*]",
    "driver_memory":   "2g",
    "executor_memory": "2g",
    "shuffle_partitions": "4",
}

# ── kafka connection (Spark side) ─────────────────────────────────────────────
FIRMS_STREAM_CONFIG = {
    "kafka.bootstrap.servers":      _BOOTSTRAP,
    "subscribe":                    "climate.fires",
    "startingOffsets":              "earliest",
    "maxOffsetsPerTrigger":         "1000",
    "kafka.allow.auto.create.topics": "true",
    "failOnDataLoss":               "false",
}

# ── batch read (for historical/training data) ─────────────────────────────────
FIRMS_BATCH_CONFIG = {
    "kafka.bootstrap.servers": _BOOTSTRAP,
    "subscribe":               "climate.fires",
    "startingOffsets":         "earliest",
    "endingOffsets":           "latest",
}

# ── write stream (sink to Delta Lake) ────────────────────────────────────────
FIRMS_SINK_CONFIG = {
    "format":           "delta",
    "output_path":      "data/delta/firms",
    "checkpoint_path":  "data/checkpoints/firms",
    "trigger_interval": "30 seconds",
}

# ── JAR packages ──────────────────────────────────────────────────────────────
FIRMS_JARS = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
    "io.delta:delta-spark_2.12:3.2.0,"
    "org.apache.kafka:kafka-clients:3.5.0"
)