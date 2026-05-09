import os
from dotenv import load_dotenv

load_dotenv()

_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "127.0.0.1:9092")  # ← 127.0.0.1 not localhost

NOAA_PRODUCER_CONFIG = {
    "bootstrap.servers":                    _BOOTSTRAP,
    "acks":                                 "all",
    "retries":                              3,
    "retry.backoff.ms":                     500,
    "linger.ms":                            10,
    "batch.size":                           16384,
    # force IPv4 resolution
    "broker.address.family":                "v4",     # ← key fix
}

NOAA_CONSUMER_CONFIG = {
    "bootstrap.servers":   _BOOTSTRAP,
    "group.id":            "noaa-consumer-group",
    "auto.offset.reset":   "earliest",
    "enable.auto.commit":  False,
    "broker.address.family": "v4",    # ← key fix
}

NOAA_TOPIC = "climate.weather"

NOAA_TOPIC_CONFIG = {
    "num_partitions":     2,
    "replication_factor": 1,
}