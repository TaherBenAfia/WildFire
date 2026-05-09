import os
from dotenv import load_dotenv

load_dotenv()

# force IPv4 — avoid localhost resolving to ::1 on some systems
_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "127.0.0.1:9092")  # ← 127.0.0.1 not localhost

FIRMS_PRODUCER_CONFIG = {
    "bootstrap.servers":                    _BOOTSTRAP,
    "acks":                                 "all",
    "retries":                              3,
    "retry.backoff.ms":                     500,
    "linger.ms":                            10,
    "batch.size":                           16384,
    # force IPv4 resolution
    "broker.address.family":                "v4",     # ← key fix
}

FIRMS_CONSUMER_CONFIG = {
    "bootstrap.servers":                    _BOOTSTRAP,
    "group.id":                             "firms-consumer-group",
    "auto.offset.reset":                    "earliest",
    "enable.auto.commit":                   False,
    "broker.address.family":                "v4",     # ← same fix
}

FIRMS_TOPIC = "climate.fires"

FIRMS_TOPIC_CONFIG = {
    "num_partitions":     3,
    "replication_factor": 1,
}