import json, time, schedule, os, csv
from confluent_kafka import Producer
from dide.config.kafka.noaa_kafka_config import NOAA_PRODUCER_CONFIG, NOAA_TOPIC
from dide.config.api.noaa_api_config import (
    NOAA_BASE_URL,
    NOAA_REQUEST,
    NOAA_PARAMS,
    NOAA_FETCH_INTERVAL_MINUTES
)
# NOAA_URL, NOAA_REQUEST
import requests
from dotenv import load_dotenv

load_dotenv()

LOCAL_FALLBACK = "/workspaces/WildFire/sahel.csv"

BATCH_SIZE  = 500
BATCH_DELAY = 0.1
MAX_RECORDS = 10000

producer = Producer({
    **NOAA_PRODUCER_CONFIG,
    "queue.buffering.max.messages": 100000,
    "queue.buffering.max.kbytes":   512000,
    "batch.num.messages":           500,
    "linger.ms":                    50,
})

def delivery_report(err, msg):
    if err:
        print(f"[NOAA] Delivery failed: {err}")
def parse_csv_file(path: str):
    with open(path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            record = {}
            for k, v in row.items():
                clean_key = k.strip().replace('"', '')  # keep uppercase
                cleaned = v.strip()                     # ← strip spaces
                if cleaned in {"9999.9", "999.9", "99.99", "999", "9999", ""}:
                    record[clean_key] = None
                else:
                    try:
                        record[clean_key] = float(cleaned)
                    except ValueError:
                        record[clean_key] = cleaned
            record["ingested_at"] = float(time.time())  # ← float not str
            yield record

def try_fetch_api() -> tuple[bool, list]:
    """
    Eagerly fetch from API — returns (success, records).
    NOT a generator so exceptions are caught immediately.
    """
    try:
        resp = requests.get(NOAA_BASE_URL, params=NOAA_PARAMS, timeout=NOAA_REQUEST["timeout"])
        resp.raise_for_status()
        lines = resp.text.strip().split("\n")
        headers = lines[0].split(",")
        records = []
        for line in lines[1:]:
            record = dict(zip(headers, line.split(",")))
            record["ingested_at"] = str(time.time())
            records.append(record)
        return True, records
    except Exception as e:
        print(f"[NOAA] API unreachable ({e.__class__.__name__}) — falling back to local file")
        return False, []

def produce_records(source):
    """Core produce loop — works with any iterable source."""
    sent  = 0
    batch = 0

    for record in source:
        if sent >= MAX_RECORDS:
            print(f"[NOAA] Reached MAX_RECORDS cap ({MAX_RECORDS}) — stopping")
            break

        try:
            producer.produce(
                topic=NOAA_TOPIC,
                key=record.get("acq_date", "").encode("utf-8"),
                value=json.dumps(record).encode("utf-8"),
                callback=delivery_report,
            )
            sent  += 1
            batch += 1

            if batch >= BATCH_SIZE:
                producer.poll(0)
                time.sleep(BATCH_DELAY)
                batch = 0
                print(f"[NOAA] Progress: {sent}/{MAX_RECORDS} records sent...")

        except BufferError:
            print(f"[NOAA] Buffer full at {sent} — waiting 1s...")
            producer.poll(1.0)
            time.sleep(1)

    producer.flush(timeout=30)
    print(f"[NOAA] Done — sent {sent} fire events → {NOAA_TOPIC}")

def fetch_and_produce():
    try:
        # try API first — eagerly, so exception is caught here
        success, records = try_fetch_api()

        if success:
            print(f"[NOAA] Fetched {len(records)} records from API")
            produce_records(iter(records))
        else:
            # stream local file — no RAM issue
            print(f"[NOAA] Loading from local file: {LOCAL_FALLBACK}")
            produce_records(parse_csv_file(LOCAL_FALLBACK))

    except FileNotFoundError:
        print(f"[NOAA] Local file not found: {LOCAL_FALLBACK}")
        print(f"        Place your CSV at: {LOCAL_FALLBACK}")
    except Exception as e:
        print(f"[NOAA] Unexpected error: {e}")

schedule.every(NOAA_FETCH_INTERVAL_MINUTES).minutes.do(fetch_and_produce)

if __name__ == "__main__":
    fetch_and_produce()
    while True:
        schedule.run_pending()
        time.sleep(30)