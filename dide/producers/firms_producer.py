import json, time, schedule, os, csv
from confluent_kafka import Producer
from dide.config.kafka.firms_kafka_config import FIRMS_PRODUCER_CONFIG, FIRMS_TOPIC
from dide.config.api.firms_api_config import (
    FIRMS_URL,
    FIRMS_REQUEST,
    FIRMS_FETCH_INTERVAL_MINUTES
)
# FIRMS_URL, FIRMS_REQUEST
import requests
from dotenv import load_dotenv

load_dotenv()

LOCAL_FALLBACK = "/workspaces/WildFire/fire_archive_SV-C2_745039.csv"

BATCH_SIZE  = 500
BATCH_DELAY = 0.1
MAX_RECORDS = 10000

producer = Producer({
    **FIRMS_PRODUCER_CONFIG,
    "queue.buffering.max.messages": 100000,
    "queue.buffering.max.kbytes":   512000,
    "batch.num.messages":           500,
    "linger.ms":                    50,
})

def delivery_report(err, msg):
    if err:
        print(f"[FIRMS] Delivery failed: {err}")

def parse_csv_file(path: str):
    """Stream local CSV row by row — memory efficient for 1.2M records."""
    with open(path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row["ingested_at"] = str(time.time())
            yield row

#### IF WE WANTED TO FETCH FROM THE API INSTEAD OF A LOCAL FILE, WE COULD UNCOMMENT THIS FUNCTION AND THE RELEVANT PARTS OF fetch_and_produce() BELOW.
def try_fetch_api() -> tuple[bool, list]:
    """
    Eagerly fetch from API — returns (success, records).
    NOT a generator so exceptions are caught immediately.
    """
    try:
        resp = requests.get(FIRMS_URL, timeout=FIRMS_REQUEST["timeout"])
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
        print(f"[FIRMS] API unreachable ({e.__class__.__name__}) — falling back to local file")
        return False, []

def produce_records(source):
    """Core produce loop — works with any iterable source."""
    sent  = 0
    batch = 0

    for record in source:
        if sent >= MAX_RECORDS:
            print(f"[FIRMS] Reached MAX_RECORDS cap ({MAX_RECORDS}) — stopping")
            break

        try:
            producer.produce(
                topic=FIRMS_TOPIC,
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
                print(f"[FIRMS] Progress: {sent}/{MAX_RECORDS} records sent...")

        except BufferError:
            print(f"[FIRMS] Buffer full at {sent} — waiting 1s...")
            producer.poll(1.0)
            time.sleep(1)

    producer.flush(timeout=30)
    print(f"[FIRMS] Done — sent {sent} fire events → {FIRMS_TOPIC}")

def fetch_and_produce():
    try:
        
        # try API first — eagerly, so exception is caught here
        success, records = try_fetch_api()

        if success:
            print(f"[FIRMS] Fetched {len(records)} records from API")
            produce_records(iter(records))
        else:
            # stream local file — no RAM issue
            print(f"[FIRMS] Loading from local file: {LOCAL_FALLBACK}")
            produce_records(parse_csv_file(LOCAL_FALLBACK))

    except FileNotFoundError:
        print(f"[FIRMS] Local file not found: {LOCAL_FALLBACK}")
        print(f"        Place your CSV at: {LOCAL_FALLBACK}")
    except Exception as e:
        print(f"[FIRMS] Unexpected error: {e}")

schedule.every(FIRMS_FETCH_INTERVAL_MINUTES).minutes.do(fetch_and_produce)

if __name__ == "__main__":
    fetch_and_produce()
    while True:
        schedule.run_pending()
        time.sleep(30)