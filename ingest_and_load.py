import os
import time
import json
from urllib.parse import quote
from datetime import datetime, timezone, timedelta
from confluent_kafka import Consumer, KafkaError, KafkaException
import pandas as pd
from snowflake.snowpark.session import Session

# Database config
CONNECTION_PARAMETERS = {
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'database': os.getenv('SNOWFLAKE_DATABASE'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
}

SNOWFLAKE_TARGET_TABLE = os.getenv('SNOWFLAKE_TARGET_TABLE')

session = Session.builder.configs(CONNECTION_PARAMETERS).create()


# Kafka Consumer Config
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
BIDS_TOPIC = "bids"
RESPONSES_TOPIC = "responses"
RENDERS_TOPIC = "renders"
GROUP_ID = "ingest_and_cache_group"

FLUSH_INTERVAL_SECONDS = 30  # flush every 2 minutes
RENDER_TIMEOUT_SECONDS = 60   # 60s from bid_time for a won bid with no render

cache = {}


# Helper Functions
def now_utc() -> datetime:
    """
    Returns the current time as an offset-aware UTC datetime.
    """
    return datetime.now(timezone.utc)

def parse_datetime_str(dt_str: str) -> datetime:
    """
    Parse an ISO8601 string. If it ends with 'Z', replace with '+00:00'.
    Example: "2025-01-15T16:00:00Z" => "2025-01-15T16:00:00+00:00".
    """
    dt_str = dt_str.replace("Z", "+00:00")
    return datetime.fromisoformat(dt_str)

def ensure_cache_entry(sid: str):
    """
    Create a partial record if 'sid' not in cache, allowing out-of-order messages.
    """
    if sid not in cache:
        cache[sid] = {
            "sid": sid,
            "bid_time": None,
            "won": None,
            "render": None
        }

def is_record_ready(record: dict) -> bool:
    """
    We only load if:
      - 'sid' is present
      - 'bid_time' is known
      - 'won' is False => ready
      - 'won' is True => either render=True or (time since bid_time >= RENDER_TIMEOUT_SECONDS)
    """
    sid = record.get("sid")
    bid_time = record.get("bid_time")
    won_val = record.get("won")

    # Must have a sid + bid_time to do any real logic
    if not sid or not bid_time:
        return False
    if won_val is None:
        return False

    # If not won => we can load as soon as we have them
    if won_val == False:
        return True

    # If won=True => check render or timeout
    if record.get("render") == True:
        return True

    if now_utc() - bid_time >= timedelta(seconds=RENDER_TIMEOUT_SECONDS):
        # Timed out => load with render=0
        return True

    return False

def finalize_record_for_db(record: dict) -> dict:
    """
    Convert our in-memory record into the final row for Snowflake:
      - Ensure 'render' is False instead of None when not provided.
      - If won is True, set render=False as it is irrelevant.
    """
    sid = record["sid"]
    bid_time_obj = record["bid_time"]
    won_val = record["won"]
    render_val = record["render"]

    bid_time_str = bid_time_obj.isoformat() if bid_time_obj else None

    final_render = render_val if render_val is not None else False

    if won_val == False:
        final_render = False

    return {
        "SID": sid,
        "BID_TIME": bid_time_str,
        "WON": won_val,
        "RENDER": final_render
    }

def load_records_to_db(records: list):
    """
    Load data into Snowflake
    """
    if not records:
        return

    try:
        df = pd.DataFrame(records, columns=["SID","BID_TIME","WON","RENDER"])
        session.write_pandas(df, table_name=SNOWFLAKE_TARGET_TABLE)
        print(f"Loaded {len(df)} rows via pandas DataFrame.")
    except Exception as exc:
        print("Error: ", exc)
        raise

# Consumer Logic
def main():
    consumer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest"
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([BIDS_TOPIC, RESPONSES_TOPIC, RENDERS_TOPIC])

    last_flush_time = time.time()

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # No new message
                pass
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    pass
                else:
                    raise KafkaException(msg.error())
            else:
                # Parse the message
                topic = msg.topic()
                data = json.loads(msg.value().decode("utf-8"))
                sid = data["sid"]

                ensure_cache_entry(sid)

                if topic == BIDS_TOPIC:
                    bid_time = parse_datetime_str(data["bid_time"])
                    cache[sid]["bid_time"] = bid_time

                elif topic == RESPONSES_TOPIC:
                    cache[sid]["won"] = data["won"]

                elif topic == RENDERS_TOPIC:
                    cache[sid]["render"] = data["rendered"]

            # Periodically flush
            now_time = time.time()
            if now_time - last_flush_time >= FLUSH_INTERVAL_SECONDS:
                flush_cache()
                last_flush_time = now_time

    except KeyboardInterrupt:
        pass
    finally:
        # Final flush on exit
        flush_cache()
        consumer.close()

def flush_cache():
    """
    Gather & insert ready records, then remove them from the cache.
    """
    ready_records = []
    sids_to_remove = []

    for sid, record in cache.items():
        if is_record_ready(record):
            db_rec = finalize_record_for_db(record)
            # As a sanity check, ensure sid is not empty
            if db_rec["SID"]:
                ready_records.append(db_rec)
            sids_to_remove.append(sid)

    if ready_records:
        load_start = time.time()
        print(f"{datetime.now()} - Loading {len(ready_records)} records...")
        load_records_to_db(ready_records)
        load_end = time.time()
        load_time = load_end - load_start
        print(f"{datetime.now()} - Loaded {len(ready_records)} records in {load_time} seconds")

    for sid in sids_to_remove:
        del cache[sid]

if __name__ == "__main__":
    main()
