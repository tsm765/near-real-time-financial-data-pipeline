import os
import sys
import time
import json
from datetime import datetime, timezone

from dotenv import load_dotenv
from kafka import KafkaConsumer

# ----------------------------------------------------
# 1. Compute project root and put it on sys.path
# ----------------------------------------------------
BASE_DIR = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

# ----------------------------------------------------
# 2. Load .env BEFORE importing storage.*
# ----------------------------------------------------
ENV_PATH = os.path.join(BASE_DIR, "configs", ".env")
load_dotenv(ENV_PATH)

print("[DEBUG] AZURE_STORAGE_CONNECTION_STRING =", os.getenv("AZURE_STORAGE_CONNECTION_STRING"))
print("[DEBUG] BASE_DIR         =", BASE_DIR)
print("[DEBUG] ENV_PATH        =", ENV_PATH)
print("[DEBUG] STORAGE_MODE env =", os.getenv("STORAGE_MODE"))
print("[DEBUG] LOCAL_DATA_DIR env =", os.getenv("LOCAL_DATA_DIR"))

# Now it's safe to import storage router (it will see STORAGE_MODE)
from storage.router import write_curated_unified  # type: ignore
from storage import config as storage_config

print("[DEBUG] storage.config.STORAGE_MODE   =", storage_config.STORAGE_MODE)
print("[DEBUG] storage.config.LOCAL_DATA_DIR =", storage_config.LOCAL_DATA_DIR)

# ----------------------------------------------------
# 3. Buffer & flush config
# ----------------------------------------------------
BUFFER: list[dict] = []
MAX_BUFFER_EVENTS = 1        # can tweak later
FLUSH_EVERY_SECONDS = 1        # set low for testing; we can raise to 60 later
last_flush = time.time()


def maybe_flush(force: bool = False) -> None:
    """
    Decide whether to flush the in-memory buffer to storage.
    Uses event count, time since last flush, or a forced flush.
    """
    global BUFFER, last_flush

    if not BUFFER:
        return

    now = time.time()
    should_flush = force or (
        len(BUFFER) >= MAX_BUFFER_EVENTS
        or (now - last_flush) >= FLUSH_EVERY_SECONDS
    )

    if not should_flush:
        return

    batch = BUFFER
    BUFFER = []
    last_flush = now

    try:
        target = write_curated_unified(batch)
        if target:
            print(f"[storage] wrote {len(batch)} events -> {target}")
        else:
            print(f"[storage] STORAGE_MODE=none, skipped write, batch size={len(batch)}")
    except Exception as e:
        # Don't crash the whole consumer on a storage error; log and continue.
        print(f"[storage] ERROR while writing batch of {len(batch)} events: {e}")


# ----------------------------------------------------
# 4. Kafka config (env already loaded)
# ----------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CONSUMER_GROUP_ID = os.getenv("CONSUMER_GROUP_ID", "stream-consumer-group")

PRICE_TOPIC = os.getenv("PRICE_TOPIC", "prices")
MENTIONS_TOPIC = os.getenv("MENTIONS_TOPIC", "mentions")

TOPICS = [PRICE_TOPIC, MENTIONS_TOPIC]


def normalize_timestamp(ts_str: str | None) -> str:
    """
    Ensure timestamp is ISO-8601 in UTC.
    We accept strings like '2025-01-15T14:20:00Z' or already-ISO ones.
    If parsing fails, we fall back to 'now'.
    """
    if not ts_str:
        return datetime.now(timezone.utc).isoformat()

    try:
        # Handle trailing 'Z' by converting to +00:00
        if ts_str.endswith("Z"):
            ts_str = ts_str.replace("Z", "+00:00")

        dt = datetime.fromisoformat(ts_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)

        return dt.isoformat()
    except Exception:
        return datetime.now(timezone.utc).isoformat()


def normalize_event(topic: str, payload: dict) -> dict | None:
    """
    Take raw payload from 'prices' or 'mentions' topic and return a unified structure.
    Returns None if the payload is invalid or missing critical fields.
    """
    base = {
        "topic": topic,
        "ticker": payload.get("ticker"),
        "source": payload.get("source"),
        "ingest_time": datetime.now(timezone.utc).isoformat(),
    }

    if not base["ticker"]:
        print(f"[WARN] Dropping event with no ticker: {payload}")
        return None

    if topic == PRICE_TOPIC:
        price = payload.get("price")
        if price is None:
            print(f"[WARN] Price event without 'price': {payload}")
            return None

        base.update(
            {
                "event_type": "price",
                "price": float(price),
                "mentions": None,
                "timestamp": normalize_timestamp(payload.get("timestamp")),
            }
        )
        return base

    if topic == MENTIONS_TOPIC:
        mentions = payload.get("mentions")
        if mentions is None:
            print(f"[WARN] Mentions event without 'mentions': {payload}")
            return None

        base.update(
            {
                "event_type": "mention",
                "price": None,
                "mentions": int(mentions),
                "timestamp": normalize_timestamp(payload.get("timestamp")),
                "news_source": payload.get("news_source"),
            }
        )
        return base

    print(f"[WARN] Unknown topic '{topic}', skipping.")
    return None


def main():
    print("[INFO] Starting unified stream consumer...")
    print(f"[INFO] Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"[INFO] Topics: {TOPICS}")
    print(f"[INFO] Consumer group: {CONSUMER_GROUP_ID}")

    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP_ID,
        auto_offset_reset="earliest",  # start from beginning if no committed offsets
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    try:
        for msg in consumer:
            payload = msg.value
            topic = msg.topic

            normalized_event = normalize_event(topic, payload)
            if normalized_event is None:
                continue  # skip invalid events

            # Add to buffer
            BUFFER.append(normalized_event)

            # Decide whether to flush to storage
            maybe_flush(force=True)

    except KeyboardInterrupt:
        print("\n[INFO] Stopping consumer (keyboard interrupt).")
    finally:
        # Close consumer & force a final flush
        consumer.close()
        print("[INFO] Consumer closed.")
        maybe_flush(force=True)


if __name__ == "__main__":
    main()
