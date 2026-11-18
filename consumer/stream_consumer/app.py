import os
import signal
import sys
import time
import json
from datetime import datetime, timezone

from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.structs import OffsetAndMetadata, TopicPartition

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
# A batch is written when EITHER limit is hit (whichever comes first).
MAX_BUFFER_EVENTS = int(os.getenv("MAX_BUFFER_EVENTS", "500"))
FLUSH_EVERY_SECONDS = float(os.getenv("FLUSH_EVERY_SECONDS", "60"))
# How long one poll() waits for new messages. Keeps the loop ticking when
# topics are idle, so the time-based flush still fires.
POLL_TIMEOUT_MS = int(os.getenv("POLL_TIMEOUT_MS", "1000"))

# Storage outages: retry with exponential backoff (1s, 2s, 4s ... capped),
# stop reading new messages meanwhile (backpressure), and give up after
# MAX_WRITE_ATTEMPTS failed writes in a row. Giving up exits the consumer
# WITHOUT committing, so Docker restarts it and Kafka re-delivers.
MAX_WRITE_ATTEMPTS = int(os.getenv("MAX_WRITE_ATTEMPTS", "10"))
RETRY_BACKOFF_MAX_SECONDS = float(os.getenv("RETRY_BACKOFF_MAX_SECONDS", "60"))

BUFFER: list[dict] = []
last_flush = time.time()
# Per partition: the next offset to commit, covering only messages we have
# fully handled (buffered, or dropped as invalid) and not yet committed.
uncommitted_offsets: dict[TopicPartition, int] = {}
failed_write_attempts = 0   # consecutive failed writes; > 0 means "storage is failing"
next_retry_at = 0.0
# Set by SIGTERM/SIGINT; the main loop checks it and shuts down cleanly.
stop_requested = False


class StorageUnavailableError(RuntimeError):
    """Raised when storage writes keep failing after MAX_WRITE_ATTEMPTS."""


def _offset_and_metadata(offset: int) -> OffsetAndMetadata:
    # kafka-python >= 2.1 has a third field (leader_epoch); older versions have two.
    if len(OffsetAndMetadata._fields) == 3:
        return OffsetAndMetadata(offset, "", -1)
    return OffsetAndMetadata(offset, "")


def mark_handled(tp: TopicPartition, msg) -> None:
    """Record that this message is fully handled; Kafka commits 'next offset to read'."""
    uncommitted_offsets[tp] = msg.offset + 1


def commit_handled_offsets(consumer) -> bool:
    """
    Commit exactly the offsets in uncommitted_offsets (never the consumer's
    current position, which can be ahead of what we've handled).
    Returns True on success. On failure the offsets are kept and retried at
    the next flush; nothing is lost (worst case: duplicates after a restart).
    """
    global uncommitted_offsets

    if not uncommitted_offsets:
        return True

    # Partitions taken away in a rebalance can't be committed by us anymore;
    # their new owner re-reads them (at-least-once), so just drop them.
    assigned = consumer.assignment()
    to_commit = {
        tp: _offset_and_metadata(offset)
        for tp, offset in uncommitted_offsets.items()
        if tp in assigned
    }

    try:
        if to_commit:
            consumer.commit(offsets=to_commit)
    except Exception as e:
        print(f"[kafka] ERROR committing offsets (will retry next flush): {e}")
        return False

    uncommitted_offsets = {}
    return True


def maybe_flush(consumer, force: bool = False) -> bool:
    """
    Write the buffer to storage if a limit is hit (or force=True),
    then commit the offsets of the messages that were handled.

    Offsets are committed ONLY after a successful write (at-least-once).
    If the write fails, the events stay in the buffer and are retried with
    backoff; after MAX_WRITE_ATTEMPTS failures in a row this raises
    StorageUnavailableError (except on the final, forced flush).

    Returns True if everything handled so far is stored and committed.
    """
    global BUFFER, last_flush, failed_write_attempts, next_retry_at

    now = time.time()
    if not force:
        if failed_write_attempts:
            if now < next_retry_at:
                return False          # storage failing: wait for the backoff
        elif len(BUFFER) < MAX_BUFFER_EVENTS and (now - last_flush) < FLUSH_EVERY_SECONDS:
            return False              # no limit hit yet

    last_flush = now

    if BUFFER:
        try:
            target = write_curated_unified(BUFFER)
            if target:
                print(f"[storage] wrote {len(BUFFER)} events -> {target}")
            else:
                print(f"[storage] STORAGE_MODE=none, skipped write, batch size={len(BUFFER)}")
        except Exception as e:
            # Keep the events and their offsets; retry later.
            failed_write_attempts += 1
            backoff = min(2 ** (failed_write_attempts - 1), RETRY_BACKOFF_MAX_SECONDS)
            next_retry_at = now + backoff
            print(f"[storage] ERROR writing batch of {len(BUFFER)} events "
                  f"(attempt {failed_write_attempts}/{MAX_WRITE_ATTEMPTS}, "
                  f"retry in {backoff:.0f}s, reading paused): {e}")
            if not force and failed_write_attempts >= MAX_WRITE_ATTEMPTS:
                raise StorageUnavailableError(
                    f"Storage write failed {failed_write_attempts} times in a row"
                ) from e
            return False

        BUFFER = []
        if failed_write_attempts:
            print("[storage] Write recovered; resuming reads.")
        failed_write_attempts = 0

    return commit_handled_offsets(consumer)


def _request_stop(signum, frame) -> None:
    global stop_requested
    print(f"[INFO] Received signal {signum}, shutting down after final flush...")
    stop_requested = True


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


def consume_loop(consumer) -> None:
    """
    Main poll loop: read messages, buffer valid events, flush on limits.
    Runs until stop_requested is set (SIGTERM / SIGINT / Ctrl+C).
    Always attempts a final flush + commit, and always closes the consumer.
    """
    try:
        while not stop_requested:
            # Backpressure: while storage is failing, pause every partition so
            # the buffer can't keep growing. We still call poll() so the
            # consumer stays in its group; paused partitions return nothing.
            if failed_write_attempts:
                consumer.pause(*consumer.assignment())
            elif consumer.paused():
                consumer.resume(*consumer.paused())

            records = consumer.poll(timeout_ms=POLL_TIMEOUT_MS)

            for tp, messages in records.items():
                for msg in messages:
                    normalized_event = normalize_event(msg.topic, msg.value)
                    if normalized_event is not None:
                        BUFFER.append(normalized_event)
                    # Only marked AFTER handling: if normalize_event raises,
                    # this message (and everything after it) stays uncommitted.
                    mark_handled(tp, msg)

            # Checked on every loop tick, even when no messages arrived,
            # so the time-based flush also fires while topics are idle.
            maybe_flush(consumer)

    except KeyboardInterrupt:
        print("\n[INFO] Stopping consumer (keyboard interrupt).")
    finally:
        try:
            # Final flush BEFORE closing, so the last batch is written and committed.
            if maybe_flush(consumer, force=True):
                print("[INFO] Final flush done.")
            else:
                print(f"[WARN] Final flush incomplete ({len(BUFFER)} events unwritten). "
                      "Uncommitted messages will be re-read on restart.")
        except Exception as e:
            print(f"[ERROR] Final flush/commit raised: {e}. "
                  "Uncommitted messages will be re-read on restart.")
        finally:
            consumer.close(autocommit=False)
            print("[INFO] Consumer closed.")


def main():
    print("[INFO] Starting unified stream consumer...")
    print(f"[INFO] Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"[INFO] Topics: {TOPICS}")
    print(f"[INFO] Consumer group: {CONSUMER_GROUP_ID}")
    print(f"[INFO] Batch: {MAX_BUFFER_EVENTS} events or {FLUSH_EVERY_SECONDS}s, whichever first")

    # docker stop sends SIGTERM; Ctrl+C sends SIGINT. Both -> clean shutdown.
    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)

    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP_ID,
        auto_offset_reset="earliest",  # start from beginning if no committed offsets
        enable_auto_commit=False,      # we commit manually, only after a successful write
        max_poll_records=MAX_BUFFER_EVENTS,  # one poll can't blow far past the batch size
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    try:
        consume_loop(consumer)
    except StorageUnavailableError as e:
        # Exit non-zero so Docker (restart: unless-stopped) restarts us;
        # uncommitted messages are re-delivered by Kafka.
        print(f"[FATAL] {e}. Exiting so the consumer can be restarted.")
        sys.exit(1)


if __name__ == "__main__":
    main()
