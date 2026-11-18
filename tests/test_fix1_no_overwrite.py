"""
Regression tests for Fix 1: batches must never overwrite each other,
and Kafka offsets must only be committed, per partition, for messages
that were handled and successfully written (at-least-once).

Run from the project root (no Kafka or Azure needed):
    python -m unittest tests.test_fix1_no_overwrite -v
"""
import importlib.util
import os
import re
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from storage import azure_writer, local_writer  # noqa: E402

# Same pattern the Synapse bronze view uses:
#   .../year=*/month=*/day=*/unified_*.parquet
SYNAPSE_PATH_RE = re.compile(
    r"curated/unified_events/year=\d{4}/month=\d{2}/day=\d{2}/unified_[^/]*\.parquet$"
)


def _event(ticker="AAPL", ts="2026-01-20T14:05:00+00:00", price=255.5):
    return {
        "event_type": "price",
        "ticker": ticker,
        "price": price,
        "mentions": None,
        "timestamp": ts,
        "source": "test",
        "ingest_time": "2026-01-20T14:05:01+00:00",
        "topic": "prices",
    }


def _load_consumer_app():
    """Import consumer/stream_consumer/app.py as a module (it isn't a package)."""
    path = PROJECT_ROOT / "consumer" / "stream_consumer" / "app.py"
    spec = importlib.util.spec_from_file_location("stream_consumer_app", path)
    module = importlib.util.module_from_spec(spec)
    with mock.patch("builtins.print"):  # silence the module's startup debug prints
        spec.loader.exec_module(module)
    return module


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------
class LocalWriterTests(unittest.TestCase):
    def test_two_batches_same_hour_both_survive(self):
        """The original bug: the 2nd batch in an hour overwrote the 1st."""
        with tempfile.TemporaryDirectory() as tmp:
            p1 = local_writer.write_curated_unified_local([_event(price=1.0)], tmp)
            p2 = local_writer.write_curated_unified_local([_event(price=2.0)], tmp)

            self.assertNotEqual(p1, p2)
            files = list(Path(tmp).rglob("*.parquet"))
            self.assertEqual(len(files), 2)

            all_rows = pd.concat(pd.read_parquet(f) for f in files)
            self.assertEqual(sorted(all_rows["price"]), [1.0, 2.0])

    def test_path_still_matches_synapse_wildcard(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = local_writer.write_curated_unified_local([_event()], tmp)
            rel = Path(path).relative_to(tmp).as_posix()
            self.assertRegex(rel, SYNAPSE_PATH_RE)
            self.assertIn("year=2026/month=01/day=20/unified_2026-01-20_14_", rel)

    def test_file_contents_unchanged(self):
        """Same columns/order as before the fix, so the SQL views keep working."""
        with tempfile.TemporaryDirectory() as tmp:
            path = local_writer.write_curated_unified_local([_event()], tmp)
            df = pd.read_parquet(path)
            self.assertEqual(
                list(df.columns),
                ["event_type", "ticker", "price", "mentions",
                 "timestamp", "source", "ingest_time", "topic"],
            )


class AzureWriterTests(unittest.TestCase):
    def test_unique_blob_names_and_no_overwrite(self):
        with mock.patch.object(azure_writer, "BlobServiceClient") as bsc:
            container = bsc.from_connection_string.return_value.get_container_client.return_value
            blob_client = container.get_blob_client.return_value

            path1 = azure_writer.write_curated_unified_azure([_event()], "conn", "finstream")
            path2 = azure_writer.write_curated_unified_azure([_event()], "conn", "finstream")

            self.assertNotEqual(path1, path2)
            self.assertRegex(path1, SYNAPSE_PATH_RE)
            self.assertRegex(path2, SYNAPSE_PATH_RE)
            for call in blob_client.upload_blob.call_args_list:
                self.assertIs(call.kwargs["overwrite"], False)


# ---------------------------------------------------------------------------
# Consumer: batching, polling, offsets, backpressure, shutdown
# ---------------------------------------------------------------------------
from kafka.structs import TopicPartition  # noqa: E402


class FakeConsumer:
    """
    Stands in for KafkaConsumer. Each scripted poll is a list of
    (topic, value) or (topic, value, partition); offsets auto-increment per
    partition like a real broker. While any partition is paused, poll()
    returns nothing and keeps the scripted data queued. When the script runs
    out, it asks the app to stop. Records write/commit/close order in `log`.
    """

    def __init__(self, app, polls, log, commit_error=None):
        self.app = app
        self.polls = list(polls)
        self.log = log
        self.commit_error = commit_error  # exception (or list of them) to raise on commit
        self.committed = {}               # TopicPartition -> committed offset
        self.commit_calls = []
        self.next_offset = {}
        self.assigned = set()
        self.paused_set = set()
        self.pause_calls = 0
        self.resume_calls = 0
        self.closed = False

    # --- kafka-python API used by the app ---
    def poll(self, timeout_ms):
        if self.paused_set:
            return {}
        if not self.polls:
            self.app.stop_requested = True
            return {}
        batch = self.polls.pop(0)
        result = {}
        for item in batch:
            topic, value = item[0], item[1]
            partition = item[2] if len(item) > 2 else 0
            tp = TopicPartition(topic, partition)
            self.assigned.add(tp)
            offset = self.next_offset.get(tp, 0)
            self.next_offset[tp] = offset + 1
            result.setdefault(tp, []).append(
                SimpleNamespace(topic=topic, partition=partition, offset=offset, value=value)
            )
        return result

    def assignment(self):
        return set(self.assigned)

    def pause(self, *tps):
        self.pause_calls += 1
        self.paused_set.update(tps)

    def resume(self, *tps):
        self.resume_calls += 1
        self.paused_set.difference_update(tps)

    def paused(self):
        return set(self.paused_set)

    def commit(self, offsets=None):
        assert offsets is not None, "app must commit explicit offsets, never a blanket commit()"
        err = self.commit_error
        if isinstance(err, list):
            err = err.pop(0) if err else None
        if err is not None:
            self.log.append("commit-FAILED")
            raise err
        self.commit_calls.append({tp: om.offset for tp, om in offsets.items()})
        for tp, om in offsets.items():
            self.committed[tp] = om.offset
        self.log.append("commit")

    def close(self, autocommit=True):
        self.closed = True
        self.log.append(f"close(autocommit={autocommit})")


def _price_msg(price, partition=0):
    return ("prices", {"ticker": "AAPL", "price": price,
                       "timestamp": "2026-01-20T14:05:00Z", "source": "test"}, partition)


def _mention_msg(partition=0):
    return ("mentions", {"ticker": "TSLA", "mentions": 1,
                         "timestamp": "2026-01-20T14:05:00Z", "source": "newsapi"}, partition)


P0 = TopicPartition("prices", 0)
P1 = TopicPartition("prices", 1)
M0 = TopicPartition("mentions", 0)


class ConsumerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = _load_consumer_app()

    def setUp(self):
        app = self.app
        app.BUFFER.clear()
        app.uncommitted_offsets.clear()
        app.failed_write_attempts = 0
        app.next_retry_at = 0.0
        app.stop_requested = False
        app.last_flush = app.time.time()
        app.MAX_BUFFER_EVENTS = 500
        app.FLUSH_EVERY_SECONDS = 60
        app.MAX_WRITE_ATTEMPTS = 10
        app.RETRY_BACKOFF_MAX_SECONDS = 0  # retry immediately in tests
        self.log = []
        self.written = []

        def fake_write(events):
            self.written.append([e["price"] for e in events])
            self.log.append(f"write({len(events)})")
            return "fake/path.parquet"

        self.write_patch = mock.patch.object(app, "write_curated_unified", side_effect=fake_write)
        self.write_mock = self.write_patch.start()
        self.print_patch = mock.patch("builtins.print")
        self.print_patch.start()

    def tearDown(self):
        self.write_patch.stop()
        self.print_patch.stop()

    def run_loop(self, polls, **kwargs):
        consumer = FakeConsumer(self.app, polls, self.log, **kwargs)
        self.app.consume_loop(consumer)
        return consumer

    # --- batching (original fix 1 behaviour) ---
    def test_no_longer_one_file_per_event(self):
        """Old behaviour: 3 events -> 3 writes. Now: one batch."""
        self.run_loop([[_price_msg(1)], [_price_msg(2)], [_price_msg(3)]])
        self.assertEqual(self.written, [[1, 2, 3]])

    def test_size_limit_triggers_write(self):
        self.app.MAX_BUFFER_EVENTS = 2
        self.run_loop([[_price_msg(1)], [_price_msg(2)], [_price_msg(3)]])
        self.assertEqual(self.written, [[1, 2], [3]])  # size limit, then shutdown

    def test_time_limit_fires_while_idle(self):
        """A lone event must be written after the interval, even with no new messages."""
        fake_now = [1000.0]
        self.app.last_flush = fake_now[0]
        consumer = FakeConsumer(self.app, [[_price_msg(1)], [], [], []], self.log)
        original_poll = consumer.poll

        def poll(timeout_ms):
            fake_now[0] += 30  # each idle tick = 30s
            return original_poll(timeout_ms)

        consumer.poll = poll
        original_flush = self.app.maybe_flush

        def flush_then_stop(c, force=False):
            done = original_flush(c, force)
            if self.written:  # stop right after the time-based write
                self.app.stop_requested = True
            return done

        with mock.patch.object(self.app.time, "time", side_effect=lambda: fake_now[0]), \
             mock.patch.object(self.app, "maybe_flush", side_effect=flush_then_stop):
            self.app.consume_loop(consumer)

        self.assertEqual(self.written, [[1]])
        self.assertEqual(consumer.committed, {P0: 1})

    def test_commit_only_after_successful_write(self):
        consumer = self.run_loop([[_price_msg(1)]])
        self.assertEqual(self.log, ["write(1)", "commit", "close(autocommit=False)"])
        self.assertEqual(consumer.committed, {P0: 1})

    def test_sigterm_flushes_and_commits_before_close(self):
        """docker stop -> SIGTERM -> final write + commit, then close."""
        consumer = FakeConsumer(self.app, [[_price_msg(1)], [_price_msg(2)]], self.log)
        original_poll = consumer.poll

        def poll_then_signal(timeout_ms):
            result = original_poll(timeout_ms)
            if not consumer.polls:  # after the last message, simulate docker stop
                self.app._request_stop(15, None)
            return result

        consumer.poll = poll_then_signal
        self.app.consume_loop(consumer)
        self.assertEqual(self.log, ["write(2)", "commit", "close(autocommit=False)"])

    def test_invalid_only_messages_still_committed(self):
        """Dropped (invalid) messages shouldn't be re-read forever."""
        bad = ("prices", {"ticker": None, "price": 1})
        consumer = self.run_loop([[bad]])
        self.assertEqual(self.written, [])
        self.assertEqual(consumer.committed, {P0: 1})

    # --- explicit per-partition offsets ---
    def test_multiple_partitions_commit_exact_offsets(self):
        consumer = self.run_loop([
            [_price_msg(1, partition=0), _price_msg(2, partition=1), _mention_msg(0)],
            [_price_msg(3, partition=0), _price_msg(4, partition=0)],
        ])
        # next offset to read = last handled offset + 1, per partition
        self.assertEqual(consumer.committed, {P0: 3, P1: 1, M0: 1})
        self.assertEqual(len(consumer.commit_calls), 1)

    def test_unexpected_exception_mid_poll_commits_only_handled(self):
        """
        Poll returns p0:[ok, ok, BOOM, ok] and p1:[ok]. The crash must:
        - still write the 2 events handled before it,
        - commit p0 only up to the bad message (offset 2), so it's re-read,
        - not commit p1 at all (never handled),
        - close the consumer, and re-raise.
        A blanket consumer.commit() would have skipped BOOM, the ok after it and p1.
        """
        original = self.app.normalize_event

        def normalize(topic, payload):
            if payload.get("boom"):
                raise ValueError("unexpected payload shape")
            return original(topic, payload)

        boom = ("prices", {"boom": True, "ticker": "AAPL"}, 0)
        polls = [[_price_msg(1, 0), _price_msg(2, 0), boom, _price_msg(3, 0), _price_msg(9, 1)]]
        consumer = FakeConsumer(self.app, polls, self.log)

        with mock.patch.object(self.app, "normalize_event", side_effect=normalize):
            with self.assertRaises(ValueError):
                self.app.consume_loop(consumer)

        self.assertEqual(self.written, [[1, 2]])
        self.assertEqual(consumer.committed, {P0: 2})
        self.assertNotIn(P1, consumer.committed)
        self.assertTrue(consumer.closed)

    # --- storage failures: bounded retries + backpressure ---
    def test_failed_write_keeps_events_and_does_not_commit(self):
        self.app.MAX_BUFFER_EVENTS = 1
        calls = {"n": 0}

        def flaky_write(events):
            calls["n"] += 1
            if calls["n"] == 1:
                self.log.append("write-FAILED")
                raise RuntimeError("Azure is down")
            self.written.append([e["price"] for e in events])
            self.log.append(f"write({len(events)})")
            return "fake/path.parquet"

        self.write_mock.side_effect = flaky_write
        consumer = self.run_loop([[_price_msg(1)], [_price_msg(2)]])

        # fail -> paused (msg 2 not read) -> retry succeeds -> commit -> resume -> msg 2
        self.assertEqual(self.log, ["write-FAILED", "write(1)", "commit",
                                    "write(1)", "commit", "close(autocommit=False)"])
        self.assertEqual(self.written, [[1], [2]])
        self.assertEqual(consumer.committed, {P0: 2})
        self.assertGreaterEqual(consumer.pause_calls, 1)
        self.assertGreaterEqual(consumer.resume_calls, 1)

    def test_prolonged_outage_is_bounded_and_exits_without_commit(self):
        """Storage never recovers: buffer stays bounded, then the consumer gives up."""
        self.app.MAX_BUFFER_EVENTS = 2
        self.app.MAX_WRITE_ATTEMPTS = 3
        sizes = []

        def always_fail(events):
            sizes.append(len(events))
            raise RuntimeError("Azure is down")

        self.write_mock.side_effect = always_fail
        polls = [[_price_msg(i)] for i in range(50)]  # far more than the buffer allows
        consumer = FakeConsumer(self.app, polls, self.log)

        with self.assertRaises(self.app.StorageUnavailableError):
            self.app.consume_loop(consumer)

        self.assertEqual(max(sizes), 2)              # never grew past the batch size
        self.assertEqual(len(self.app.BUFFER), 2)
        self.assertEqual(len(consumer.polls), 48)    # the rest stayed in Kafka
        self.assertEqual(sizes, [2, 2, 2, 2])        # 3 attempts + 1 final flush
        self.assertEqual(consumer.committed, {})     # nothing committed -> re-read on restart
        self.assertTrue(consumer.closed)

    # --- commit failures ---
    def test_commit_failure_midrun_is_retried_with_later_offsets(self):
        self.app.MAX_BUFFER_EVENTS = 1
        err = [RuntimeError("broker hiccup"), None, None]
        consumer = self.run_loop([[_price_msg(1)], [_price_msg(2)]], commit_error=err)

        self.assertEqual(self.written, [[1], [2]])  # events not written twice
        self.assertEqual(consumer.committed, {P0: 2})
        self.assertIn("commit-FAILED", self.log)

    def test_final_commit_failure_still_closes_and_keeps_offsets(self):
        consumer = self.run_loop([[_price_msg(1)]], commit_error=RuntimeError("broker gone"))
        self.assertEqual(self.log, ["write(1)", "commit-FAILED", "close(autocommit=False)"])
        self.assertTrue(consumer.closed)
        self.assertEqual(consumer.committed, {})
        self.assertEqual(self.app.uncommitted_offsets, {P0: 1})

    def test_consumer_closed_even_if_final_commit_raises_unexpectedly(self):
        """Belt and braces: close() runs even if the commit path itself blows up."""
        with mock.patch.object(self.app, "commit_handled_offsets",
                               side_effect=RuntimeError("unexpected")):
            consumer = self.run_loop([[_price_msg(1)]])
        self.assertTrue(consumer.closed)

    # --- config ---
    def test_settings_come_from_env(self):
        with mock.patch.dict(os.environ, {"MAX_BUFFER_EVENTS": "42",
                                          "FLUSH_EVERY_SECONDS": "7.5",
                                          "MAX_WRITE_ATTEMPTS": "4",
                                          "RETRY_BACKOFF_MAX_SECONDS": "9"}):
            fresh = _load_consumer_app()
        self.assertEqual(fresh.MAX_BUFFER_EVENTS, 42)
        self.assertEqual(fresh.FLUSH_EVERY_SECONDS, 7.5)
        self.assertEqual(fresh.MAX_WRITE_ATTEMPTS, 4)
        self.assertEqual(fresh.RETRY_BACKOFF_MAX_SECONDS, 9.0)

    def test_defaults(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            for k in ("MAX_BUFFER_EVENTS", "FLUSH_EVERY_SECONDS",
                      "MAX_WRITE_ATTEMPTS", "RETRY_BACKOFF_MAX_SECONDS"):
                os.environ.pop(k, None)
            fresh = _load_consumer_app()
        self.assertEqual(fresh.MAX_BUFFER_EVENTS, 500)
        self.assertEqual(fresh.FLUSH_EVERY_SECONDS, 60.0)
        self.assertEqual(fresh.MAX_WRITE_ATTEMPTS, 10)
        self.assertEqual(fresh.RETRY_BACKOFF_MAX_SECONDS, 60.0)


if __name__ == "__main__":
    unittest.main()
