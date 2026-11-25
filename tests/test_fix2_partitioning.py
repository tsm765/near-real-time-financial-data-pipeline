"""
Regression tests for Fix 2: every event is written to the partition of its
OWN timestamp (UTC year/month/day/hour), partial write failures keep exactly
the unwritten events, and invalid timestamps are rejected explicitly.

Run from the project root (no Kafka or Azure needed):
    python -m unittest tests.test_fix2_partitioning -v
"""
import re
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from storage import azure_writer, local_writer  # noqa: E402
from datetime import datetime, timezone  # noqa: E402

from storage.parquet_utils import (  # noqa: E402
    PartialWriteError,
    WriteResult,
    group_events_by_partition,
    parse_timestamps,
    unified_events_to_parquet_bytes,
    write_partitioned,
)
from tests.test_fix1_no_overwrite import (  # noqa: E402  (helpers only, not TestCases)
    SYNAPSE_PATH_RE,
    FakeConsumer,
    _load_consumer_app,
)

FILENAME_RE = re.compile(
    r"year=(\d{4})/month=(\d{2})/day=(\d{2})/unified_(\d{4})-(\d{2})-(\d{2})_(\d{2})_[0-9a-f]{32}\.parquet$"
)


def _event(ts, price=1.0, ticker="AAPL"):
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


class _TmpDirTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = tmp.name
        p = mock.patch("builtins.print")
        self.print_mock = p.start()
        self.addCleanup(p.stop)

    def write_local(self, events) -> WriteResult:
        return local_writer.write_curated_unified_local(events, self.tmp)

    def files(self):
        return sorted(Path(self.tmp).rglob("*.parquet"))

    def rel(self, path) -> str:
        return Path(path).relative_to(self.tmp).as_posix()

    def assert_rows_belong_to_their_file(self, path):
        """Every row's UTC timestamp matches the folder date and filename hour."""
        m = FILENAME_RE.search(self.rel(path))
        self.assertIsNotNone(m, self.rel(path))
        year, month, day, fy, fm, fd, hour = map(int, m.groups())
        self.assertEqual((year, month, day), (fy, fm, fd))
        for ts in pd.read_parquet(path)["timestamp"]:
            self.assertEqual((ts.year, ts.month, ts.day, ts.hour), (year, month, day, hour), path)


# ---------------------------------------------------------------------------
# Partitioning: each event goes to its own hour
# ---------------------------------------------------------------------------
class PartitioningTests(_TmpDirTest):
    def test_batch_spanning_hours_writes_one_file_per_hour(self):
        events = [
            _event("2026-01-20T14:05:00+00:00", 1),
            _event("2026-01-20T16:10:00+00:00", 2),
            _event("2026-01-20T14:59:59+00:00", 3),
            _event("2026-01-19T08:00:00+00:00", 4),  # older article: previous day
        ]
        result = self.write_local(events)

        self.assertEqual(len(result.paths), 3)
        self.assertEqual(result.rejected, [])
        self.assertEqual(sorted(self.rel(p) for p in result.paths), [self.rel(p) for p in self.files()])
        for path in result.paths:
            self.assertRegex(self.rel(path), SYNAPSE_PATH_RE)
            self.assert_rows_belong_to_their_file(path)

        all_rows = pd.concat(pd.read_parquet(p) for p in result.paths)
        self.assertEqual(sorted(all_rows["price"]), [1, 2, 3, 4])

    def test_old_bug_later_events_not_filed_under_earliest_date(self):
        """Before: the whole batch went to the earliest event's folder (day=19)."""
        result = self.write_local([
            _event("2026-01-19T23:00:00+00:00", 1),
            _event("2026-01-20T10:00:00+00:00", 2),
        ])
        by_day = {FILENAME_RE.search(self.rel(p)).group(3): pd.read_parquet(p)["price"].tolist()
                  for p in result.paths}
        self.assertEqual(by_day, {"19": [1.0], "20": [2.0]})

    def test_midnight_month_and_year_boundaries(self):
        cases = {
            "2026-01-20T23:59:59.999999+00:00": "year=2026/month=01/day=20/unified_2026-01-20_23_",
            "2026-01-21T00:00:00+00:00":        "year=2026/month=01/day=21/unified_2026-01-21_00_",
            "2026-01-31T23:30:00+00:00":        "year=2026/month=01/day=31/unified_2026-01-31_23_",
            "2026-02-01T00:30:00+00:00":        "year=2026/month=02/day=01/unified_2026-02-01_00_",
            "2026-12-31T23:30:00+00:00":        "year=2026/month=12/day=31/unified_2026-12-31_23_",
            "2027-01-01T00:00:01+00:00":        "year=2027/month=01/day=01/unified_2027-01-01_00_",
        }
        result = self.write_local([_event(ts, i) for i, ts in enumerate(cases)])
        rels = sorted(self.rel(p) for p in result.paths)
        self.assertEqual(len(rels), len(cases))
        for expected_prefix in cases.values():
            self.assertEqual(sum(r.startswith("curated/unified_events/" + expected_prefix) for r in rels), 1,
                             expected_prefix)
        for path in result.paths:
            self.assert_rows_belong_to_their_file(path)

    def test_offsets_and_mixed_iso_formats_partition_by_utc(self):
        cases = [
            ("2026-01-21T00:30:00+02:00",      (2026, 1, 20, 22)),  # Helsinki -> previous UTC day
            ("2026-01-20T21:15:00.5-05:00",    (2026, 1, 21, 2)),   # New York -> next UTC day
            ("2026-01-20T14:05:00Z",           (2026, 1, 20, 14)),
            ("2026-01-20T14:06:00.123456Z",    (2026, 1, 20, 14)),
            ("2026-01-20T14:07:00.25+00:00",   (2026, 1, 20, 14)),
            ("2026-01-20T14:08:00",            (2026, 1, 20, 14)),  # naive -> treated as UTC
        ]
        groups, rejected = group_events_by_partition([_event(ts, i) for i, (ts, _) in enumerate(cases)])
        self.assertEqual(rejected, [])
        got = {key: [e["price"] for e in evs] for key, evs in groups.items()}
        self.assertEqual(got, {
            (2026, 1, 20, 14): [2, 3, 4, 5],
            (2026, 1, 20, 22): [0],
            (2026, 1, 21, 2): [1],
        })

        result = self.write_local([_event(ts, i) for i, (ts, _) in enumerate(cases)])
        for path in result.paths:
            self.assert_rows_belong_to_their_file(path)
        self.assertEqual(sum(len(pd.read_parquet(p)) for p in result.paths), len(cases))

    def test_single_hour_batch_still_one_file(self):
        result = self.write_local([_event("2026-01-20T14:0%d:00Z" % i, i) for i in range(5)])
        self.assertEqual(len(result.paths), 1)
        self.assertEqual(len(pd.read_parquet(result.paths[0])), 5)


# ---------------------------------------------------------------------------
# Invalid / missing timestamps: rejected explicitly, never mis-filed or lost silently
# ---------------------------------------------------------------------------
class InvalidTimestampTests(_TmpDirTest):
    BAD = [None, "", "not a date", "2026-13-45T00:00:00Z", 1768917900]

    def test_invalid_timestamps_rejected_and_reported_valid_ones_written(self):
        bad_events = [_event(ts, 100 + i) for i, ts in enumerate(self.BAD)]
        good = [_event("2026-01-20T14:05:00Z", 1), _event("2026-01-20T15:05:00Z", 2)]
        result = self.write_local([good[0], *bad_events, good[1]])

        self.assertEqual(result.rejected, bad_events)          # exactly the bad ones, in order
        written = pd.concat(pd.read_parquet(p) for p in result.paths)
        self.assertEqual(sorted(written["price"]), [1.0, 2.0])  # none of the bad ones written
        self.assertIn("5 event(s) rejected", str(result))

        warnings = [c.args[0] for c in self.print_mock.call_args_list if "rejecting event" in c.args[0]]
        self.assertEqual(len(warnings), len(self.BAD))          # one explicit warning per event

    def test_all_invalid_batch_writes_nothing_and_does_not_raise(self):
        """Previously this raised and would have been retried until the consumer gave up."""
        events = [_event(ts) for ts in self.BAD]
        result = self.write_local(events)
        self.assertEqual(result.paths, [])
        self.assertEqual(result.rejected, events)
        self.assertEqual(self.files(), [])

    def test_missing_timestamp_key_is_rejected(self):
        event = _event("2026-01-20T14:05:00Z")
        del event["timestamp"]
        groups, rejected = group_events_by_partition([event])
        self.assertEqual((groups, rejected), ({}, [event]))


# ---------------------------------------------------------------------------
# Partial failures: written files are kept, every unwritten event is returned
# ---------------------------------------------------------------------------
class PartialWriteTests(_TmpDirTest):
    def _three_hours(self):
        return [
            _event("2026-01-20T16:00:00Z", 3),
            _event("2026-01-20T14:00:00Z", 1),
            _event(None, 99),                     # rejected
            _event("2026-01-20T15:00:00Z", 2),
            _event("2026-01-20T16:30:00Z", 4),
        ]

    def test_partial_write_error_carries_every_unwritten_event(self):
        events = self._three_hours()
        calls = []

        def write_group(key, group):
            calls.append(key)
            if key[3] == 15:
                raise OSError("disk full")
            return f"file-{key[3]}"

        with self.assertRaises(PartialWriteError) as ctx:
            write_partitioned(events, write_group)
        err = ctx.exception

        self.assertEqual(err.written_paths, ["file-14"])
        # the failing hour (15) AND every hour after it (16) - same dict objects
        self.assertEqual([e["price"] for e in err.unwritten_events], [2, 3, 4])
        for e in err.unwritten_events:
            self.assertTrue(any(e is original for original in events))
        self.assertEqual([e["price"] for e in err.rejected], [99])
        self.assertNotIn(99, [e["price"] for e in err.unwritten_events])  # rejected never retried
        self.assertEqual(calls, [(2026, 1, 20, 14), (2026, 1, 20, 15)])  # stopped at the failure
        self.assertIsInstance(err.__cause__, OSError)

    def test_failure_on_first_partition_returns_whole_valid_batch(self):
        events = self._three_hours()

        def write_group(key, group):
            raise OSError("down")

        with self.assertRaises(PartialWriteError) as ctx:
            write_partitioned(events, write_group)
        self.assertEqual(ctx.exception.written_paths, [])
        self.assertEqual(sorted(e["price"] for e in ctx.exception.unwritten_events), [1, 2, 3, 4])

    def test_azure_partial_upload(self):
        events = self._three_hours()
        with mock.patch.object(azure_writer, "BlobServiceClient") as bsc:
            container = bsc.from_connection_string.return_value.get_container_client.return_value
            blob_client = container.get_blob_client.return_value
            blob_client.upload_blob.side_effect = [None, RuntimeError("Azure blip")]

            with self.assertRaises(PartialWriteError) as ctx:
                azure_writer.write_curated_unified_azure(events, "conn", "finstream")

        err = ctx.exception
        self.assertEqual(len(err.written_paths), 1)
        self.assertRegex(err.written_paths[0], SYNAPSE_PATH_RE)
        self.assertIn("unified_2026-01-20_14_", err.written_paths[0])
        self.assertEqual([e["price"] for e in err.unwritten_events], [2, 3, 4])
        self.assertEqual(bsc.from_connection_string.call_count, 1)  # one client per batch
        for call in blob_client.upload_blob.call_args_list:
            self.assertIs(call.kwargs["overwrite"], False)

    def test_azure_multi_partition_success(self):
        events = [e for e in self._three_hours() if e["timestamp"]]
        with mock.patch.object(azure_writer, "BlobServiceClient") as bsc:
            container = bsc.from_connection_string.return_value.get_container_client.return_value
            result = azure_writer.write_curated_unified_azure(events, "conn", "finstream")
            blob_paths = [c.args[0] for c in container.get_blob_client.call_args_list]

        self.assertEqual(result.paths, blob_paths)
        self.assertEqual([re.search(r"_(\d{2})_[0-9a-f]{32}", p).group(1) for p in blob_paths],
                         ["14", "15", "16"])
        for p in blob_paths:
            self.assertRegex(p, SYNAPSE_PATH_RE)


# ---------------------------------------------------------------------------
# Consumer: partial failures + rejections through the real poll loop
# ---------------------------------------------------------------------------
def _price_msg(ts, price, partition=0):
    return ("prices", {"ticker": "AAPL", "price": price, "timestamp": ts, "source": "test"}, partition)


class _ConsumerBase(_TmpDirTest):
    """Shared consumer setup; defines no tests itself."""

    @classmethod
    def setUpClass(cls):
        cls.app = _load_consumer_app()

    def setUp(self):
        super().setUp()
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
        app.RETRY_BACKOFF_MAX_SECONDS = 0
        self.log = []

    def run_loop(self, polls, write, **kwargs):
        consumer = FakeConsumer(self.app, polls, self.log, **kwargs)
        with mock.patch.object(self.app, "write_curated_unified", side_effect=write):
            self.app.consume_loop(consumer)
        return consumer

    def _write_with_failures(self, fail_hours):
        """
        Real local writer, but each hour in fail_hours fails once.
        Records which prices were persisted per write call.
        """
        persisted, failures_left = [], dict(fail_hours)

        def write(events):
            original = local_writer.write_partitioned

            def flaky_partitioned(evs, write_group):
                def wrapped(key, group):
                    if failures_left.get(key[3], 0) > 0:
                        failures_left[key[3]] -= 1
                        self.log.append(f"fail({key[3]})")
                        raise RuntimeError("Azure blip")
                    path = write_group(key, group)
                    persisted.extend(e["price"] for e in group)
                    self.log.append(f"write({key[3]})")
                    return path
                return original(evs, wrapped)

            with mock.patch.object(local_writer, "write_partitioned", side_effect=flaky_partitioned):
                return local_writer.write_curated_unified_local(events, self.tmp)

        return write, persisted


class ConsumerPartitionTests(_ConsumerBase):
    def test_partial_failure_retries_only_unwritten_then_commits(self):
        write, persisted = self._write_with_failures({15: 1})
        self.app.MAX_BUFFER_EVENTS = 3  # flush inside the loop, where retries happen
        polls = [[
            _price_msg("2026-01-20T14:00:00Z", 1, 0),
            _price_msg("2026-01-20T15:00:00Z", 2, 1),
            _price_msg("2026-01-20T16:00:00Z", 3, 0),
        ]]
        consumer = self.run_loop(polls, write)

        # 14 written, 15 fails -> retry writes 15 and 16 only (14 never rewritten)
        self.assertEqual(self.log, ["write(14)", "fail(15)", "write(15)", "write(16)",
                                    "commit", "close(autocommit=False)"])
        self.assertEqual(sorted(persisted), [1, 2, 3])          # each exactly once
        self.assertEqual(len(self.files()), 3)
        from kafka.structs import TopicPartition
        self.assertEqual(consumer.committed, {TopicPartition("prices", 0): 2,
                                              TopicPartition("prices", 1): 1})

    def test_no_commit_until_every_event_persisted(self):
        """Partial write, then storage stays down: nothing is committed, buffer = unwritten only."""
        write, persisted = self._write_with_failures({15: 999})
        self.app.MAX_WRITE_ATTEMPTS = 3
        self.app.MAX_BUFFER_EVENTS = 3
        polls = [[
            _price_msg("2026-01-20T14:00:00Z", 1),
            _price_msg("2026-01-20T15:00:00Z", 2),
            _price_msg("2026-01-20T16:00:00Z", 3),
        ]]
        consumer = FakeConsumer(self.app, polls, self.log)
        with mock.patch.object(self.app, "write_curated_unified", side_effect=write):
            with self.assertRaises(self.app.StorageUnavailableError):
                self.app.consume_loop(consumer)

        self.assertEqual(persisted, [1])                         # only hour 14 made it
        self.assertEqual(consumer.committed, {})                 # -> all re-read on restart
        self.assertEqual(sorted(e["price"] for e in self.app.BUFFER), [2, 3])
        self.assertTrue(consumer.closed)
        self.assertNotIn("commit", self.log)

    def test_rejected_timestamps_are_committed_not_retried(self):
        """
        An event with an unusable timestamp is intentionally rejected: its
        offset is committed (it won't be re-read forever) and it is never written.
        normalize_timestamp() is bypassed here to simulate one reaching storage.
        """
        write_calls = []

        def write(events):
            write_calls.append(len(events))
            return local_writer.write_curated_unified_local(events, self.tmp)

        polls = [[
            _price_msg("2026-01-20T14:00:00Z", 1),
            _price_msg("garbage", 2),
            _price_msg("2026-01-21T09:00:00Z", 3),
        ]]
        with mock.patch.object(self.app, "normalize_timestamp", side_effect=lambda ts: ts):
            consumer = self.run_loop(polls, write)

        self.assertEqual(write_calls, [3])                       # one write, no retries
        rows = pd.concat(pd.read_parquet(p) for p in self.files())
        self.assertEqual(sorted(rows["price"]), [1.0, 3.0])
        from kafka.structs import TopicPartition
        self.assertEqual(consumer.committed, {TopicPartition("prices", 0): 3})
        self.assertEqual(self.app.BUFFER, [])

    def test_end_to_end_multi_day_batch_lands_in_correct_folders(self):
        polls = [[_price_msg("2026-01-31T23:59:00Z", 1)],
                 [_price_msg("2026-02-01T00:01:00Z", 2)],
                 [_price_msg("2026-02-01T00:02:00+01:00", 3)]]  # = Jan 31 23:02 UTC

        def write(events):
            return local_writer.write_curated_unified_local(events, self.tmp)

        self.run_loop(polls, write)
        by_folder = {}
        for f in self.files():
            folder = "/".join(self.rel(f).split("/")[2:5])
            by_folder.setdefault(folder, []).extend(pd.read_parquet(f)["price"].tolist())
            self.assert_rows_belong_to_their_file(f)
        self.assertEqual({k: sorted(v) for k, v in by_folder.items()}, {
            "year=2026/month=01/day=31": [1.0, 3.0],
            "year=2026/month=02/day=01": [2.0],
        })


# ---------------------------------------------------------------------------
# Both timestamp columns validated; every event is persisted OR rejected
# ---------------------------------------------------------------------------
class IngestTimeAndInvariantTests(_TmpDirTest):
    def _persisted_prices(self):
        files = self.files()
        if not files:
            return []
        return pd.concat(pd.read_parquet(f) for f in files)["price"].tolist()

    def test_invalid_ingest_time_rejected_explicitly(self):
        good = _event("2026-01-20T14:05:00Z", 1)
        bad = []
        for i, ingest in enumerate([None, "", "garbage", "2026-02-30T00:00:00Z"]):
            e = _event("2026-01-20T14:05:00Z", 10 + i)
            e["ingest_time"] = ingest
            bad.append(e)
        missing = _event("2026-01-20T14:05:00Z", 20)
        del missing["ingest_time"]
        bad.append(missing)

        result = self.write_local([good, *bad])

        self.assertEqual(result.rejected, bad)
        self.assertEqual(self._persisted_prices(), [1.0])
        warnings = [c.args[0] for c in self.print_mock.call_args_list if "rejecting event" in c.args[0]]
        self.assertEqual(len(warnings), len(bad))
        for w in warnings:
            self.assertIn("invalid ingest_time", w)

    def test_every_input_event_is_persisted_or_rejected(self):
        """The invariant: nothing is silently dropped, nothing is both written and rejected."""
        events = [
            _event("2026-01-20T14:05:00Z", 1),
            _event("garbage", 2),                                    # bad timestamp
            dict(_event("2026-01-20T15:05:00Z", 3), ingest_time="x"),  # bad ingest_time
            dict(_event(None, 4), ingest_time=None),                 # both bad
            dict(_event("2026-01-20T16:05:00Z", 5), ticker=None),    # missing ticker
            dict(_event("2026-01-20T16:05:00Z", 6), ticker=""),      # empty ticker
            _event("2026-01-21T09:00:00+02:00", 7),
            _event("2026-01-20T15:59:59.5Z", 8),
        ]
        result = self.write_local(events)

        persisted = self._persisted_prices()
        rejected = [e["price"] for e in result.rejected]
        self.assertEqual(sorted(persisted), [1.0, 7.0, 8.0])
        self.assertEqual(sorted(rejected), [2.0, 3.0, 4.0, 5.0, 6.0])
        self.assertEqual(set(persisted) & set(rejected), set())               # disjoint
        self.assertEqual(sorted(persisted + rejected), [e["price"] for e in events])  # complete
        self.assertEqual(len(persisted) + len(result.rejected), len(events))

    def test_invariant_holds_on_partial_write(self):
        """Persisted + unwritten (to retry) + rejected = exactly the input."""
        events = [
            _event("2026-01-20T14:00:00Z", 1),
            _event("2026-01-20T15:00:00Z", 2),
            dict(_event("2026-01-20T15:00:00Z", 3), ingest_time="bad"),
            _event("2026-01-20T16:00:00Z", 4),
            _event(None, 5),
        ]
        persisted = []

        def write_group(key, group):
            if key[3] == 15:
                raise OSError("down")
            persisted.extend(e["price"] for e in group)
            return f"file-{key[3]}"

        with self.assertRaises(PartialWriteError) as ctx:
            write_partitioned(events, write_group)
        unwritten = [e["price"] for e in ctx.exception.unwritten_events]
        rejected = [e["price"] for e in ctx.exception.rejected]

        self.assertEqual((persisted, unwritten, sorted(rejected)), ([1], [2, 4], [3, 5]))
        self.assertEqual(sorted(persisted + unwritten + rejected), [1, 2, 3, 4, 5])

    def test_serializer_refuses_invalid_rows_instead_of_dropping(self):
        """Safety net: if an invalid row ever bypassed validation, fail loudly."""
        for bad in (dict(_event("2026-01-20T14:05:00Z"), ingest_time="garbage"),
                    _event("garbage"),
                    dict(_event("2026-01-20T14:05:00Z"), ticker=None)):
            with self.assertRaises(ValueError):
                unified_events_to_parquet_bytes([_event("2026-01-20T14:05:00Z"), bad])


# ---------------------------------------------------------------------------
# Consumer logging + the real normalize_timestamp()
# ---------------------------------------------------------------------------
class ConsumerLoggingAndNormalizeTests(_ConsumerBase):
    def _storage_logs(self):
        return [c.args[0] for c in self.print_mock.call_args_list
                if c.args and str(c.args[0]).startswith("[storage]")]

    def _write_local(self, events):
        return local_writer.write_curated_unified_local(events, self.tmp)

    def test_success_log_separates_persisted_and_rejected(self):
        polls = [[
            _price_msg("2026-01-20T14:00:00Z", 1),
            _price_msg("garbage", 2),
            _price_msg("2026-01-21T09:00:00Z", 3),
        ]]
        # bypass the normalizer here only to get an invalid timestamp to storage
        with mock.patch.object(self.app, "normalize_timestamp", side_effect=lambda ts: ts):
            self.run_loop(polls, self._write_local)

        summary = [l for l in self._storage_logs() if "persisted" in l]
        self.assertEqual(len(summary), 1)
        self.assertIn("persisted 2 events in 2 file(s)", summary[0])
        self.assertIn("REJECTED 1 events", summary[0])
        self.assertFalse(any("persisted 3" in l or "wrote 3" in l for l in self._storage_logs()))

    def test_success_log_all_persisted(self):
        self.run_loop([[_price_msg("2026-01-20T14:00:00Z", 1), _price_msg("2026-01-20T15:00:00Z", 2)]],
                      self._write_local)
        summary = [l for l in self._storage_logs() if "persisted" in l]
        self.assertEqual(len(summary), 1)
        self.assertIn("persisted 2 events in 2 file(s)", summary[0])
        self.assertNotIn("REJECTED", summary[0])

    def test_normalize_timestamp_directly(self):
        """The real function: invalid/missing source timestamps fall back to 'now' (UTC)."""
        before = datetime.now(timezone.utc)
        bad_inputs = [None, "", "garbage", "2026-02-30T00:00:00Z", 1768917900]
        outputs = [self.app.normalize_timestamp(v) for v in bad_inputs]
        after = datetime.now(timezone.utc)

        parsed = parse_timestamps(outputs)
        for value, out, ts in zip(bad_inputs, outputs, parsed):
            self.assertFalse(pd.isna(ts), f"{value!r} -> {out!r} not parseable")
            self.assertTrue(before <= ts.to_pydatetime() <= after, f"{value!r} -> {out!r}")

        # valid inputs are kept and converted to UTC
        self.assertEqual(self.app.normalize_timestamp("2026-01-21T00:30:00+02:00"),
                         "2026-01-20T22:30:00+00:00")
        self.assertEqual(self.app.normalize_timestamp("2026-01-20T14:05:00Z"),
                         "2026-01-20T14:05:00+00:00")
        self.assertEqual(self.app.normalize_timestamp("2026-01-20T14:05:00"),
                         "2026-01-20T14:05:00+00:00")

    def test_invalid_source_timestamps_through_real_normalizer(self):
        """
        End to end with the REAL normalize_timestamp() (no bypass).
        Current, documented behaviour: a missing/invalid source timestamp is
        replaced with the consumer's 'now', so the event is PERSISTED under
        the current hour, not rejected. Nothing is silently lost either way.
        """
        msgs = [
            _price_msg("2026-01-20T14:00:00Z", 1),
            _price_msg(None, 2),
            _price_msg("", 3),
            _price_msg("garbage", 4),
            _price_msg("2026-02-30T00:00:00Z", 5),
            _price_msg(1768917900, 6),
        ]
        no_ts = ("prices", {"ticker": "AAPL", "price": 7, "source": "test"}, 0)  # key missing
        before = datetime.now(timezone.utc)
        consumer = self.run_loop([[*msgs, no_ts]], self._write_local)
        after = datetime.now(timezone.utc)

        rows = pd.concat(pd.read_parquet(f) for f in self.files())
        self.assertEqual(sorted(rows["price"]), [1, 2, 3, 4, 5, 6, 7])  # all persisted, none rejected
        self.assertFalse(any("REJECTED" in l for l in self._storage_logs()))

        by_price = dict(zip(rows["price"], rows["timestamp"]))
        self.assertEqual(by_price[1], pd.Timestamp("2026-01-20T14:00:00Z"))
        for price in (2, 3, 4, 5, 6, 7):  # fell back to the consumer's clock
            self.assertTrue(before <= by_price[price].to_pydatetime() <= after, price)
        for f in self.files():
            self.assert_rows_belong_to_their_file(f)

        from kafka.structs import TopicPartition
        self.assertEqual(consumer.committed, {TopicPartition("prices", 0): 7})


if __name__ == "__main__":
    unittest.main()
