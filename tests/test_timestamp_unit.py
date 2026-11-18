"""
Regression tests: Parquet timestamps must stay epoch-NANOSECONDS (UTC),
whatever pandas version is installed, because the Synapse silver view
(sql/vw_unified_events_clean.sql) converts them by dividing by 1e9.

Run from the project root (no Kafka or Azure needed):
    python -m unittest tests.test_timestamp_unit -v
"""
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from storage.parquet_utils import unified_events_to_parquet_bytes  # noqa: E402

# A file the pipeline wrote in January 2026, before any of these changes.
REFERENCE_FILE = (
    PROJECT_ROOT / "data" / "curated" / "unified_events"
    / "year=2026" / "month=01" / "day=18" / "unified_2026-01-18_00.parquet"
)

EVENT = {
    "event_type": "price",
    "ticker": "AAPL",
    "price": 255.53,
    "mentions": None,
    "timestamp": "2026-01-20T14:05:00.123456Z",
    "source": "alpha_vantage",
    "ingest_time": "2026-01-20T14:05:01+00:00",
    "topic": "prices",
}


def _synapse_silver_conversion(raw: int) -> datetime:
    """Python copy of the vw_unified_events_clean timestamp math."""
    seconds = int(raw / 1_000_000_000)          # CAST([timestamp] / 1000000000 AS INT)
    millis = int((raw / 1_000_000) % 1000)      # CAST(([timestamp] / 1000000) % 1000 AS INT)
    return datetime(1970, 1, 1) + timedelta(seconds=seconds, milliseconds=millis)


def _epoch_ns(dt: datetime) -> int:
    """Exact epoch-nanoseconds for an aware datetime (no float rounding)."""
    delta = dt - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (delta.days * 86_400 + delta.seconds) * 1_000_000_000 + delta.microseconds * 1_000


class TimestampUnitTests(unittest.TestCase):
    def setUp(self):
        # Every Parquet file a test writes goes in here; deleted after each test.
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp_dir = Path(tmp.name)
        self._n_files = 0

    def _write_file(self, events) -> str:
        """Write events exactly like the pipeline does, to a temp .parquet file."""
        self._n_files += 1
        path = self.tmp_dir / f"batch_{self._n_files}.parquet"
        path.write_bytes(unified_events_to_parquet_bytes(events))
        return str(path)

    def _write(self, events):
        # Read back from a file path (not an in-memory buffer): pyarrow can abort
        # at interpreter exit after in-memory reads on some machines. The pipeline
        # itself never reads Parquet, so this only matters for the tests.
        return pq.read_table(self._write_file(events))

    def _raw(self, table, column="timestamp") -> list[int]:
        return table.column(column).cast(pa.int64()).to_pylist()

    def test_timestamps_written_as_nanoseconds_utc(self):
        table = self._write([EVENT])
        expected = pa.timestamp("ns", tz="UTC")
        self.assertEqual(table.schema.field("timestamp").type, expected)
        self.assertEqual(table.schema.field("ingest_time").type, expected)

    def test_parquet_schema_matches_files_already_in_the_lake(self):
        """
        New files must have the same Parquet schema (what Synapse reads:
        physical type + logical type per column) as the January files.
        """
        new_schema = pq.ParquetFile(self._write_file([EVENT])).schema
        old_schema = pq.ParquetFile(REFERENCE_FILE).schema
        self.assertTrue(new_schema.equals(old_schema),
                        f"\nNEW:\n{new_schema}\nOLD (January):\n{old_schema}")

    def test_raw_value_is_epoch_nanoseconds(self):
        raw = self._raw(self._write([EVENT]))[0]
        self.assertEqual(raw, 1_768_917_900_123_456_000)

    def test_synapse_view_math_gives_the_right_time(self):
        """What vw_unified_events_clean would produce from the value we write."""
        raw = self._raw(self._write([EVENT]))[0]
        self.assertEqual(_synapse_silver_conversion(raw), datetime(2026, 1, 20, 14, 5, 0, 123000))

    def test_non_utc_input_is_converted_to_utc(self):
        event = dict(EVENT, timestamp="2026-01-20T16:05:00+02:00")  # Helsinki time
        raw = self._raw(self._write([event]))[0]
        self.assertEqual(_synapse_silver_conversion(raw), datetime(2026, 1, 20, 14, 5, 0))

    def test_mixed_iso_formats_in_one_batch_all_survive(self):
        """
        One batch mixing ISO shapes: with/without fractional seconds (of
        different lengths) and Z / +00:00 / +02:00 / -05:00 offsets, in BOTH
        timestamp columns. Without format="ISO8601", pandas guesses one format
        from the first value and the others silently become NaT -> rows dropped.
        """
        utc = timezone.utc
        cases = [
            # (timestamp in,                     ingest_time in,                     timestamp as UTC)
            ("2026-01-20T14:05:00Z",             "2026-01-20T14:05:01.000001+00:00", datetime(2026, 1, 20, 14, 5, 0, tzinfo=utc)),
            ("2026-01-20T14:05:00.123456Z",      "2026-01-20T14:05:01Z",             datetime(2026, 1, 20, 14, 5, 0, 123456, tzinfo=utc)),
            ("2026-01-20T16:05:00+02:00",        "2026-01-20T16:05:01.5+02:00",      datetime(2026, 1, 20, 14, 5, 0, tzinfo=utc)),
            ("2026-01-20T14:05:00.5+00:00",      "2026-01-20T09:05:01-05:00",        datetime(2026, 1, 20, 14, 5, 0, 500000, tzinfo=utc)),
            ("2026-01-20T09:05:00.25-05:00",     "2026-01-20T14:05:01.123Z",         datetime(2026, 1, 20, 14, 5, 0, 250000, tzinfo=utc)),
            ("2026-01-20T14:05:00.654321+00:00", "2026-01-20T14:05:01+00:00",        datetime(2026, 1, 20, 14, 5, 0, 654321, tzinfo=utc)),
        ]
        expected_ingest = [
            datetime(2026, 1, 20, 14, 5, 1, 1, tzinfo=utc),
            datetime(2026, 1, 20, 14, 5, 1, tzinfo=utc),
            datetime(2026, 1, 20, 14, 5, 1, 500000, tzinfo=utc),
            datetime(2026, 1, 20, 14, 5, 1, tzinfo=utc),
            datetime(2026, 1, 20, 14, 5, 1, 123000, tzinfo=utc),
            datetime(2026, 1, 20, 14, 5, 1, tzinfo=utc),
        ]
        events = [
            dict(EVENT, price=float(i), timestamp=ts, ingest_time=ingest)
            for i, (ts, ingest, _) in enumerate(cases)
        ]

        table = self._write(events)

        self.assertEqual(table.num_rows, len(cases), "rows were dropped (NaT timestamps)")
        self.assertEqual(table.column("price").to_pylist(), [float(i) for i in range(len(cases))])
        self.assertEqual(self._raw(table, "timestamp"), [_epoch_ns(c[2]) for c in cases])
        self.assertEqual(self._raw(table, "ingest_time"), [_epoch_ns(d) for d in expected_ingest])


class RequirementsPinnedTests(unittest.TestCase):
    def test_every_dependency_is_pinned(self):
        """Unpinned deps are how pandas 3 would have slipped into a rebuild."""
        lines = [
            line.strip()
            for line in (PROJECT_ROOT / "requirements.txt").read_text().splitlines()
            if line.strip() and not line.strip().startswith("#")
        ]
        self.assertTrue(lines)
        for line in lines:
            self.assertRegex(line, r"^[A-Za-z0-9_.\-\[\]]+==[0-9][^\s]*$", f"not pinned: {line}")


if __name__ == "__main__":
    unittest.main()
