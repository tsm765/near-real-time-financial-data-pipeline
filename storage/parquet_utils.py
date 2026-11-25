from __future__ import annotations

import io
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Tuple

import pandas as pd

# The canonical column order for our unified events table
UNIFIED_COLUMNS = [
    "event_type",   # str: "price" or "mention"
    "ticker",       # str: e.g. "AAPL"
    "price",        # float (nullable)
    "mentions",     # int (nullable)
    "timestamp",    # datetime UTC (event time)
    "source",       # str: "alpha_vantage", "newsapi", etc.
    "ingest_time",  # datetime UTC (when consumer processed it)
    "topic",        # str: "prices" or "mentions"
]

# Timestamp type written to Parquet: nanoseconds, UTC (see step 3 below).
TIMESTAMP_DTYPE = "datetime64[ns, UTC]"

# Columns every persisted row must have. Events missing any of them are
# rejected explicitly (group_events_by_partition), never dropped silently.
REQUIRED_COLUMNS = ["timestamp", "ingest_time", "ticker"]


# (year, month, day, hour) of an event's own UTC timestamp
PartitionKey = Tuple[int, int, int, int]


def parse_timestamps(values) -> pd.Series:
    """
    The ONE timestamp parser used for both partitioning and Parquet writing,
    so the two can never disagree about an event's time.

    - utc=True: offsets are converted to UTC; naive strings are taken as UTC.
    - format="ISO8601": each value is parsed on its own, so mixed ISO shapes
      (with/without fractional seconds, Z/+HH:MM) all parse correctly.
    - errors="coerce": missing or unparseable values become NaT. Callers must
      handle NaT explicitly (see group_events_by_partition).
    """
    return pd.to_datetime(
        pd.Series(values, dtype=object), utc=True, errors="coerce", format="ISO8601"
    )


def _ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make sure all expected columns exist, even if some are missing in the input.
    Missing columns are filled with None, then we reorder columns.
    """
    for col in UNIFIED_COLUMNS:
        if col not in df.columns:
            df[col] = None
    # Reorder columns to our canonical order
    return df[UNIFIED_COLUMNS]


def unified_events_to_parquet_bytes(events: List[Dict[str, Any]]) -> bytes:
    """
    Take a list of unified event dicts and return Parquet file contents as bytes.
    This is used both for local writing and Azure uploads.
    """
    if not events:
        raise ValueError("No events provided to unified_events_to_parquet_bytes")

    # 1) Build DataFrame
    df = pd.DataFrame(events)

    # 2) Ensure all expected columns exist and are ordered
    df = _ensure_columns(df)

    # 3) Enforce datetime types (UTC) for timestamp and ingest_time.
    #    The unit is pinned to NANOSECONDS on purpose: the Synapse silver view
    #    (vw_unified_events_clean) converts these as epoch-nanoseconds
    #    (divides by 1e9). pandas 3 defaults to microseconds, which would break
    #    that conversion, so we never rely on the pandas default here.
    #    format="ISO8601" parses each value on its own. Without it, pandas
    #    guesses one format from the first value, and differently shaped ISO
    #    strings (e.g. with/without fractional seconds) silently become NaT
    #    and are then dropped in step 6.
    for col in ("timestamp", "ingest_time"):
        df[col] = parse_timestamps(df[col]).astype(TIMESTAMP_DTYPE)

    # 4) Enforce string columns
    for col in ["event_type", "ticker", "source", "topic"]:
        df[col] = df[col].astype("string")

    # 5) Enforce numeric types (nullable)
    # price -> float, mentions -> nullable integer
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["mentions"] = pd.to_numeric(df["mentions"], errors="coerce").astype("Int64")

    # 6) Never drop rows silently. Events with a missing/invalid timestamp,
    #    ingest_time or ticker must already have been rejected explicitly by
    #    group_events_by_partition(); if any reach this point, fail loudly.
    invalid = df[REQUIRED_COLUMNS].isna().any(axis=1)
    if invalid.any():
        raise ValueError(
            f"{int(invalid.sum())} of {len(df)} rows have a missing/invalid "
            f"{'/'.join(REQUIRED_COLUMNS)}; they must be rejected before serialization"
        )

    # 7) Write DataFrame to Parquet in memory
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    return buffer.getvalue()


def group_events_by_partition(
    events: List[Dict[str, Any]],
) -> Tuple[Dict[PartitionKey, List[Dict[str, Any]]], List[Dict[str, Any]]]:
    """
    Split a batch by each event's OWN timestamp (UTC year/month/day/hour),
    so every event lands in its correct partition.

    Every input event ends up in exactly one of:
      - groups: {(year, month, day, hour): [events...]}, sorted by time
      - rejected: events that can't be persisted correctly: missing or
        unparseable `timestamp` or `ingest_time` (both parsed with the same
        parser used for writing), or a missing ticker. They are NOT written
        anywhere; the caller reports them explicitly.
    """
    groups: Dict[PartitionKey, List[Dict[str, Any]]] = {}
    rejected: List[Dict[str, Any]] = []

    timestamps = parse_timestamps([e.get("timestamp") for e in events])
    ingest_times = parse_timestamps([e.get("ingest_time") for e in events])
    for event, ts, ingest in zip(events, timestamps, ingest_times):
        if rejection_reason(event, ts, ingest):
            rejected.append(event)
            continue
        key = (int(ts.year), int(ts.month), int(ts.day), int(ts.hour))
        groups.setdefault(key, []).append(event)

    return dict(sorted(groups.items())), rejected


def rejection_reason(event: Dict[str, Any], ts=None, ingest=None) -> str:
    """
    Why an event can't be persisted ("" if it can). ts/ingest are the parsed
    values; when omitted they are parsed here with the shared parser.
    """
    if ts is None:
        ts = parse_timestamps([event.get("timestamp")]).iloc[0]
    if ingest is None:
        ingest = parse_timestamps([event.get("ingest_time")]).iloc[0]
    reasons = []
    if pd.isna(ts):
        reasons.append(f"invalid timestamp {event.get('timestamp')!r}")
    if pd.isna(ingest):
        reasons.append(f"invalid ingest_time {event.get('ingest_time')!r}")
    ticker = event.get("ticker")
    if ticker is None or (isinstance(ticker, float) and pd.isna(ticker)) or ticker == "":
        reasons.append("missing ticker")
    return ", ".join(reasons)


@dataclass
class WriteResult:
    """Outcome of writing one batch: one path per partition, plus rejected events."""
    paths: List[str] = field(default_factory=list)
    rejected: List[Dict[str, Any]] = field(default_factory=list)

    def __str__(self) -> str:
        text = f"{len(self.paths)} file(s): " + ", ".join(self.paths)
        if self.rejected:
            text += f" | {len(self.rejected)} event(s) rejected"
        return text


class PartialWriteError(RuntimeError):
    """
    Some partitions of a batch were written, then a write failed.

    - written_paths: files that were successfully written (do NOT rewrite them)
    - unwritten_events: EVERY event not yet persisted (the failing partition
      and all partitions after it). Rejected events are not included: they
      were handled on purpose and must not be retried.
    """

    def __init__(self, written_paths, unwritten_events, rejected, cause):
        self.written_paths = list(written_paths)
        self.unwritten_events = list(unwritten_events)
        self.rejected = list(rejected)
        super().__init__(
            f"{len(self.written_paths)} partition file(s) written, "
            f"{len(self.unwritten_events)} event(s) unwritten: {cause}"
        )


def write_partitioned(
    events: List[Dict[str, Any]],
    write_group: Callable[[PartitionKey, List[Dict[str, Any]]], str],
) -> WriteResult:
    """
    Write a batch as one file per partition, using write_group(key, events)
    for each partition (local disk or Azure). Shared by both writers.

    Raises PartialWriteError if any partition fails, carrying every event
    that was not written so the caller can retry exactly those.
    """
    groups, rejected = group_events_by_partition(events)

    for event in rejected:
        print(
            f"[storage] WARN rejecting event (not written): {rejection_reason(event)} | "
            f"ticker={event.get('ticker')!r} topic={event.get('topic')!r} "
            f"timestamp={event.get('timestamp')!r} ingest_time={event.get('ingest_time')!r}"
        )

    written: List[str] = []
    keys = list(groups)
    for i, key in enumerate(keys):
        try:
            written.append(write_group(key, groups[key]))
        except Exception as e:
            unwritten = [ev for k in keys[i:] for ev in groups[k]]
            raise PartialWriteError(written, unwritten, rejected, e) from e

    return WriteResult(paths=written, rejected=rejected)


def build_unified_filename(year: int, month: int, day: int, hour: int) -> str:
    """
    Build a unique Parquet file name for one batch.

    The UUID suffix guarantees that two batches landing in the same hour
    never share a name, so one batch can never overwrite another.
    The "unified_" prefix and ".parquet" suffix are kept so the Synapse
    bronze view wildcard (unified_*.parquet) still matches.
    """
    return f"unified_{year}-{month:02d}-{day:02d}_{hour:02d}_{uuid.uuid4().hex}.parquet"
