from __future__ import annotations

import io
import uuid
from typing import Any, Dict, List

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

    # 3) Enforce datetime types (UTC) for timestamp and ingest_time
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df["ingest_time"] = pd.to_datetime(df["ingest_time"], utc=True, errors="coerce")

    # 4) Enforce string columns
    for col in ["event_type", "ticker", "source", "topic"]:
        df[col] = df[col].astype("string")

    # 5) Enforce numeric types (nullable)
    # price -> float, mentions -> nullable integer
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["mentions"] = pd.to_numeric(df["mentions"], errors="coerce").astype("Int64")

    # 6) Drop any rows with no timestamp or ticker (invalid for analytics)
    df = df.dropna(subset=["timestamp", "ticker"])
    if df.empty:
        raise ValueError("All rows became invalid after type enforcement (missing timestamp/ticker)")

    # 7) Write DataFrame to Parquet in memory
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    return buffer.getvalue()


def get_partition_path_from_events(events: List[Dict[str, Any]]) -> tuple[int, int, int, int]:
    """
    Given a list of events, look at their timestamp field and pick an "anchor" time.
    We use the minimum timestamp in the batch as the anchor for partitioning.

    Returns: (year, month, day, hour) as integers.
    """
    ts = pd.to_datetime(
        [e.get("timestamp") for e in events],
        utc=True,
        errors="coerce",
    )
    ts = ts.dropna()
    if ts.empty:
        raise ValueError("No valid timestamps in events for partitioning")

    anchor = ts.min()
    return int(anchor.year), int(anchor.month), int(anchor.day), int(anchor.hour)


def build_unified_filename(year: int, month: int, day: int, hour: int) -> str:
    """
    Build a unique Parquet file name for one batch.

    The UUID suffix guarantees that two batches landing in the same hour
    never share a name, so one batch can never overwrite another.
    The "unified_" prefix and ".parquet" suffix are kept so the Synapse
    bronze view wildcard (unified_*.parquet) still matches.
    """
    return f"unified_{year}-{month:02d}-{day:02d}_{hour:02d}_{uuid.uuid4().hex}.parquet"
