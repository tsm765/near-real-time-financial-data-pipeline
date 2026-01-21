from __future__ import annotations

from pathlib import Path
from typing import List, Dict, Any

from .parquet_utils import (
    get_partition_path_from_events,
    unified_events_to_parquet_bytes,
)


def write_curated_unified_local(events: List[Dict[str, Any]], base_dir: str) -> str:
    """
    Write a batch of unified events to a local Parquet file.

    - events: list of normalized event dicts (unified schema)
    - base_dir: local root folder, e.g. "./data"

    Returns the full path to the written Parquet file as a string.
    """
    if not events:
        raise ValueError("write_curated_unified_local called with empty events list")

    # Determine partition from events (based on min timestamp in the batch)
    year, month, day, hour = get_partition_path_from_events(events)

    # Build the relative directory and file name for curated/unified_events
    rel_dir = (
        Path("curated")
        / "unified_events"
        / f"year={year}"
        / f"month={month:02d}"
        / f"day={day:02d}"
    )
    filename = f"unified_{year}-{month:02d}-{day:02d}_{hour:02d}.parquet"

    # Full path on local disk
    full_path = Path(base_dir) / rel_dir / filename

    # Make sure the directory exists
    full_path.parent.mkdir(parents=True, exist_ok=True)

    # Convert events to Parquet bytes
    data = unified_events_to_parquet_bytes(events)

    # Write bytes to disk
    with open(full_path, "wb") as f:
        f.write(data)

    return str(full_path)
