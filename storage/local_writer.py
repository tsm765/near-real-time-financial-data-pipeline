from __future__ import annotations

from pathlib import Path
from typing import List, Dict, Any

from .parquet_utils import (
    PartitionKey,
    WriteResult,
    build_unified_filename,
    unified_events_to_parquet_bytes,
    write_partitioned,
)


def write_curated_unified_local(events: List[Dict[str, Any]], base_dir: str) -> WriteResult:
    """
    Write a batch of unified events to local Parquet files, one file per
    (year, month, day, hour) of the events' own timestamps.

    - events: list of normalized event dicts (unified schema)
    - base_dir: local root folder, e.g. "./data"

    Returns a WriteResult (written file paths + rejected events).
    Raises PartialWriteError if a partition fails after others were written.
    """
    if not events:
        raise ValueError("write_curated_unified_local called with empty events list")

    def write_group(key: PartitionKey, group: List[Dict[str, Any]]) -> str:
        year, month, day, hour = key
        rel_dir = (
            Path("curated")
            / "unified_events"
            / f"year={year}"
            / f"month={month:02d}"
            / f"day={day:02d}"
        )
        # Unique per file -> files in the same hour never overwrite each other
        full_path = Path(base_dir) / rel_dir / build_unified_filename(year, month, day, hour)
        full_path.parent.mkdir(parents=True, exist_ok=True)

        data = unified_events_to_parquet_bytes(group)

        # "xb" = fail instead of overwriting an existing file
        with open(full_path, "xb") as f:
            f.write(data)
        return str(full_path)

    return write_partitioned(events, write_group)
