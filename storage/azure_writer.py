from __future__ import annotations

from typing import List, Dict, Any

from azure.storage.blob import BlobServiceClient

from .parquet_utils import (
    PartitionKey,
    WriteResult,
    build_unified_filename,
    unified_events_to_parquet_bytes,
    write_partitioned,
)


def write_curated_unified_azure(
    events: List[Dict[str, Any]],
    conn_str: str,
    container: str,
) -> WriteResult:
    """
    Write a batch of unified events as Parquet blobs to Azure Blob Storage / ADLS Gen2,
    one blob per (year, month, day, hour) of the events' own timestamps.

    - events: list of normalized event dicts (unified schema)
    - conn_str: Azure Storage connection string
    - container: container name, e.g. "finstream"

    Returns a WriteResult (blob paths within the container + rejected events).
    Raises PartialWriteError if a partition fails after others were written.
    """
    if not events:
        raise ValueError("write_curated_unified_azure called with empty events list")

    # NOTE: we assume the container already exists.
    # You will create it once in the Azure Portal or via CLI.
    service_client = BlobServiceClient.from_connection_string(conn_str)
    container_client = service_client.get_container_client(container)

    def write_group(key: PartitionKey, group: List[Dict[str, Any]]) -> str:
        year, month, day, hour = key
        # Unique blob name per file -> files in the same hour never overwrite each other
        blob_path = (
            f"curated/unified_events/"
            f"year={year}/month={month:02d}/day={day:02d}/"
            f"{build_unified_filename(year, month, day, hour)}"
        )
        data = unified_events_to_parquet_bytes(group)
        blob_client = container_client.get_blob_client(blob_path)
        # overwrite=False: if a name ever clashed, fail loudly instead of silently losing data
        blob_client.upload_blob(data, overwrite=False)
        return blob_path

    return write_partitioned(events, write_group)
