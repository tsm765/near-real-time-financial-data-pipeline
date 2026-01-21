from __future__ import annotations

from typing import List, Dict, Any

from azure.storage.blob import BlobServiceClient

from .parquet_utils import (
    get_partition_path_from_events,
    unified_events_to_parquet_bytes,
)


def write_curated_unified_azure(
    events: List[Dict[str, Any]],
    conn_str: str,
    container: str,
) -> str:
    """
    Write a batch of unified events as a Parquet blob to Azure Blob Storage / ADLS Gen2.

    - events: list of normalized event dicts (unified schema)
    - conn_str: Azure Storage connection string
    - container: container name, e.g. "finstream"

    Returns the blob path (within the container) as a string.
    """
    if not events:
        raise ValueError("write_curated_unified_azure called with empty events list")

    # Determine partition (based on min timestamp in the batch)
    year, month, day, hour = get_partition_path_from_events(events)

    # Build blob path for curated/unified_events
    blob_path = (
        f"curated/unified_events/"
        f"year={year}/month={month:02d}/day={day:02d}/"
        f"unified_{year}-{month:02d}-{day:02d}_{hour:02d}.parquet"
    )

    # Convert events to Parquet bytes
    data = unified_events_to_parquet_bytes(events)

    # Create client and upload
    service_client = BlobServiceClient.from_connection_string(conn_str)
    container_client = service_client.get_container_client(container)

    # NOTE: we assume the container already exists.
    # You will create it once in the Azure Portal or via CLI.
    blob_client = container_client.get_blob_client(blob_path)
    blob_client.upload_blob(data, overwrite=True)

    return blob_path
