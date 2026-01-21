from __future__ import annotations

from typing import List, Dict, Any, Optional

from .config import (
    STORAGE_MODE,
    LOCAL_DATA_DIR,
    AZURE_STORAGE_CONNECTION_STRING,
    AZURE_CONTAINER_NAME,
)

from .local_writer import write_curated_unified_local
from .azure_writer import write_curated_unified_azure


def write_curated_unified(events: List[Dict[str, Any]]) -> Optional[str]:
    """
    Route a batch of unified events to the correct storage destination based on STORAGE_MODE.

    STORAGE_MODE:
      - "none": do nothing, return None
      - "local": write to local Parquet, return file path
      - "azure": upload to ADLS Gen2 (Blob), return blob path
    """
    mode = STORAGE_MODE

    if mode == "none":
        return None

    if mode == "local":
        return write_curated_unified_local(events, base_dir=LOCAL_DATA_DIR)

    if mode == "azure":
        if not AZURE_STORAGE_CONNECTION_STRING:
            raise RuntimeError(
                "STORAGE_MODE=azure but AZURE_STORAGE_CONNECTION_STRING is missing."
            )
        return write_curated_unified_azure(
            events,
            conn_str=AZURE_STORAGE_CONNECTION_STRING,
            container=AZURE_CONTAINER_NAME,
        )

    raise ValueError(f"Unknown STORAGE_MODE: {mode}")
