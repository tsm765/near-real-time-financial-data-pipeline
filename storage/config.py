import os


def _getenv(name: str, default: str | None = None) -> str | None:
    """
    Helper to read environment variables.
    Returns default if the variable is missing or empty.
    """
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value


# STORAGE_MODE controls where data is written:
#   "none"  -> don't write, just skip (Part 1-style, printing only)
#   "local" -> write Parquet files to LOCAL_DATA_DIR
#   "azure" -> upload Parquet files to Azure Blob / ADLS Gen2
STORAGE_MODE: str = (_getenv("STORAGE_MODE", "none") or "none").lower()

# Local mode base directory, used when STORAGE_MODE="local"
LOCAL_DATA_DIR: str = _getenv("LOCAL_DATA_DIR", "./data") or "./data"

# Azure mode settings, used when STORAGE_MODE="azure"
AZURE_STORAGE_CONNECTION_STRING: str | None = _getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_CONTAINER_NAME: str = _getenv("AZURE_CONTAINER_NAME", "finstream") or "finstream"
