"""Shared paths for the F1 event data pipeline."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CACHE_DIR = PROJECT_ROOT / "cache"
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
CURATED_DATA_DIR = PROJECT_ROOT / "data" / "curated"
ARTIFACTS_DIR = PROJECT_ROOT / "data" / "artifacts"

# Create local working directories when the pipeline is first used.
for directory in (CACHE_DIR, RAW_DATA_DIR, CURATED_DATA_DIR, ARTIFACTS_DIR):
    directory.mkdir(parents=True, exist_ok=True)

