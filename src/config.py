"""Zentrale Pfadkonfiguration für das Validierungsprojekt."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CACHE_DIR = PROJECT_ROOT / "cache"
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DATA_DIR = PROJECT_ROOT / "data" / "processed"

# Benötigte Arbeitsverzeichnisse beim Import der Konfiguration sicherstellen.
for directory in (CACHE_DIR, RAW_DATA_DIR, PROCESSED_DATA_DIR):
    directory.mkdir(parents=True, exist_ok=True)

