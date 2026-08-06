from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import math
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from f1_pipeline.persistence import atomic_json, sha256
from f1_pipeline.settings import PROJECT_ROOT, RAW_DATA_DIR

API_URL = "https://www.wikidata.org/w/api.php"
REQUEST_TIMEOUT_SECONDS = 30
REVIEWED_CIRCUIT_MAPPINGS_PATH = PROJECT_ROOT / "config" / "reviewed_circuit_mappings.json"


class WikidataError(RuntimeError):
    pass


class MissingCircuitMappingError(WikidataError):
    def __init__(self, circuit_key: int, mapping: dict[str, Any]) -> None:
        super().__init__(f"Circuit key {circuit_key} has no reviewed Wikidata mapping.")
        self.circuit_key = circuit_key
        self.mapping = mapping


@dataclass(frozen=True)
class ReviewedCircuit:
    entity_id: str
    expected_label: str
    expected_country: str


@dataclass(frozen=True)
class CircuitReference:
    source_circuit_key: int
    wikidata_entity_id: str
    circuit_label: str
    latitude: float
    longitude: float
    crs: str
    coordinate_revision: int
    coordinate_retrieved_at: str
    verification_status: str
    raw_path: str
    raw_sha256: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _relative_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError:
        return str(path)


def load_reviewed_circuits(
    path: Path = REVIEWED_CIRCUIT_MAPPINGS_PATH,
) -> tuple[dict[int, ReviewedCircuit], dict[str, Any]]:
    try:
        with path.open(encoding="utf-8") as stream:
            payload = json.load(stream)
    except (OSError, json.JSONDecodeError) as exc:
        raise WikidataError(f"Reviewed circuit mapping could not be loaded: {exc}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != 1:
        raise WikidataError("Reviewed circuit mapping has an unsupported schema.")
    records = payload.get("mappings")
    if not isinstance(records, list) or not records:
        raise WikidataError("Reviewed circuit mapping must contain at least one record.")
    mappings: dict[int, ReviewedCircuit] = {}
    entity_ids: set[str] = set()
    for record in records:
        if not isinstance(record, dict):
            raise WikidataError("Reviewed circuit mapping contains an invalid record.")
        circuit_key = record.get("source_circuit_key")
        entity_id = record.get("wikidata_entity_id")
        expected_label = record.get("expected_label")
        expected_country = record.get("expected_country")
        if record.get("source_system") != "openf1" or record.get("review_status") != "reviewed":
            raise WikidataError("Circuit mappings must be reviewed OpenF1 records.")
        if isinstance(circuit_key, bool) or not isinstance(circuit_key, int) or circuit_key <= 0:
            raise WikidataError("Circuit mappings require a positive integer source_circuit_key.")
        if not isinstance(entity_id, str) or re.fullmatch(r"Q[1-9][0-9]*", entity_id) is None:
            raise WikidataError("Circuit mappings require a valid Wikidata entity ID.")
        if not isinstance(expected_label, str) or not expected_label.strip():
            raise WikidataError("Circuit mappings require an expected label.")
        if not isinstance(expected_country, str) or not expected_country.strip():
            raise WikidataError("Circuit mappings require an expected country.")
        if circuit_key in mappings or entity_id in entity_ids:
            raise WikidataError("Circuit mappings must have unique circuit keys and Wikidata entities.")
        mappings[circuit_key] = ReviewedCircuit(
            entity_id=entity_id,
            expected_label=expected_label.strip(),
            expected_country=expected_country.strip(),
        )
        entity_ids.add(entity_id)
    return mappings, {
        "path": _relative_path(path),
        "sha256": sha256(path),
        "schema_version": 1,
        "record_count": len(mappings),
    }


class WikidataClient:
    def __init__(self) -> None:
        try:
            import truststore
        except ImportError as exc:
            raise WikidataError(
                "The 'truststore' package is missing. Run 'pip install -r requirements.txt'."
            ) from exc
        truststore.inject_into_ssl()
        retry = Retry(
            total=4,
            connect=4,
            read=4,
            status=4,
            backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET"}),
            respect_retry_after_header=True,
        )
        self.session = requests.Session()
        self.session.mount("https://", HTTPAdapter(max_retries=retry))
        self.session.headers.update({"User-Agent": "f1-strat/1.0"})

    def get_entity(self, entity_id: str) -> dict[str, Any]:
        params = {
            "action": "wbgetentities",
            "ids": entity_id,
            "props": "info|labels|descriptions|claims",
            "languages": "en",
            "format": "json",
            "formatversion": 2,
        }
        try:
            response = self.session.get(API_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()
            payload = response.json()
        except requests.exceptions.Timeout as exc:
            raise WikidataError("Wikidata request timed out.") from exc
        except requests.exceptions.JSONDecodeError as exc:
            raise WikidataError("Wikidata did not return valid JSON.") from exc
        except requests.exceptions.RequestException as exc:
            raise WikidataError(f"Wikidata request failed: {exc}") from exc
        if not isinstance(payload, dict):
            raise WikidataError("Wikidata returned an unexpected response format.")
        return payload

    def search_entities(self, query: str) -> dict[str, Any]:
        params = {
            "action": "wbsearchentities",
            "search": query,
            "language": "en",
            "uselang": "en",
            "type": "item",
            "limit": 5,
            "format": "json",
            "formatversion": 2,
        }
        try:
            response = self.session.get(API_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()
            payload = response.json()
        except requests.exceptions.Timeout as exc:
            raise WikidataError("Wikidata candidate search timed out.") from exc
        except requests.exceptions.JSONDecodeError as exc:
            raise WikidataError("Wikidata candidate search did not return valid JSON.") from exc
        except requests.exceptions.RequestException as exc:
            raise WikidataError(f"Wikidata candidate search failed: {exc}") from exc
        if not isinstance(payload, dict) or not isinstance(payload.get("search"), list):
            raise WikidataError("Wikidata candidate search returned an unexpected response format.")
        return payload


def discover_circuit_candidates(
    circuit_key: int,
    circuit_name: str,
    *,
    location: str | None = None,
    client: WikidataClient | None = None,
    retrieved_at: datetime | None = None,
    mapping_path: Path = REVIEWED_CIRCUIT_MAPPINGS_PATH,
    raw_dir: Path = RAW_DATA_DIR,
) -> dict[str, Any]:
    _, mapping = load_reviewed_circuits(mapping_path)
    query = circuit_name.strip() if isinstance(circuit_name, str) else ""
    if not query:
        raise WikidataError("Circuit candidate search requires a circuit name.")
    checked_at = retrieved_at or datetime.now(timezone.utc)
    payload = (client or WikidataClient()).search_entities(query)
    snapshot = {
        "source_circuit_key": circuit_key,
        "source_location": location,
        "query": query,
        "retrieved_at": checked_at.astimezone(timezone.utc).isoformat(),
        "response": payload,
    }
    timestamp = checked_at.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    raw_path = (
        raw_dir
        / "snapshots"
        / "wikidata"
        / "candidates"
        / f"openf1_circuit_{circuit_key}_{timestamp}.json"
    )
    atomic_json(snapshot, raw_path)
    candidates: list[dict[str, str]] = []
    for candidate in payload["search"]:
        if not isinstance(candidate, dict):
            continue
        entity_id = candidate.get("id")
        if not isinstance(entity_id, str) or re.fullmatch(r"Q[1-9][0-9]*", entity_id) is None:
            continue
        candidates.append(
            {
                "wikidata_entity_id": entity_id,
                "label": str(candidate.get("label", "")),
                "description": str(candidate.get("description", "")),
            }
        )
    return {
        "status": "partial" if candidates else "unavailable",
        "fetched": True,
        "error": f"Circuit key {circuit_key} requires a reviewed Wikidata mapping.",
        "request": {
            "url": API_URL,
            "query": query,
            "source_location": location,
            "limit": 5,
        },
        "mapping": mapping,
        "candidates": candidates,
        "raw_path": _relative_path(raw_path),
        "raw_sha256": sha256(raw_path),
    }


def normalize_circuit_reference(
    circuit_key: int,
    payload: dict[str, Any],
    *,
    retrieved_at: datetime,
    raw_path: Path,
    reviewed_circuits: Mapping[int, ReviewedCircuit] | None = None,
) -> CircuitReference:
    mappings = reviewed_circuits
    if mappings is None:
        mappings, _ = load_reviewed_circuits()
    reviewed = mappings.get(circuit_key)
    if reviewed is None:
        raise WikidataError(f"Circuit key {circuit_key} has no reviewed Wikidata mapping.")
    entity = payload.get("entities", {}).get(reviewed.entity_id)
    if not isinstance(entity, dict) or entity.get("missing") is not None:
        raise WikidataError(f"Wikidata entity {reviewed.entity_id} is unavailable.")
    label = entity.get("labels", {}).get("en", {}).get("value")
    description = entity.get("descriptions", {}).get("en", {}).get("value", "")
    if label != reviewed.expected_label or reviewed.expected_country.casefold() not in str(description).casefold():
        raise WikidataError(f"Wikidata entity {reviewed.entity_id} does not match the reviewed circuit.")
    coordinate_claims = entity.get("claims", {}).get("P625", [])
    coordinates = [
        claim.get("mainsnak", {}).get("datavalue", {}).get("value")
        for claim in coordinate_claims
        if claim.get("rank") != "deprecated"
        and claim.get("mainsnak", {}).get("snaktype") == "value"
    ]
    coordinates = [value for value in coordinates if isinstance(value, dict)]
    if len(coordinates) != 1:
        raise WikidataError(f"Wikidata entity {reviewed.entity_id} does not have one usable P625 coordinate.")
    coordinate = coordinates[0]
    try:
        latitude = float(coordinate["latitude"])
        longitude = float(coordinate["longitude"])
        revision = int(entity["lastrevid"])
    except (KeyError, TypeError, ValueError) as exc:
        raise WikidataError(f"Wikidata entity {reviewed.entity_id} has invalid coordinate metadata.") from exc
    if not math.isfinite(latitude) or not -90 <= latitude <= 90:
        raise WikidataError("Wikidata latitude is outside the WGS84 range.")
    if not math.isfinite(longitude) or not -180 <= longitude <= 180:
        raise WikidataError("Wikidata longitude is outside the WGS84 range.")
    if coordinate.get("globe") != "http://www.wikidata.org/entity/Q2":
        raise WikidataError("Wikidata coordinate is not referenced to Earth.")
    return CircuitReference(
        source_circuit_key=circuit_key,
        wikidata_entity_id=reviewed.entity_id,
        circuit_label=str(label),
        latitude=latitude,
        longitude=longitude,
        crs="EPSG:4326",
        coordinate_revision=revision,
        coordinate_retrieved_at=retrieved_at.astimezone(timezone.utc).isoformat(),
        verification_status="verified",
        raw_path=_relative_path(raw_path),
        raw_sha256=sha256(raw_path),
    )


def load_circuit_reference(
    circuit_key: int,
    *,
    refresh: bool = False,
    client: WikidataClient | None = None,
    retrieved_at: datetime | None = None,
    mapping_path: Path = REVIEWED_CIRCUIT_MAPPINGS_PATH,
) -> tuple[CircuitReference, dict[str, Any]]:
    mappings, mapping = load_reviewed_circuits(mapping_path)
    reviewed = mappings.get(circuit_key)
    if reviewed is None:
        raise MissingCircuitMappingError(circuit_key, mapping)
    checked_at = retrieved_at or datetime.now(timezone.utc)
    snapshot_dir = RAW_DATA_DIR / "snapshots" / "wikidata"
    existing: list[Path] = sorted(snapshot_dir.glob(f"{reviewed.entity_id}_*.json"))
    fetched = refresh or not existing
    payload: dict[str, Any] = {}
    raw_path: Path
    if fetched:
        payload = (client or WikidataClient()).get_entity(reviewed.entity_id)
        timestamp = checked_at.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        raw_path = snapshot_dir / f"{reviewed.entity_id}_{timestamp}.json"
        atomic_json(payload, raw_path)
        snapshot_retrieved_at = checked_at
    else:
        raw_path = existing[-1]
        with raw_path.open(encoding="utf-8") as stream:
            payload = json.load(stream)
        snapshot_retrieved_at = datetime.fromtimestamp(
            raw_path.stat().st_mtime, timezone.utc
        )
    reference = normalize_circuit_reference(
        circuit_key,
        payload,
        retrieved_at=snapshot_retrieved_at,
        raw_path=raw_path,
        reviewed_circuits=mappings,
    )
    return reference, {
        "status": "available" if fetched else "stale",
        "fetched": fetched,
        "request": {"url": API_URL, "entity_id": reviewed.entity_id},
        "mapping": mapping,
    }



