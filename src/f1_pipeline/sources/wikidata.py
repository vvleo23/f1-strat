from __future__ import annotations

import json
import os
import re
import time
import unicodedata
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Mapping, Protocol

import math
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from f1_pipeline.persistence import atomic_json, sha256
from f1_pipeline.settings import CURATED_DATA_DIR, PROJECT_ROOT, RAW_DATA_DIR

API_URL = "https://www.wikidata.org/w/api.php"
REQUEST_TIMEOUT_SECONDS = 30
REVIEWED_CIRCUIT_MAPPINGS_PATH = PROJECT_ROOT / "config" / "reviewed_circuit_mappings.json"
AUTO_CIRCUIT_MAPPINGS_PATH = (
        CURATED_DATA_DIR / "registries" / "auto_wikidata_circuit_mappings.json"
)
AUTO_MAPPING_SCHEMA_VERSION = 1


class WikidataError(RuntimeError):
    pass


class MissingCircuitMappingError(WikidataError):
    def __init__(self, circuit_key: int, mapping: dict[str, Any]) -> None:
        super().__init__(f"Circuit key {circuit_key} has no reviewed Wikidata mapping.")
        self.circuit_key = circuit_key
        self.mapping = mapping


class CircuitResolutionError(WikidataError):
    def __init__(self, message: str, result: dict[str, Any]) -> None:
        super().__init__(message)
        self.result = result


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


def _stored_path(value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else PROJECT_ROOT / path


def _normalized_name(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode()
    words = re.findall(r"[a-z0-9]+", normalized.casefold())
    ignored = {
        "autodrome",
        "autodromo",
        "circuit",
        "de",
        "grand",
        "international",
        "prix",
        "raceway",
        "racing",
        "street",
        "the",
    }
    return " ".join(word for word in words if word not in ignored)


def _name_matches(expected: str, candidate: str) -> bool:
    if not expected or not candidate:
        return False
    return (
            expected in candidate
            or candidate in expected
            or SequenceMatcher(None, expected, candidate).ratio() >= 0.9
    )


def _country_matches(country_name: str, description: str) -> bool:
    expected = " ".join(re.findall(r"[a-z0-9]+", country_name.casefold()))
    actual = " ".join(re.findall(r"[a-z0-9]+", description.casefold()))
    if expected and expected in actual:
        return True
    aliases = {
        "united arab emirates": {"uae", "u a e"},
        "united kingdom": {"uk", "u k"},
        "united states": {"us", "u s", "usa", "u s a"},
    }
    actual_words = set(actual.split())
    return any(
        alias in actual if " " in alias else alias in actual_words
        for alias in aliases.get(expected, set())
    )


def _auto_registry(path: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not path.exists():
        return [], {
            "path": _relative_path(path),
            "schema_version": AUTO_MAPPING_SCHEMA_VERSION,
            "record_count": 0,
            "sha256": None,
        }
    try:
        with path.open(encoding="utf-8") as stream:
            payload = json.load(stream)
    except (OSError, json.JSONDecodeError) as exc:
        raise WikidataError(f"Automatic circuit mapping could not be loaded: {exc}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != AUTO_MAPPING_SCHEMA_VERSION:
        raise WikidataError("Automatic circuit mapping has an unsupported schema.")
    records = payload.get("mappings")
    if not isinstance(records, list) or any(not isinstance(record, dict) for record in records):
        raise WikidataError("Automatic circuit mapping contains invalid records.")
    keys: set[int] = set()
    for record in records:
        circuit_key = record.get("source_circuit_key")
        entity_id = record.get("wikidata_entity_id")
        if (
                record.get("source_system") != "openf1"
                or record.get("verification_status") != "auto_verified"
                or isinstance(circuit_key, bool)
                or not isinstance(circuit_key, int)
                or circuit_key <= 0
                or not isinstance(entity_id, str)
                or re.fullmatch(r"Q[1-9][0-9]*", entity_id) is None
                or circuit_key in keys
        ):
            raise WikidataError("Automatic circuit mapping contains an invalid record.")
        keys.add(circuit_key)
    return records, {
        "path": _relative_path(path),
        "schema_version": AUTO_MAPPING_SCHEMA_VERSION,
        "record_count": len(records),
        "sha256": sha256(path),
    }


def _write_auto_mapping(record: dict[str, Any], path: Path) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_suffix(f"{path.suffix}.lock")
    deadline = time.monotonic() + 10
    descriptor: int | None = None
    while descriptor is None:
        try:
            descriptor = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            if time.monotonic() >= deadline:
                raise WikidataError("Automatic circuit mapping is locked by another pipeline run.")
            time.sleep(0.05)
    os.close(descriptor)
    try:
        records, _ = _auto_registry(path)
        records = [
            existing
            for existing in records
            if existing.get("source_circuit_key") != record["source_circuit_key"]
        ]
        records.append(record)
        records.sort(key=lambda item: int(item["source_circuit_key"]))
        atomic_json(
            {"schema_version": AUTO_MAPPING_SCHEMA_VERSION, "mappings": records},
            path,
        )
    finally:
        lock_path.unlink(missing_ok=True)
    _, provenance = _auto_registry(path)
    return provenance


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


class WikidataSearchProvider(Protocol):
    def search_entities(self, query: str) -> dict[str, Any]: ...


class WikidataProvider(WikidataSearchProvider, Protocol):
    def get_entity(self, entity_id: str) -> dict[str, Any]: ...


def discover_circuit_candidates(
        circuit_key: int,
        circuit_name: str,
        *,
        location: str | None = None,
        client: WikidataSearchProvider | None = None,
        retrieved_at: datetime | None = None,
        mapping_path: Path = REVIEWED_CIRCUIT_MAPPINGS_PATH,
        raw_dir: Path = RAW_DATA_DIR,
) -> dict[str, Any]:
    _, mapping = load_reviewed_circuits(mapping_path)
    query = circuit_name.strip() if isinstance(circuit_name, str) else ""
    if not query:
        raise WikidataError("Circuit candidate search requires a circuit name.")
    checked_at = retrieved_at or datetime.now(timezone.utc)
    active_client: WikidataSearchProvider = client if client is not None else WikidataClient()
    payload = active_client.search_entities(query)
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


def _auto_reference(
        circuit_key: int,
        circuit_name: str,
        country_name: str,
        entity_id: str,
        payload: dict[str, Any],
        *,
        location: str | None,
        retrieved_at: datetime,
        raw_path: Path,
) -> CircuitReference:
    entity = payload.get("entities", {}).get(entity_id)
    if not isinstance(entity, dict) or entity.get("missing") is not None:
        raise WikidataError(f"Wikidata entity {entity_id} is unavailable.")
    label = entity.get("labels", {}).get("en", {}).get("value")
    description = str(entity.get("descriptions", {}).get("en", {}).get("value", ""))
    normalized_label = _normalized_name(str(label or ""))
    normalized_description_name = _normalized_name(description)
    normalized_circuit = _normalized_name(circuit_name)
    normalized_location = _normalized_name(location or "")
    name_matches = bool(
        normalized_circuit
        and (
                _name_matches(normalized_circuit, normalized_label)
                or _name_matches(normalized_circuit, normalized_description_name)
        )
    )
    location_matches = bool(
        normalized_location
        and (
                _name_matches(normalized_location, normalized_label)
                or _name_matches(normalized_location, normalized_description_name)
        )
    )
    if not isinstance(label, str) or not (name_matches or location_matches):
        raise WikidataError(f"Wikidata entity {entity_id} does not match the OpenF1 circuit name.")
    normalized_description = description.casefold()
    if not _country_matches(country_name, description):
        raise WikidataError(f"Wikidata entity {entity_id} does not match the OpenF1 country.")
    if not any(
            term in normalized_description
            for term in ("circuit", "motor racing", "motorsport", "race track", "racetrack")
    ):
        raise WikidataError(f"Wikidata entity {entity_id} is not described as a racing circuit.")
    coordinate_claims = entity.get("claims", {}).get("P625", [])
    coordinates = [
        claim.get("mainsnak", {}).get("datavalue", {}).get("value")
        for claim in coordinate_claims
        if isinstance(claim, dict)
           and claim.get("rank") != "deprecated"
           and claim.get("mainsnak", {}).get("snaktype") == "value"
    ]
    coordinates = [value for value in coordinates if isinstance(value, dict)]
    if len(coordinates) != 1:
        raise WikidataError(f"Wikidata entity {entity_id} does not have one usable P625 coordinate.")
    coordinate = coordinates[0]
    try:
        latitude = float(coordinate["latitude"])
        longitude = float(coordinate["longitude"])
        revision = int(entity["lastrevid"])
    except (KeyError, TypeError, ValueError) as exc:
        raise WikidataError(f"Wikidata entity {entity_id} has invalid coordinate metadata.") from exc
    if not math.isfinite(latitude) or not -90 <= latitude <= 90:
        raise WikidataError("Wikidata latitude is outside the WGS84 range.")
    if not math.isfinite(longitude) or not -180 <= longitude <= 180:
        raise WikidataError("Wikidata longitude is outside the WGS84 range.")
    if coordinate.get("globe") != "http://www.wikidata.org/entity/Q2":
        raise WikidataError("Wikidata coordinate is not referenced to Earth.")
    return CircuitReference(
        source_circuit_key=circuit_key,
        wikidata_entity_id=entity_id,
        circuit_label=label,
        latitude=latitude,
        longitude=longitude,
        crs="EPSG:4326",
        coordinate_revision=revision,
        coordinate_retrieved_at=retrieved_at.astimezone(timezone.utc).isoformat(),
        verification_status="auto_verified",
        raw_path=_relative_path(raw_path),
        raw_sha256=sha256(raw_path),
    )


def resolve_circuit_reference(
        circuit_key: int,
        circuit_name: str,
        country_name: str,
        *,
        location: str | None = None,
        refresh: bool = False,
        client: WikidataProvider | None = None,
        retrieved_at: datetime | None = None,
        mapping_path: Path = REVIEWED_CIRCUIT_MAPPINGS_PATH,
        auto_mapping_path: Path = AUTO_CIRCUIT_MAPPINGS_PATH,
        raw_dir: Path = RAW_DATA_DIR,
) -> tuple[CircuitReference, dict[str, Any]]:
    mappings, reviewed_mapping = load_reviewed_circuits(mapping_path)
    if circuit_key in mappings:
        return load_circuit_reference(
            circuit_key,
            refresh=refresh,
            client=client,
            retrieved_at=retrieved_at,
            mapping_path=mapping_path,
            raw_dir=raw_dir,
        )
    checked_at = retrieved_at or datetime.now(timezone.utc)
    records, auto_mapping = _auto_registry(auto_mapping_path)
    stored = next(
        (record for record in records if record["source_circuit_key"] == circuit_key),
        None,
    )
    if stored is not None and not refresh:
        raw_path = _stored_path(str(stored["entity_raw_path"]))
        try:
            if sha256(raw_path) != stored.get("entity_raw_sha256"):
                raise WikidataError("Automatic Wikidata entity snapshot failed hash validation.")
            with raw_path.open(encoding="utf-8") as stream:
                payload = json.load(stream)
            reference = _auto_reference(
                circuit_key,
                circuit_name,
                country_name,
                str(stored["wikidata_entity_id"]),
                payload,
                location=location,
                retrieved_at=datetime.fromisoformat(str(stored["retrieved_at"])),
                raw_path=raw_path,
            )
        except (OSError, json.JSONDecodeError, ValueError, KeyError, WikidataError):
            stored = None
        else:
            return reference, {
                "status": "stale",
                "fetched": False,
                "request": {"url": API_URL, "entity_id": reference.wikidata_entity_id},
                "mapping": reviewed_mapping,
                "auto_mapping": auto_mapping,
                "resolution": "auto_registry",
            }
    query = circuit_name.strip()
    if not _normalized_name(circuit_name) or not country_name.strip():
        raise WikidataError("Automatic circuit resolution requires a circuit name and country.")
    active_client = client or WikidataClient()
    search_payload = active_client.search_entities(query)
    if not isinstance(search_payload.get("search"), list):
        raise WikidataError("Wikidata candidate search returned an unexpected response format.")
    timestamp = checked_at.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    search_path = (
            raw_dir
            / "snapshots"
            / "wikidata"
            / "candidates"
            / f"openf1_circuit_{circuit_key}_{timestamp}.json"
    )
    atomic_json(
        {
            "source_circuit_key": circuit_key,
            "source_circuit_name": circuit_name,
            "source_country": country_name,
            "query": query,
            "retrieved_at": checked_at.astimezone(timezone.utc).isoformat(),
            "response": search_payload,
        },
        search_path,
    )
    validated: list[tuple[CircuitReference, Path]] = []
    rejected: list[dict[str, str]] = []
    seen: set[str] = set()
    for candidate in search_payload.get("search", []):
        if not isinstance(candidate, dict):
            continue
        entity_id = candidate.get("id")
        if (
                not isinstance(entity_id, str)
                or re.fullmatch(r"Q[1-9][0-9]*", entity_id) is None
                or entity_id in seen
        ):
            continue
        seen.add(entity_id)
        try:
            entity_payload = active_client.get_entity(entity_id)
        except WikidataError as exc:
            rejected.append({"wikidata_entity_id": entity_id, "reason": str(exc)})
            continue
        entity_path = raw_dir / "snapshots" / "wikidata" / f"{entity_id}_{timestamp}.json"
        atomic_json(entity_payload, entity_path)
        try:
            reference = _auto_reference(
                circuit_key,
                circuit_name,
                country_name,
                entity_id,
                entity_payload,
                location=location,
                retrieved_at=checked_at,
                raw_path=entity_path,
            )
        except WikidataError as exc:
            rejected.append({"wikidata_entity_id": entity_id, "reason": str(exc)})
        else:
            validated.append((reference, entity_path))
    base_result = {
        "fetched": True,
        "request": {"url": API_URL, "query": query, "limit": 5},
        "mapping": reviewed_mapping,
        "auto_mapping": auto_mapping,
        "search_raw_path": _relative_path(search_path),
        "search_raw_sha256": sha256(search_path),
        "validated_candidates": [item[0].wikidata_entity_id for item in validated],
        "rejected_candidates": rejected,
    }
    if len(validated) != 1:
        status = "partial" if validated or seen else "unavailable"
        result = {
            **base_result,
            "status": status,
            "error": (
                "Automatic Wikidata resolution is ambiguous."
                if len(validated) > 1
                else "No Wikidata candidate passed automatic circuit verification."
            ),
        }
        raise CircuitResolutionError(str(result["error"]), result)
    reference, entity_path = validated[0]
    record = {
        "source_system": "openf1",
        "source_circuit_key": circuit_key,
        "source_circuit_name": circuit_name,
        "source_location": location,
        "source_country": country_name,
        "wikidata_entity_id": reference.wikidata_entity_id,
        "wikidata_label": reference.circuit_label,
        "coordinate_revision": reference.coordinate_revision,
        "search_raw_path": _relative_path(search_path),
        "search_raw_sha256": sha256(search_path),
        "entity_raw_path": _relative_path(entity_path),
        "entity_raw_sha256": reference.raw_sha256,
        "retrieved_at": reference.coordinate_retrieved_at,
        "verification_status": "auto_verified",
    }
    auto_mapping = _write_auto_mapping(record, auto_mapping_path)
    return reference, {
        **base_result,
        "status": "available",
        "raw_path": reference.raw_path,
        "raw_sha256": reference.raw_sha256,
        "auto_mapping": auto_mapping,
        "resolution": "auto_verified",
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
        client: WikidataProvider | None = None,
        retrieved_at: datetime | None = None,
        mapping_path: Path = REVIEWED_CIRCUIT_MAPPINGS_PATH,
        raw_dir: Path = RAW_DATA_DIR,
) -> tuple[CircuitReference, dict[str, Any]]:
    mappings, mapping = load_reviewed_circuits(mapping_path)
    reviewed = mappings.get(circuit_key)
    if reviewed is None:
        raise MissingCircuitMappingError(circuit_key, mapping)
    checked_at = retrieved_at or datetime.now(timezone.utc)
    snapshot_dir = raw_dir / "snapshots" / "wikidata"
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
