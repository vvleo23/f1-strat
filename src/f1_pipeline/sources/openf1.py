from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Protocol

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from f1_pipeline.persistence import atomic_parquet
from f1_pipeline.settings import RAW_DATA_DIR

BASE_URL = "https://api.openf1.org/v1"
REQUEST_TIMEOUT_SECONDS = 90
MIN_REQUEST_INTERVAL_SECONDS = 2.1


class OpenF1Error(RuntimeError):
    pass


class JsonClient(Protocol):
    def get_json(self, endpoint: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        ...


class OpenF1Client:
    def __init__(
            self,
            *,
            error_type: type[RuntimeError] = OpenF1Error,
            user_agent: str = "f1-strat/1.0",
    ) -> None:
        try:
            import truststore
        except ImportError as exc:
            raise error_type(
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
        self._error_type = error_type
        self.session = requests.Session()
        self.session.mount("https://", HTTPAdapter(max_retries=retry))
        self.session.headers.update({"User-Agent": user_agent})
        self._last_request_at = 0.0

    def get_json(self, endpoint: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < MIN_REQUEST_INTERVAL_SECONDS:
            time.sleep(MIN_REQUEST_INTERVAL_SECONDS - elapsed)
        try:
            response: requests.Response | None = None
            for attempt in range(3):
                response = self.session.get(
                    f"{BASE_URL}/{endpoint.lstrip('/')}",
                    params=params,
                    timeout=REQUEST_TIMEOUT_SECONDS,
                )
                self._last_request_at = time.monotonic()
                if endpoint != "location" or response.status_code != 422 or attempt == 2:
                    break
                time.sleep(5 * (attempt + 1))
            if response is None:
                raise self._error_type(f"OpenF1 endpoint '{endpoint}' returned no response.")
            response.raise_for_status()
            payload = response.json()
        except requests.exceptions.Timeout as exc:
            raise self._error_type(f"OpenF1 endpoint '{endpoint}' timed out.") from exc
        except requests.exceptions.JSONDecodeError as exc:
            raise self._error_type(
                f"OpenF1 endpoint '{endpoint}' returned invalid JSON."
            ) from exc
        except requests.exceptions.RequestException as exc:
            raise self._error_type(f"OpenF1 endpoint '{endpoint}' failed: {exc}") from exc
        if not isinstance(payload, list) or not all(isinstance(row, dict) for row in payload):
            raise self._error_type(
                f"OpenF1 endpoint '{endpoint}' returned an invalid payload."
            )
        return payload


def season_cache_path(season: int, endpoint: str, suffix: str | None = None) -> Path:
    name = f"openf1_{season}_{endpoint}"
    if suffix:
        name += f"_{suffix}"
    return RAW_DATA_DIR / f"{name}.parquet"


def session_cache_path(session_key: int, endpoint: str) -> Path:
    return RAW_DATA_DIR / f"openf1_{session_key}_{endpoint}.parquet"


def location_driver_cache_path(session_key: int, driver_number: int) -> Path:
    return RAW_DATA_DIR / f"openf1_{session_key}_location_driver_{driver_number}.parquet"


def make_parquet_safe(endpoint: str, frame: pd.DataFrame) -> pd.DataFrame:
    safe = frame.copy()
    if endpoint == "intervals":
        for column in ("gap_to_leader", "interval"):
            if column in safe.columns:
                safe[column] = safe[column].map(
                    lambda value: None if pd.isna(value) else str(value)
                )
    return safe


def write_latest_parquet(frame: pd.DataFrame, path: Path) -> None:
    atomic_parquet(frame, path)
