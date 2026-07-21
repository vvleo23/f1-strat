"""Validiert historische Renndaten über die REST-API von OpenF1."""

import sys
from typing import Any

import pandas as pd
import requests

if __package__:
    from .config import RAW_DATA_DIR
else:
    from config import RAW_DATA_DIR

BASE_URL = "https://api.openf1.org/v1"
REQUEST_TIMEOUT_SECONDS = 60


class OpenF1APIError(RuntimeError):
    """Beschreibt einen verständlichen Fehler beim OpenF1-Abruf."""


def get_json(endpoint: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    """Ruft einen OpenF1-Endpunkt ab und validiert dessen JSON-Antwort."""
    url = f"{BASE_URL}/{endpoint.lstrip('/')}"

    try:
        response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        payload = response.json()
    except requests.exceptions.Timeout as exc:
        raise OpenF1APIError(
            f"Zeitüberschreitung beim Abruf des Endpunkts '{endpoint}'."
        ) from exc
    except requests.exceptions.JSONDecodeError as exc:
        raise OpenF1APIError(
            f"Der Endpunkt '{endpoint}' lieferte kein gültiges JSON."
        ) from exc
    except requests.exceptions.HTTPError as exc:
        status_code = exc.response.status_code if exc.response is not None else "unbekannt"
        raise OpenF1APIError(
            f"HTTP-Fehler {status_code} beim Abruf des Endpunkts '{endpoint}'."
        ) from exc
    except requests.exceptions.RequestException as exc:
        raise OpenF1APIError(
            f"Netzwerkfehler beim Abruf des Endpunkts '{endpoint}': {exc}"
        ) from exc

    if not isinstance(payload, list):
        raise OpenF1APIError(
            f"Unerwartetes Antwortformat von '{endpoint}': Eine Liste wurde erwartet."
        )
    if not payload:
        raise OpenF1APIError(
            f"Der Endpunkt '{endpoint}' lieferte für {params} keine Daten."
        )
    if not all(isinstance(item, dict) for item in payload):
        raise OpenF1APIError(
            f"Der Endpunkt '{endpoint}' enthält unerwartete Datensätze."
        )

    return payload


def print_preview(name: str, frame: pd.DataFrame) -> None:
    """Gibt Dimension und erste Zeilen eines OpenF1-Datensatzes aus."""
    print(f"\n{name}: Shape {frame.shape}")
    print(frame.head().to_string(index=False))


def main() -> int:
    """Lädt OpenF1-Daten zum Italien-Grand-Prix 2024 und speichert Rohdaten."""
    try:
        print("Suche OpenF1-Session für das Rennen in Italien 2024 ...")
        sessions = get_json(
            "sessions",
            {
                "year": 2024,
                "country_name": "Italy",
                "location": "Monza",
                "session_name": "Race",
            },
        )
        monza_sessions = [
            session for session in sessions if session.get("location") == "Monza"
        ]
        if len(monza_sessions) != 1:
            raise OpenF1APIError(
                "Die Rennsession 2024 in Monza konnte nicht eindeutig bestimmt werden."
            )

        session_key = monza_sessions[0].get("session_key")
        if session_key is None:
            raise OpenF1APIError("Die gefundene Session enthält keinen session_key.")

        print(f"Session gefunden: session_key={session_key}")
        endpoint_params: dict[str, dict[str, Any]] = {
            "drivers": {"session_key": session_key},
            "laps": {"session_key": session_key},
            "weather": {"session_key": session_key},
            "pit": {"session_key": session_key},
            "race_control": {"session_key": session_key},
            "car_data": {"session_key": session_key, "driver_number": 1},
        }

        frames: dict[str, pd.DataFrame] = {}
        for endpoint, params in endpoint_params.items():
            print(f"Lade Endpunkt '{endpoint}' ...")
            frames[endpoint] = pd.DataFrame(get_json(endpoint, params))
            print_preview(endpoint, frames[endpoint])

        output_files = {
            "laps": RAW_DATA_DIR / "openf1_monza_2024_race_laps.parquet",
            "weather": RAW_DATA_DIR / "openf1_monza_2024_race_weather.parquet",
            "car_data": RAW_DATA_DIR
            / "openf1_monza_2024_race_car_data_driver_1.parquet",
        }
        for dataset, output_file in output_files.items():
            frames[dataset].to_parquet(output_file, index=False)
            print(f"{dataset} gespeichert: {output_file}")

        return 0
    except OpenF1APIError as exc:
        print(f"OpenF1-Fehler: {exc}", file=sys.stderr)
    except OSError as exc:
        print(f"Dateifehler beim Speichern der OpenF1-Daten: {exc}", file=sys.stderr)
    except Exception as exc:
        print(f"OpenF1-Daten konnten nicht verarbeitet werden: {exc}", file=sys.stderr)

    return 1


if __name__ == "__main__":
    raise SystemExit(main())


