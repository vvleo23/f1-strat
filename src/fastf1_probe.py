"""Validiert verfügbare FastF1-Daten am Monza-Qualifying 2024."""

import sys

import fastf1
import pandas as pd
import requests

if __package__:
    from .config import CACHE_DIR, RAW_DATA_DIR
else:
    from config import CACHE_DIR, RAW_DATA_DIR

OUTPUT_FILE = RAW_DATA_DIR / "fastf1_monza_2024_q_fastest_telemetry.parquet"
TELEMETRY_SAMPLE_COLUMNS = [
    "Distance",
    "Speed",
    "Throttle",
    "Brake",
    "nGear",
    "RPM",
]


def main() -> int:
    """Lädt die Session, untersucht ihre Daten und speichert Telemetrie."""
    try:
        fastf1.Cache.enable_cache(str(CACHE_DIR))
        print(f"FastF1-Cache aktiviert: {CACHE_DIR}")
        print("Lade Monza 2024 Qualifying inklusive Telemetrie und Wetter ...")

        session = fastf1.get_session(2024, "Monza", "Q")
        session.load(telemetry=True, weather=True)

        laps = session.laps
        weather_data = session.weather_data
        print(f"\nRunden: {laps.shape[0]} Zeilen")
        print(f"Verfügbare laps-Spalten:\n{list(laps.columns)}")
        print(f"\nWetterdaten: {weather_data.shape[0]} Zeilen")
        print(f"Verfügbare weather_data-Spalten:\n{list(weather_data.columns)}")

        fastest_lap = laps.pick_fastest()
        if fastest_lap is None or fastest_lap.empty:
            raise ValueError("FastF1 hat keine gültige schnellste Runde geliefert.")

        driver = fastest_lap.get("Driver", "unbekannt")
        lap_time = fastest_lap.get("LapTime", pd.NaT)
        print(f"\nSchnellste Runde: {driver}, Rundenzeit: {lap_time}")

        telemetry = fastest_lap.get_car_data().add_distance()
        if telemetry.empty:
            raise ValueError("Für die schnellste Runde wurde keine Car-Telemetrie geliefert.")

        print(f"Verfügbare telemetry-Spalten:\n{list(telemetry.columns)}")
        missing_columns = [
            column for column in TELEMETRY_SAMPLE_COLUMNS if column not in telemetry.columns
        ]
        if missing_columns:
            raise ValueError(
                "Erwartete Telemetriespalten fehlen: " + ", ".join(missing_columns)
            )

        print("\nBeispiel aus der Telemetrie der schnellsten Runde:")
        print(telemetry[TELEMETRY_SAMPLE_COLUMNS].head(10).to_string(index=False))

        telemetry.to_parquet(OUTPUT_FILE, index=False)
        print(f"\nTelemetrie erfolgreich gespeichert: {OUTPUT_FILE}")
        return 0
    except requests.exceptions.Timeout:
        print("Fehler: Zeitüberschreitung beim Abruf der FastF1-Daten.", file=sys.stderr)
    except requests.exceptions.RequestException as exc:
        print(f"Netzwerkfehler beim Abruf der FastF1-Daten: {exc}", file=sys.stderr)
    except (KeyError, TypeError, ValueError) as exc:
        print(f"Die geladenen FastF1-Daten konnten nicht verarbeitet werden: {exc}", file=sys.stderr)
    except OSError as exc:
        print(f"Dateifehler beim Speichern der FastF1-Daten: {exc}", file=sys.stderr)
    except Exception as exc:  # FastF1 verwendet mehrere eigene Ladefehler.
        print(f"FastF1-Session konnte nicht geladen werden: {exc}", file=sys.stderr)

    return 1


if __name__ == "__main__":
    raise SystemExit(main())

