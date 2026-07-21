"""Erstellt einen ersten Plausibilitätscheck der FastF1-Renndaten."""

import sys

import fastf1
import requests

if __package__:
    from .config import CACHE_DIR
else:
    from config import CACHE_DIR


def main() -> int:
    """Fasst die Runden des Monza-Rennens 2024 je Fahrer zusammen."""
    try:
        fastf1.Cache.enable_cache(str(CACHE_DIR))
        print("Lade Monza 2024 Race für den Plausibilitätscheck ...")
        session = fastf1.get_session(2024, "Monza", "R")
        session.load(telemetry=False, weather=False)

        laps = session.laps
        required_columns = {"Driver", "LapNumber", "LapTime"}
        missing_columns = required_columns.difference(laps.columns)
        if missing_columns:
            raise ValueError(
                "Erwartete Rundenspalten fehlen: " + ", ".join(sorted(missing_columns))
            )
        if laps.empty:
            raise ValueError("FastF1 hat für das Rennen keine Rundendaten geliefert.")

        summary = (
            laps.groupby("Driver", observed=True)
            .agg(
                Anzahl_Runden=("LapNumber", "count"),
                Beste_Rundenzeit=("LapTime", "min"),
                Durchschnittliche_Rundenzeit=("LapTime", "mean"),
            )
            .dropna(subset=["Beste_Rundenzeit"])
            .sort_values("Beste_Rundenzeit")
        )

        if summary.empty:
            raise ValueError("Es konnten keine gültigen Fahrer-Rundenzeiten ermittelt werden.")

        print("\nTop 10 nach bester Rundenzeit:")
        print(summary.head(10).to_string())
        print(
            "\nHinweis: Diese Übersicht ist ein erster Plausibilitätscheck "
            "und noch kein vollständiger Quellenvergleich."
        )
        return 0
    except requests.exceptions.Timeout:
        print("Fehler: Zeitüberschreitung beim Abruf der FastF1-Daten.", file=sys.stderr)
    except requests.exceptions.RequestException as exc:
        print(f"Netzwerkfehler beim Abruf der FastF1-Daten: {exc}", file=sys.stderr)
    except (KeyError, TypeError, ValueError) as exc:
        print(f"FastF1-Rundendaten konnten nicht ausgewertet werden: {exc}", file=sys.stderr)
    except Exception as exc:  # FastF1 kann eigene Session-Fehler auslösen.
        print(f"FastF1-Rennen konnte nicht geladen werden: {exc}", file=sys.stderr)

    return 1


if __name__ == "__main__":
    raise SystemExit(main())

