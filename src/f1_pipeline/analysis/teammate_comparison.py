from __future__ import annotations

import pandas as pd


COMPARISON_METRICS = (
    "Races",
    "Points",
    "Wins",
    "Podiums",
    "Best finish",
    "Average finish",
    "DNFs",
    "DNF rate",
    "Qualifying sessions",
    "Best qualifying",
    "Average qualifying position",
)


def session_conditions(
    weather: pd.DataFrame,
    stints: pd.DataFrame,
    race_control: pd.DataFrame,
) -> tuple[str, float | None]:
    rainfall = pd.to_numeric(
        weather.get("rainfall", pd.Series(dtype=float)), errors="coerce"
    )
    temperatures = pd.to_numeric(
        weather.get("air_temperature", pd.Series(dtype=float)), errors="coerce"
    ).dropna()
    compounds = stints.get("compound", pd.Series(dtype=str)).astype("string").str.upper()
    lap_start = pd.to_numeric(
        stints.get("lap_start", pd.Series(dtype=float)), errors="coerce"
    )
    lap_end = pd.to_numeric(
        stints.get("lap_end", pd.Series(dtype=float)), errors="coerce"
    )
    wet_tyres = (
        compounds.isin(("INTERMEDIATE", "WET"))
        & lap_start.gt(0)
        & lap_end.ge(lap_start)
    )
    messages = race_control.get("message", pd.Series(dtype=str)).astype("string").str.strip()
    wet_messages = messages.str.match(
        r"(?:TRACK (?:IS |HAS BEEN )?(?:DECLARED )?WET\b|"
        r"WET (?:TRACK|RACE|SESSION)\b|"
        r"(?:LIGHT |HEAVY )?RAIN(?:FALL)? (?:DETECTED|REPORTED|IN SECTOR|AT TURN)\b)",
        case=False,
        na=False,
    )
    wet = rainfall.gt(0).any() or wet_tyres.any() or wet_messages.any()
    condition = "Wet" if wet else "Dry"
    return condition, float(temperatures.mean()) if not temperatures.empty else None


def _best_result(results: pd.DataFrame) -> str:
    positions = pd.to_numeric(
        results.get("position", pd.Series(dtype=float)), errors="coerce"
    ).dropna()
    if positions.empty:
        return "—"
    best = results.loc[positions.idxmin()]
    parts = [f"P{int(positions.min())}"]
    if pd.notna(best.get("circuit_name")):
        parts.append(str(best["circuit_name"]))
    temperature = best.get("air_temperature")
    if pd.notna(temperature):
        parts.append(f"{temperature:.0f}°C")
    parts.append(str(best["condition"]))
    return " · ".join(parts)


def driver_comparison_metrics(
    race_results: pd.DataFrame,
    qualifying_results: pd.DataFrame,
    driver_number: int,
) -> pd.DataFrame:
    races = race_results[
        pd.to_numeric(race_results["driver_number"], errors="coerce").eq(driver_number)
    ]
    qualifying = qualifying_results[
        pd.to_numeric(
            qualifying_results.get("driver_number", pd.Series(dtype=float)), errors="coerce"
        ).eq(driver_number)
    ]
    columns = {}
    for condition in ("Overall", "Dry", "Wet"):
        race = races if condition == "Overall" else races[races["condition"].eq(condition)]
        quali = (
            qualifying if condition == "Overall" else qualifying[
                qualifying.get("condition", pd.Series(dtype=str)).eq(condition)
            ]
        )
        positions = pd.to_numeric(
            race.get("position", pd.Series(dtype=float)), errors="coerce"
        ).dropna()
        quali_positions = pd.to_numeric(
            quali.get("position", pd.Series(dtype=float)), errors="coerce"
        ).dropna()
        points = pd.to_numeric(
            race.get("points", pd.Series(dtype=float)), errors="coerce"
        )
        dnfs = race.get("dnf", pd.Series(dtype="boolean"))
        race_count = race["session_id"].nunique()
        dnf_count = (
            int(dnfs.sum())
            if race_count and len(dnfs) == len(race) and dnfs.notna().all()
            else None
        )
        points_total = (
            f"{points.sum():g}"
            if race_count and len(points) == len(race) and points.notna().all()
            else "—"
        )
        columns[condition] = [
            str(race_count) if race_count else "—",
            points_total,
            str(int(positions.eq(1).sum())) if not positions.empty else "—",
            str(int(positions.le(3).sum())) if not positions.empty else "—",
            _best_result(race),
            f"{positions.mean():.1f}" if not positions.empty else "—",
            str(dnf_count) if dnf_count is not None else "—",
            f"{dnf_count / race_count:.0%}" if dnf_count is not None else "—",
            str(quali["session_id"].nunique()) if not quali.empty else "—",
            _best_result(quali),
            f"{quali_positions.mean():.1f}" if not quali_positions.empty else "—",
        ]
    comparison = pd.DataFrame(columns, index=COMPARISON_METRICS)
    comparison.loc[
        ["Wins", "Podiums", "Best finish", "Best qualifying"], ["Dry", "Wet"]
    ] = ""
    return comparison
