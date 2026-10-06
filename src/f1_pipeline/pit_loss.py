from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from f1_pipeline.settings import PROJECT_ROOT

DEFAULT_PIT_LOSS_PATH = PROJECT_ROOT / "config" / "pit_loss_seconds.json"


class PitLossConfigError(RuntimeError):
    pass


@dataclass(frozen=True)
class PitLoss:
    key: str
    green_seconds: float
    vsc_seconds: float
    sc_seconds: float
    source_asset: str
    neutralized_basis: str

    @property
    def seconds(self) -> float:
        return self.green_seconds

    def for_status(self, status: str) -> float:
        normalized = str(status).strip().upper()
        if normalized == "SC":
            return self.sc_seconds
        if normalized == "VSC":
            return self.vsc_seconds
        return self.green_seconds


def _normalized(value: Any) -> str:
    return " ".join(str(value or "").strip().casefold().split())


def load_pit_loss(
    *identifiers: Any,
    path: Path = DEFAULT_PIT_LOSS_PATH,
) -> PitLoss | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PitLossConfigError(f"Could not read pit-loss configuration: {exc}") from exc
    if payload.get("schema_version") not in {1, 2} or payload.get("unit") != "seconds":
        raise PitLossConfigError("Pit-loss configuration has an unsupported schema.")
    factors = payload.get("neutralized_factors", {})
    try:
        vsc_factor = float(factors.get("vsc", 0.60))
        sc_factor = float(factors.get("sc", 0.50))
    except (TypeError, ValueError) as exc:
        raise PitLossConfigError("Pit-loss configuration has invalid neutralized factors.") from exc
    if not all(math.isfinite(value) and 0 < value <= 1 for value in (vsc_factor, sc_factor)):
        raise PitLossConfigError("Neutralized pit-loss factors must be within (0, 1].")
    circuits = payload.get("circuits")
    if not isinstance(circuits, list):
        raise PitLossConfigError("Pit-loss configuration has no circuit list.")
    requested = {_normalized(value) for value in identifiers if _normalized(value)}
    matches: list[PitLoss] = []
    seen_aliases: set[str] = set()
    for record in circuits:
        if not isinstance(record, dict):
            raise PitLossConfigError("Pit-loss configuration contains an invalid row.")
        aliases = {_normalized(value) for value in record.get("aliases", [])}
        aliases.discard("")
        if not aliases or aliases.intersection(seen_aliases):
            raise PitLossConfigError("Pit-loss aliases must be present and unique.")
        seen_aliases.update(aliases)
        try:
            green = float(record["average_pit_loss"])
        except (KeyError, TypeError, ValueError) as exc:
            raise PitLossConfigError("Pit-loss configuration contains an invalid value.") from exc
        if not math.isfinite(green) or green <= 0:
            raise PitLossConfigError("Pit-loss values must be positive finite seconds.")
        if requested.intersection(aliases):
            matches.append(
                PitLoss(
                    key=str(record.get("key") or ""),
                    green_seconds=green,
                    vsc_seconds=green * vsc_factor,
                    sc_seconds=green * sc_factor,
                    source_asset=str(payload.get("source_asset") or ""),
                    neutralized_basis=str(
                        factors.get("basis")
                        or "Versioned project assumption derived from green-flag loss."
                    ),
                )
            )
    if len(matches) > 1:
        raise PitLossConfigError("Meeting identifiers match multiple pit-loss rows.")
    return matches[0] if matches else None
