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
    seconds: float
    source_asset: str


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
    if payload.get("schema_version") != 1 or payload.get("unit") != "seconds":
        raise PitLossConfigError("Pit-loss configuration has an unsupported schema.")
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
            seconds = float(record["average_pit_loss"])
        except (KeyError, TypeError, ValueError) as exc:
            raise PitLossConfigError("Pit-loss configuration contains an invalid value.") from exc
        if not math.isfinite(seconds) or seconds <= 0:
            raise PitLossConfigError("Pit-loss values must be positive finite seconds.")
        if requested.intersection(aliases):
            matches.append(
                PitLoss(
                    key=str(record.get("key") or ""),
                    seconds=seconds,
                    source_asset=str(payload.get("source_asset") or ""),
                )
            )
    if len(matches) > 1:
        raise PitLossConfigError("Meeting identifiers match multiple pit-loss rows.")
    return matches[0] if matches else None
