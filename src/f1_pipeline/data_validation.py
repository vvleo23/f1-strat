"""Runtime validation helpers for persisted pipeline tables."""

from __future__ import annotations

from collections.abc import Collection, Sequence

import pandas as pd


class DataValidationError(ValueError):
    """Describe a schema or content validation failure."""


def validate_frame(
    frame: pd.DataFrame,
    *,
    name: str,
    required_columns: Collection[str],
    key_columns: Sequence[str] = (),
    datetime_columns: Collection[str] = (),
    numeric_columns: Collection[str] = (),
    required_non_null: Collection[str] = (),
    expected_session_key: int | None = None,
    allow_empty: bool = False,
) -> None:
    """Validate a source-shaped or curated dataframe without filling missing data."""
    if not isinstance(frame, pd.DataFrame):
        raise DataValidationError(f"{name} is not a pandas DataFrame.")

    missing_columns = sorted(set(required_columns).difference(frame.columns))
    if missing_columns:
        raise DataValidationError(
            f"{name} is missing required columns: {', '.join(missing_columns)}."
        )
    if frame.empty:
        if allow_empty:
            return
        raise DataValidationError(f"{name} is empty.")

    for column in required_non_null:
        values = frame[column]
        missing = values.isna()
        if values.dtype == "object":
            missing |= values.astype(str).str.strip().eq("")
        if missing.any():
            raise DataValidationError(f"{name}.{column} contains missing values.")

    if expected_session_key is not None:
        values = pd.to_numeric(frame["session_key"], errors="coerce")
        if values.isna().any() or not values.eq(expected_session_key).all():
            raise DataValidationError(
                f"{name}.session_key does not consistently equal {expected_session_key}."
            )

    for column in datetime_columns:
        values = frame[column]
        parsed = pd.to_datetime(values, utc=True, errors="coerce", format="mixed")
        invalid = values.notna() & parsed.isna()
        if invalid.any():
            raise DataValidationError(f"{name}.{column} contains invalid UTC timestamps.")

    for column in numeric_columns:
        values = frame[column]
        parsed = pd.to_numeric(values, errors="coerce")
        invalid = values.notna() & parsed.isna()
        if invalid.any():
            raise DataValidationError(f"{name}.{column} contains non-numeric values.")

    if key_columns:
        duplicate_count = int(frame.duplicated(list(key_columns)).sum())
        if duplicate_count:
            key = ", ".join(key_columns)
            raise DataValidationError(
                f"{name} contains {duplicate_count} duplicate rows for key ({key})."
            )



