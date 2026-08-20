from __future__ import annotations

from typing import Any

import requests


class JobServiceError(RuntimeError):
    pass


def submit_job(
    service_url: str,
    payload: dict[str, Any],
    *,
    timeout_seconds: float = 5.0,
) -> dict[str, Any]:
    try:
        response = requests.post(
            f"{service_url.rstrip('/')}/jobs",
            json=payload,
            timeout=timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError) as exc:
        raise JobServiceError(f"The data job service is unavailable: {exc}") from exc
    if not isinstance(payload, dict) or not payload.get("job_id"):
        raise JobServiceError("The data job service returned an invalid response.")
    return payload


def get_job(
    service_url: str,
    identifier: str,
    *,
    timeout_seconds: float = 5.0,
) -> dict[str, Any]:
    try:
        response = requests.get(
            f"{service_url.rstrip('/')}/jobs/{identifier}",
            timeout=timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError) as exc:
        raise JobServiceError(f"Could not read data job status: {exc}") from exc
    if not isinstance(payload, dict):
        raise JobServiceError("The data job service returned an invalid response.")
    return payload


