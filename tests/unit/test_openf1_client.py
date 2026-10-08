from __future__ import annotations

import unittest
from typing import Any

import requests

from f1_pipeline.sources.openf1 import OpenF1Client, OpenF1Error


class FakeResponse:
    def __init__(self, status_code: int, payload: Any = None) -> None:
        self.status_code = status_code
        self._payload = payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(f"{self.status_code} error")

    def json(self) -> Any:
        return self._payload


class FakeSession:
    def __init__(self, response: FakeResponse) -> None:
        self.response = response
        self.calls: list[dict[str, Any]] = []

    def get(self, url: str, *, params: dict[str, Any], timeout: float) -> FakeResponse:
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        return self.response

    def mount(self, *args: Any, **kwargs: Any) -> None:
        pass

    headers: dict[str, str] = {}


class OpenF1ClientTest(unittest.TestCase):
    def test_http_response_rules(self) -> None:
        payload = [{"session_key": 42, "driver_number": 1}]
        for status, endpoint, allow_empty, expected in (
            (404, "starting_grid", True, []),
            (404, "starting_grid", False, None),
            (500, "laps", True, None),
            (200, "laps", True, payload),
        ):
            with self.subTest(status=status, allow_empty=allow_empty):
                client = OpenF1Client()
                client.session = FakeSession(FakeResponse(status, payload))
                if expected is None:
                    with self.assertRaises(OpenF1Error):
                        client.get_json(
                            endpoint, {"session_key": 42},
                            treat_404_as_empty=allow_empty,
                        )
                else:
                    self.assertEqual(
                        client.get_json(
                            endpoint, {"session_key": 42},
                            treat_404_as_empty=allow_empty,
                        ),
                        expected,
                    )


if __name__ == "__main__":
    unittest.main()
