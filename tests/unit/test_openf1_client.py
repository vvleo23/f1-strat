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
    def test_treat_404_as_empty_returns_empty_list_without_raising(self) -> None:
        client = OpenF1Client()
        client.session = FakeSession(FakeResponse(404, {"detail": "No results found."}))

        result = client.get_json(
            "starting_grid", {"session_key": 42}, treat_404_as_empty=True
        )

        self.assertEqual(result, [])

    def test_404_without_opt_in_still_raises(self) -> None:
        client = OpenF1Client()
        client.session = FakeSession(FakeResponse(404, {"detail": "No results found."}))

        with self.assertRaises(OpenF1Error):
            client.get_json("starting_grid", {"session_key": 42})

    def test_other_error_status_still_raises_even_with_opt_in(self) -> None:
        client = OpenF1Client()
        client.session = FakeSession(FakeResponse(500))

        with self.assertRaises(OpenF1Error):
            client.get_json("laps", {"session_key": 42}, treat_404_as_empty=True)

    def test_successful_response_is_unaffected_by_the_new_parameter(self) -> None:
        client = OpenF1Client()
        payload = [{"session_key": 42, "driver_number": 1}]
        client.session = FakeSession(FakeResponse(200, payload))

        result = client.get_json("laps", {"session_key": 42}, treat_404_as_empty=True)

        self.assertEqual(result, payload)


if __name__ == "__main__":
    unittest.main()
