from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from f1_pipeline.persistence import atomic_json
from f1_pipeline.sources.wikidata import (
    CircuitResolutionError,
    resolve_circuit_reference,
)


class FakeWikidataClient:
    def __init__(self, candidates: list[str], entities: dict[str, dict[str, Any]]) -> None:
        self.candidates = candidates
        self.entities = entities
        self.search_calls = 0
        self.entity_calls = 0

    def search_entities(self, query: str) -> dict[str, Any]:
        self.search_calls += 1
        return {
            "search": [
                {"id": entity_id, "label": "candidate", "description": "candidate"}
                for entity_id in self.candidates
            ]
        }

    def get_entity(self, entity_id: str) -> dict[str, Any]:
        self.entity_calls += 1
        return {"entities": {entity_id: self.entities[entity_id]}}


def entity(
        entity_id: str,
        *,
        label: str = "Circuit de Spa-Francorchamps",
        description: str = "motor-racing circuit in Stavelot, Belgium",
        coordinates: bool = True,
) -> dict[str, Any]:
    claims = {}
    if coordinates:
        claims["P625"] = [
            {
                "rank": "normal",
                "mainsnak": {
                    "snaktype": "value",
                    "datavalue": {
                        "value": {
                            "latitude": 50.4372,
                            "longitude": 5.9714,
                            "globe": "http://www.wikidata.org/entity/Q2",
                        }
                    },
                },
            }
        ]
    return {
        "id": entity_id,
        "lastrevid": 12345,
        "labels": {"en": {"value": label}},
        "descriptions": {"en": {"value": description}},
        "claims": claims,
    }


class WikidataAutoResolutionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name)
        self.mapping_path = root / "reviewed.json"
        self.auto_mapping_path = root / "auto.json"
        self.raw_dir = root / "raw"
        atomic_json(
            {
                "schema_version": 1,
                "mappings": [
                    {
                        "source_system": "openf1",
                        "source_circuit_key": 4,
                        "wikidata_entity_id": "Q171356",
                        "expected_label": "Hungaroring",
                        "expected_country": "Hungary",
                        "review_status": "reviewed",
                    }
                ],
            },
            self.mapping_path,
        )
        self.retrieved_at = datetime(2026, 8, 28, tzinfo=timezone.utc)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def resolve(self, client: FakeWikidataClient):
        return resolve_circuit_reference(
            7,
            "Spa-Francorchamps",
            "Belgium",
            client=client,
            retrieved_at=self.retrieved_at,
            mapping_path=self.mapping_path,
            auto_mapping_path=self.auto_mapping_path,
            raw_dir=self.raw_dir,
        )

    def test_unique_verified_candidate_is_persisted_and_reused(self) -> None:
        client = FakeWikidataClient(["Q172179"], {"Q172179": entity("Q172179")})

        reference, result = self.resolve(client)
        cached_reference, cached_result = self.resolve(
            FakeWikidataClient([], {})
        )

        self.assertEqual(reference.wikidata_entity_id, "Q172179")
        self.assertEqual(reference.verification_status, "auto_verified")
        self.assertEqual(result["resolution"], "auto_verified")
        self.assertEqual(cached_reference, reference)
        self.assertEqual(cached_result["resolution"], "auto_registry")
        self.assertEqual(cached_result["status"], "stale")
        with self.auto_mapping_path.open(encoding="utf-8") as stream:
            registry = json.load(stream)
        self.assertEqual(len(registry["mappings"]), 1)
        self.assertEqual(registry["mappings"][0]["source_circuit_key"], 7)
        Path(registry["mappings"][0]["entity_raw_path"]).write_text(
            "{}", encoding="utf-8"
        )
        with self.assertRaises(CircuitResolutionError):
            self.resolve(FakeWikidataClient([], {}))

    def test_ambiguous_verified_candidates_remain_partial(self) -> None:
        client = FakeWikidataClient(
            ["Q1", "Q2"],
            {"Q1": entity("Q1"), "Q2": entity("Q2")},
        )

        with self.assertRaises(CircuitResolutionError) as raised:
            self.resolve(client)

        self.assertEqual(raised.exception.result["status"], "partial")
        self.assertEqual(
            raised.exception.result["validated_candidates"], ["Q1", "Q2"]
        )
        self.assertFalse(self.auto_mapping_path.exists())

    def test_wrong_country_and_missing_coordinate_are_rejected(self) -> None:
        invalid_entities = {
            "Q1": entity("Q1", description="motor-racing circuit in France"),
            "Q2": entity("Q2", coordinates=False),
        }

        with self.assertRaises(CircuitResolutionError) as raised:
            self.resolve(FakeWikidataClient(["Q1", "Q2"], invalid_entities))

        result = raised.exception.result
        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["validated_candidates"], [])
        self.assertEqual(len(result["rejected_candidates"]), 2)
        self.assertFalse(self.auto_mapping_path.exists())

    def test_accepts_known_country_abbreviation_with_complete_circuit_evidence(self) -> None:
        client = FakeWikidataClient(
            ["Q171402"],
            {
                "Q171402": entity(
                    "Q171402",
                    label="Silverstone Circuit",
                    description="motor racing circuit in the UK",
                )
            },
        )

        reference, _ = resolve_circuit_reference(
            2,
            "Silverstone",
            "United Kingdom",
            location="Silverstone",
            client=client,
            retrieved_at=self.retrieved_at,
            mapping_path=self.mapping_path,
            auto_mapping_path=self.auto_mapping_path,
            raw_dir=self.raw_dir,
        )

        self.assertEqual(reference.wikidata_entity_id, "Q171402")


if __name__ == "__main__":
    unittest.main()
