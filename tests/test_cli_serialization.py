import json
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cli import _scan_results_to_csv, _scan_results_to_json, _scan_results_to_jsonl


class TestSerialization:
    @pytest.fixture
    def sample_results(self):
        return [
            {
                "base_type": "Tabula Rasa",
                "ilvl": 1,
                "listed_price": 10.0,
                "ml_value": 15.0,
                "profit": 5.0,
                "trusted_profit": 4.0,
                "whisper": "@Seller Hi, I want to buy",
                "trade_link": "https://example.com/trade/1",
            },
            {
                "base_type": "Imbued Wand",
                "ilvl": 86,
                "listed_price": 50.0,
                "ml_value": 80.0,
                "profit": 30.0,
                "trusted_profit": 24.0,
                "whisper": "@Seller2 Hi, I want to buy",
                "trade_link": "https://example.com/trade/2",
            },
        ]

    def test_serialization_json(self, sample_results):
        result = _scan_results_to_json(sample_results)

        assert result is not None
        parsed = json.loads(result)
        assert len(parsed) == 2
        assert parsed[0]["base_type"] == "Tabula Rasa"
        assert parsed[0]["trusted_profit"] == 4.0
        assert parsed[1]["base_type"] == "Imbued Wand"

    def test_serialization_csv(self, sample_results):
        result = _scan_results_to_csv(sample_results)

        lines = result.strip().split("\n")
        assert len(lines) == 3
        header = lines[0]
        assert "base_type" in header
        assert "listed_price" in header
        assert "ml_value" in header
        assert "profit" in header
        assert "trusted_profit" in header

    def test_serialization_jsonl(self, sample_results):
        result = _scan_results_to_jsonl(sample_results)

        lines = result.strip().split("\n")
        assert len(lines) == 2

        parsed1 = json.loads(lines[0])
        assert parsed1["base_type"] == "Tabula Rasa"
        assert parsed1["profit"] == 5.0
        assert parsed1["trusted_profit"] == 4.0

        parsed2 = json.loads(lines[1])
        assert parsed2["base_type"] == "Imbued Wand"
        assert parsed2["profit"] == 30.0
        assert parsed2["trusted_profit"] == 24.0

    def test_serialization_empty_results(self):
        result_json = _scan_results_to_json([])
        assert result_json == "[]"

        result_csv = _scan_results_to_csv([])
        assert result_csv == ""

        result_jsonl = _scan_results_to_jsonl([])
        assert result_jsonl == ""
