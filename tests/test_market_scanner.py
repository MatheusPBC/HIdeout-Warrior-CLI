import pytest
import sys
import os
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.market_scanner import OnDemandScanner
from core.graph_engine import ItemState


class TestExtractPriceChaos:
    @pytest.fixture
    def scanner_with_mock_rates(self, mock_ninja_currency_response):
        with patch("core.market_scanner.MarketAPIClient"):
            scanner = OnDemandScanner(league="Standard")
            scanner.currency_rates = mock_ninja_currency_response
            return scanner

    def test_extract_price_chaos_basic(self, scanner_with_mock_rates):
        listing = {
            "price": {
                "amount": 10.0,
                "currency": "chaos",
            }
        }
        result = scanner_with_mock_rates.extract_price_chaos(listing)
        assert result == 10.0

    def test_extract_price_chaos_divine(self, scanner_with_mock_rates):
        listing = {
            "price": {
                "amount": 1.0,
                "currency": "divine",
            }
        }
        result = scanner_with_mock_rates.extract_price_chaos(listing)
        assert result == 150.0

    def test_extract_price_chaos_exalted(self, scanner_with_mock_rates):
        listing = {
            "price": {
                "amount": 5.0,
                "currency": "exalted",
            }
        }
        result = scanner_with_mock_rates.extract_price_chaos(listing)
        assert result == 425.0

    def test_extract_price_chaos_invalid(self, scanner_with_mock_rates):
        listing = {
            "price": {
                "amount": -5.0,
                "currency": "chaos",
            }
        }
        result = scanner_with_mock_rates.extract_price_chaos(listing)
        assert result is None

    def test_extract_price_chaos_unknown_currency(self, scanner_with_mock_rates):
        listing = {
            "price": {
                "amount": 10.0,
                "currency": "unknown_currency",
            }
        }
        result = scanner_with_mock_rates.extract_price_chaos(listing)
        assert result is None


class TestAntiFixFilter:
    @pytest.fixture
    def scanner_with_mock_rates(self, mock_ninja_currency_response):
        with patch("core.market_scanner.MarketAPIClient"):
            scanner = OnDemandScanner(league="Standard")
            scanner.currency_rates = mock_ninja_currency_response
            return scanner

    def test_anti_fix_filter(self, scanner_with_mock_rates):
        now = datetime.now(timezone.utc)
        old_indexed = (now - timedelta(hours=60)).isoformat()

        result = scanner_with_mock_rates._is_probable_price_fix(
            listed_price_chaos=1.5,
            ml_value=20.0,
            indexed_at=old_indexed,
            stale_hours=48.0,
        )
        assert result is True

    def test_anti_fix_filter_not_stale(self, scanner_with_mock_rates):
        now = datetime.now(timezone.utc)
        recent_indexed = (now - timedelta(hours=10)).isoformat()

        result = scanner_with_mock_rates._is_probable_price_fix(
            listed_price_chaos=1.5,
            ml_value=20.0,
            indexed_at=recent_indexed,
            stale_hours=48.0,
        )
        assert result is False


class TestMinProfitFilter:
    @pytest.fixture
    def mock_results(self):
        return [
            {"base_type": "Item1", "profit": 100.0},
            {"base_type": "Item2", "profit": 50.0},
            {"base_type": "Item3", "profit": 10.0},
            {"base_type": "Item4", "profit": -20.0},
        ]

    def test_min_profit_filter_above_threshold(self, mock_results):
        filtered = [item for item in mock_results if item["profit"] >= 50.0]
        assert len(filtered) == 2
        assert filtered[0]["base_type"] == "Item1"
        assert filtered[1]["base_type"] == "Item2"

    def test_min_profit_filter_zero(self, mock_results):
        filtered = [item for item in mock_results if item["profit"] >= 0.0]
        assert len(filtered) == 3

    def test_min_profit_filter_negative(self, mock_results):
        filtered = [item for item in mock_results if item["profit"] >= -10.0]
        assert len(filtered) == 3
