import pytest
import sys
import os
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.market_scanner import ListingSnapshot, OnDemandScanner
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


class TestOpportunityScoring:
    @pytest.fixture
    def scanner_with_mock_rates(self, mock_ninja_currency_response):
        with patch("core.market_scanner.MarketAPIClient"):
            scanner = OnDemandScanner(league="Standard")
            scanner.currency_rates = mock_ninja_currency_response
            scanner.api_client.league = "Standard"
            return scanner

    def test_score_penalizes_risk_flags(self, scanner_with_mock_rates):
        clean_score = scanner_with_mock_rates._compute_opportunity_score(
            listed_price=40.0,
            ml_value=120.0,
            ml_confidence=0.8,
            risk_flags=[],
        )
        risky_score = scanner_with_mock_rates._compute_opportunity_score(
            listed_price=40.0,
            ml_value=120.0,
            ml_confidence=0.8,
            risk_flags=["price_fix_suspected", "low_confidence"],
        )
        assert clean_score > risky_score

    def test_risk_flags_include_high_ticket_low_confidence(
        self, scanner_with_mock_rates
    ):
        snapshot = ListingSnapshot(
            item_id="item-1",
            base_type="Imbued Wand",
            ilvl=84,
            listed_price=100.0,
            listing_currency="chaos",
            listing_amount=100.0,
            seller="seller",
            indexed_at=None,
            whisper="@seller hi",
            trade_link="https://example.com/trade#item-1",
            trade_search_link="https://example.com/trade",
            corrupted=False,
            fractured=False,
        )

        flags = scanner_with_mock_rates._risk_flags(
            snapshot=snapshot,
            ml_value=130.0,
            ml_confidence=0.7,
            stale_hours=48.0,
        )

        assert "high_ticket_low_confidence" in flags

    def test_score_penalizes_high_ticket_low_confidence_flag(
        self, scanner_with_mock_rates
    ):
        baseline_score = scanner_with_mock_rates._compute_opportunity_score(
            listed_price=90.0,
            ml_value=140.0,
            ml_confidence=0.78,
            risk_flags=[],
        )
        penalized_score = scanner_with_mock_rates._compute_opportunity_score(
            listed_price=90.0,
            ml_value=140.0,
            ml_confidence=0.78,
            risk_flags=["high_ticket_low_confidence"],
        )

        assert penalized_score < baseline_score

    def test_build_opportunity_includes_score_and_flags(
        self, scanner_with_mock_rates, mock_item_detail
    ):
        scanner_with_mock_rates.oracle.predict_value = MagicMock(
            return_value=(60.0, 0.45)
        )
        opportunity = scanner_with_mock_rates._build_opportunity(
            mock_item_detail,
            query_id="abc123",
            stale_hours=48.0,
        )
        assert opportunity is not None
        assert opportunity.score >= 0
        assert "low_confidence" in opportunity.risk_flags
        assert opportunity.resolved_league == "Standard"


class TestMinListedPriceFilter:
    @patch("core.market_scanner.MarketAPIClient")
    @patch("core.market_scanner.PricePredictor")
    def test_scan_opportunities_filters_by_min_listed_price(
        self,
        mock_oracle_cls,
        mock_client_cls,
    ):
        mock_client = mock_client_cls.return_value
        mock_client.league = "Standard"
        mock_client.sync_ninja_economy.return_value = {"Chaos Orb": 1.0}
        mock_client.search_items.return_value = ("query123", ["a", "b"])
        mock_client.fetch_item_details.return_value = [
            {
                "id": "result_low",
                "listing": {
                    "whisper": "@seller hi",
                    "price": {"amount": 5.0, "currency": "chaos"},
                    "account": {"name": "seller1"},
                    "indexed": "2026-03-10T09:00:00Z",
                },
                "item": {
                    "id": "item_low",
                    "baseType": "Driftwood Wand",
                    "ilvl": 80,
                    "explicitMods": ["+# to maximum Life"],
                    "implicitMods": [],
                    "corrupted": False,
                    "fractured": False,
                    "influences": {},
                },
            },
            {
                "id": "result_high",
                "listing": {
                    "whisper": "@seller hi",
                    "price": {"amount": 60.0, "currency": "chaos"},
                    "account": {"name": "seller2"},
                    "indexed": "2026-03-10T09:00:00Z",
                },
                "item": {
                    "id": "item_high",
                    "baseType": "Imbued Wand",
                    "ilvl": 84,
                    "explicitMods": ["+#% increased Spell Damage"],
                    "implicitMods": [],
                    "corrupted": False,
                    "fractured": False,
                    "influences": {},
                },
            },
        ]

        mock_oracle = mock_oracle_cls.return_value
        mock_oracle.predict_value.return_value = (120.0, 0.8)

        scanner = OnDemandScanner(league="Standard")
        opportunities, stats = scanner.scan_opportunities(
            max_items=2,
            min_listed_price=50.0,
            anti_fix=False,
        )

        assert len(opportunities) == 1
        assert opportunities[0].item_id == "item_high"
        assert opportunities[0].listed_price == 60.0
        assert stats.filtered_min_listed_price == 1


class TestSafeBuyDynamicConfidence:
    @patch("core.market_scanner.MarketAPIClient")
    @patch("core.market_scanner.PricePredictor")
    def test_safe_buy_uses_dynamic_confidence_threshold_by_price(
        self,
        mock_oracle_cls,
        mock_client_cls,
    ):
        mock_client = mock_client_cls.return_value
        mock_client.league = "Standard"
        mock_client.sync_ninja_economy.return_value = {"Chaos Orb": 1.0}
        mock_client.search_items.return_value = ("query123", ["a", "b", "c"])
        mock_client.fetch_item_details.return_value = [
            {
                "id": "result_low_price",
                "listing": {
                    "whisper": "@seller hi",
                    "price": {"amount": 40.0, "currency": "chaos"},
                    "account": {"name": "seller1"},
                    "indexed": "2026-03-10T09:00:00Z",
                },
                "item": {
                    "id": "item_low_price",
                    "baseType": "Driftwood Wand",
                    "ilvl": 80,
                    "explicitMods": ["+# to maximum Life"],
                    "implicitMods": [],
                    "corrupted": False,
                    "fractured": False,
                    "influences": {},
                },
            },
            {
                "id": "result_mid_price",
                "listing": {
                    "whisper": "@seller hi",
                    "price": {"amount": 60.0, "currency": "chaos"},
                    "account": {"name": "seller2"},
                    "indexed": "2026-03-10T09:00:00Z",
                },
                "item": {
                    "id": "item_mid_price",
                    "baseType": "Imbued Wand",
                    "ilvl": 84,
                    "explicitMods": ["+#% increased Spell Damage"],
                    "implicitMods": [],
                    "corrupted": False,
                    "fractured": False,
                    "influences": {},
                },
            },
            {
                "id": "result_high_price",
                "listing": {
                    "whisper": "@seller hi",
                    "price": {"amount": 130.0, "currency": "chaos"},
                    "account": {"name": "seller3"},
                    "indexed": "2026-03-10T09:00:00Z",
                },
                "item": {
                    "id": "item_high_price",
                    "baseType": "Opal Ring",
                    "ilvl": 84,
                    "explicitMods": ["+# to all Elemental Resistances"],
                    "implicitMods": [],
                    "corrupted": False,
                    "fractured": False,
                    "influences": {},
                },
            },
        ]

        mock_oracle = mock_oracle_cls.return_value
        mock_oracle.predict_value.side_effect = [
            (80.0, 0.75),
            (100.0, 0.79),
            (200.0, 0.81),
        ]

        scanner = OnDemandScanner(league="Standard")
        opportunities, stats = scanner.scan_opportunities(
            max_items=3,
            anti_fix=False,
            safe_buy=True,
        )

        returned_ids = {opportunity.item_id for opportunity in opportunities}
        assert returned_ids == {"item_low_price", "item_mid_price"}
        assert stats.filtered_safe_buy_confidence == 1
