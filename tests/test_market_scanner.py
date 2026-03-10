import os
import sys
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.market_scanner import OnDemandScanner


def _make_item_detail(
    *,
    item_id: str = "item_id_1",
    base_type: str = "Tabula Rasa",
    ilvl: int = 84,
    price_amount: float = 10.0,
    currency: str = "chaos",
    indexed_at: str | None = None,
    explicit_mods: list[str] | None = None,
    corrupted: bool = False,
    fractured: bool = False,
    influences: dict | None = None,
):
    if indexed_at is None:
        indexed_at = datetime.now(timezone.utc).isoformat()
    if explicit_mods is None:
        explicit_mods = ["Life: +50", "Mana: +20"]
    if influences is None:
        influences = {}

    return {
        "listing": {
            "price": {
                "type": "price",
                "amount": price_amount,
                "currency": currency,
            },
            "account": {
                "name": "SellerAccount",
                "online": True,
            },
            "whisper": "@SellerAccount Hi, I would like to buy your item",
            "indexed": indexed_at,
        },
        "item": {
            "id": item_id,
            "baseType": base_type,
            "ilvl": ilvl,
            "rarity": "rare",
            "explicitMods": explicit_mods,
            "implicitMods": [],
            "corrupted": corrupted,
            "fractured": fractured,
            "influences": influences,
        },
    }


class TestExtractPriceChaos:
    @pytest.fixture
    def scanner_with_mock_rates(self, mock_ninja_currency_response):
        with patch("core.market_scanner.MarketAPIClient"):
            scanner = OnDemandScanner(league="Standard")
            scanner.currency_rates = mock_ninja_currency_response
            return scanner

    def test_extract_price_chaos_basic(self, scanner_with_mock_rates):
        listing = {"price": {"amount": 10.0, "currency": "chaos"}}
        assert scanner_with_mock_rates.extract_price_chaos(listing) == 10.0

    def test_extract_price_chaos_divine(self, scanner_with_mock_rates):
        listing = {"price": {"amount": 1.0, "currency": "divine"}}
        assert scanner_with_mock_rates.extract_price_chaos(listing) == 150.0

    def test_extract_price_chaos_exalted(self, scanner_with_mock_rates):
        listing = {"price": {"amount": 5.0, "currency": "exalted"}}
        assert scanner_with_mock_rates.extract_price_chaos(listing) == 425.0

    def test_extract_price_chaos_invalid(self, scanner_with_mock_rates):
        listing = {"price": {"amount": -5.0, "currency": "chaos"}}
        assert scanner_with_mock_rates.extract_price_chaos(listing) is None

    def test_extract_price_chaos_unknown_currency(self, scanner_with_mock_rates):
        listing = {"price": {"amount": 10.0, "currency": "unknown_currency"}}
        assert scanner_with_mock_rates.extract_price_chaos(listing) is None


class TestAntiFixFilter:
    @pytest.fixture
    def scanner_with_mock_rates(self, mock_ninja_currency_response):
        with patch("core.market_scanner.MarketAPIClient"):
            scanner = OnDemandScanner(league="Standard")
            scanner.currency_rates = mock_ninja_currency_response
            return scanner

    def test_anti_fix_filter(self, scanner_with_mock_rates):
        old_indexed = (datetime.now(timezone.utc) - timedelta(hours=60)).isoformat()
        result = scanner_with_mock_rates._is_probable_price_fix(
            listed_price_chaos=1.5,
            ml_value=20.0,
            indexed_at=old_indexed,
            stale_hours=48.0,
        )
        assert result is True

    def test_anti_fix_filter_not_stale(self, scanner_with_mock_rates):
        recent_indexed = (datetime.now(timezone.utc) - timedelta(hours=10)).isoformat()
        result = scanner_with_mock_rates._is_probable_price_fix(
            listed_price_chaos=1.5,
            ml_value=20.0,
            indexed_at=recent_indexed,
            stale_hours=48.0,
        )
        assert result is False


class TestOpportunityScoring:
    @pytest.fixture
    def scanner_with_mock_rates(self, mock_ninja_currency_response):
        with patch("core.market_scanner.MarketAPIClient"):
            scanner = OnDemandScanner(league="Standard")
            scanner.currency_rates = mock_ninja_currency_response
            scanner.api_client.league = "Standard"
            return scanner

    def test_score_penalizes_risk_flags(self, scanner_with_mock_rates):
        clean_score, clean_trusted_profit = scanner_with_mock_rates._compute_opportunity_score(
            listed_price=40.0,
            ml_value=120.0,
            ml_confidence=0.8,
            risk_flags=[],
        )
        risky_score, risky_trusted_profit = scanner_with_mock_rates._compute_opportunity_score(
            listed_price=40.0,
            ml_value=120.0,
            ml_confidence=0.8,
            risk_flags=["price_fix_suspected", "low_confidence"],
        )
        assert clean_trusted_profit == risky_trusted_profit
        assert clean_score > risky_score

    def test_build_opportunity_includes_score_flags_and_trusted_profit(
        self, scanner_with_mock_rates, mock_item_detail
    ):
        scanner_with_mock_rates.oracle.predict_value = MagicMock(return_value=(60.0, 0.45))
        opportunity = scanner_with_mock_rates._build_opportunity(
            mock_item_detail,
            query_id="abc123",
            stale_hours=48.0,
        )
        assert opportunity is not None
        assert opportunity.score >= 0
        assert opportunity.trusted_profit == 22.5
        assert "low_confidence" in opportunity.risk_flags
        assert opportunity.resolved_league == "Standard"


class TestScanProfiles:
    @pytest.fixture
    def scanner_with_mock_rates(self, mock_ninja_currency_response):
        with patch("core.market_scanner.MarketAPIClient"):
            scanner = OnDemandScanner(league="Standard")
            scanner.currency_rates = mock_ninja_currency_response
            scanner.api_client.league = "Standard"
            scanner.api_client.search_items = MagicMock(return_value=("abc123", ["id1", "id2", "id3"]))
            return scanner

    def test_open_market_filters_low_confidence(self, scanner_with_mock_rates):
        detail = _make_item_detail(price_amount=7.0)
        scanner_with_mock_rates.api_client.fetch_item_details = MagicMock(return_value=[detail])
        scanner_with_mock_rates.oracle.predict_value = MagicMock(return_value=(40.0, 0.40))

        opportunities, stats = scanner_with_mock_rates.scan_opportunities(item_class="", max_items=1)

        assert opportunities == []
        assert stats.scan_profile == "open_market"
        assert stats.filtered_open_confidence == 1

    def test_open_market_filters_cheap_low_confidence(self, scanner_with_mock_rates):
        detail = _make_item_detail(price_amount=2.0)
        scanner_with_mock_rates.api_client.fetch_item_details = MagicMock(return_value=[detail])
        scanner_with_mock_rates.oracle.predict_value = MagicMock(return_value=(35.0, 0.55))

        opportunities, stats = scanner_with_mock_rates.scan_opportunities(item_class="", max_items=1)

        assert opportunities == []
        assert stats.filtered_open_cheap_low_confidence == 1

    def test_open_market_filters_cheap_low_profit(self, scanner_with_mock_rates):
        detail = _make_item_detail(price_amount=4.0)
        scanner_with_mock_rates.api_client.fetch_item_details = MagicMock(return_value=[detail])
        scanner_with_mock_rates.oracle.predict_value = MagicMock(return_value=(18.0, 0.75))

        opportunities, stats = scanner_with_mock_rates.scan_opportunities(item_class="", max_items=1)

        assert opportunities == []
        assert stats.filtered_open_cheap_low_profit == 1

    def test_open_market_filters_cheap_stale(self, scanner_with_mock_rates):
        indexed_at = (datetime.now(timezone.utc) - timedelta(hours=18)).isoformat()
        detail = _make_item_detail(price_amount=2.0, indexed_at=indexed_at)
        scanner_with_mock_rates.api_client.fetch_item_details = MagicMock(return_value=[detail])
        scanner_with_mock_rates.oracle.predict_value = MagicMock(return_value=(30.0, 0.75))

        opportunities, stats = scanner_with_mock_rates.scan_opportunities(item_class="", max_items=1)

        assert opportunities == []
        assert stats.filtered_open_cheap_stale == 1

    def test_open_market_allows_cheap_recent_high_confidence_item(self, scanner_with_mock_rates):
        indexed_at = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
        detail = _make_item_detail(price_amount=2.0, indexed_at=indexed_at)
        scanner_with_mock_rates.api_client.fetch_item_details = MagicMock(return_value=[detail])
        scanner_with_mock_rates.oracle.predict_value = MagicMock(return_value=(30.0, 0.75))

        opportunities, stats = scanner_with_mock_rates.scan_opportunities(item_class="", max_items=1)

        assert len(opportunities) == 1
        assert opportunities[0].trusted_profit == 21.0
        assert stats.scan_profile == "open_market"

    def test_targeted_scan_keeps_item_rejected_by_open_market(self, scanner_with_mock_rates):
        detail = _make_item_detail(base_type="Imbued Wand", price_amount=2.0)
        scanner_with_mock_rates.api_client.fetch_item_details = MagicMock(return_value=[detail])
        scanner_with_mock_rates.oracle.predict_value = MagicMock(return_value=(35.0, 0.55))

        opportunities, stats = scanner_with_mock_rates.scan_opportunities(
            item_class="Imbued Wand",
            max_items=1,
        )

        assert len(opportunities) == 1
        assert stats.scan_profile == "targeted"
        assert stats.filtered_open_cheap_low_confidence == 0

    def test_ranking_prefers_trusted_profit_and_confidence(self, scanner_with_mock_rates):
        details = [
            _make_item_detail(item_id="cheap", base_type="Cobalt Jewel", price_amount=2.0),
            _make_item_detail(item_id="stable", base_type="Sadist Garb", price_amount=20.0),
        ]
        scanner_with_mock_rates.api_client.search_items = MagicMock(return_value=("abc123", ["cheap", "stable"]))
        scanner_with_mock_rates.api_client.fetch_item_details = MagicMock(return_value=details)
        scanner_with_mock_rates.oracle.predict_value = MagicMock(side_effect=[(55.0, 0.60), (56.0, 0.92)])

        opportunities, _ = scanner_with_mock_rates.scan_opportunities(item_class="", max_items=2)

        assert len(opportunities) == 2
        assert opportunities[0].item_id == "stable"
        assert opportunities[0].trusted_profit >= opportunities[1].trusted_profit - 1.0
