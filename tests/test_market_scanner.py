import os
import sys
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.market_scanner import OnDemandScanner
from core.ml_oracle import ValuationResult


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
        explicit_mods = ["+# to maximum Life", "+#% to Fire Resistance"]
    if influences is None:
        influences = {}

    return {
        "listing": {
            "price": {
                "type": "price",
                "amount": price_amount,
                "currency": currency,
            },
            "account": {"name": "SellerAccount", "online": True},
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


class TestOpportunityScoring:
    @pytest.fixture
    def scanner_with_mock_rates(self, mock_ninja_currency_response):
        with patch("core.market_scanner.MarketAPIClient"):
            scanner = OnDemandScanner(league="Standard")
            scanner.currency_rates = mock_ninja_currency_response
            scanner.api_client.league = "Standard"
            return scanner

    def test_build_opportunity_includes_family_valuation_and_market_fields(self, scanner_with_mock_rates):
        item = _make_item_detail(base_type="Imbued Wand", explicit_mods=["+#% increased Spell Damage", "+#% increased Cast Speed"])
        scanner_with_mock_rates.oracle.predict = MagicMock(
            return_value=ValuationResult(
                predicted_value=80.0,
                confidence=0.74,
                item_family="wand_caster",
                model_source="family_fallback",
                feature_completeness=0.67,
            )
        )

        built = scanner_with_mock_rates._build_opportunity(item, query_id="abc123", stale_hours=48.0)
        assert built is not None
        opportunity, normalized_item = built
        assert normalized_item.item_family == "wand_caster"
        assert opportunity.item_family == "wand_caster"
        assert opportunity.valuation_result["model_source"] == "family_fallback"
        assert opportunity.trusted_profit > 0
        assert "family_fallback" in opportunity.risk_flags

    def test_high_ticket_low_confidence_flag(self, scanner_with_mock_rates):
        item = _make_item_detail(base_type="Opal Ring", price_amount=120.0)
        scanner_with_mock_rates.oracle.predict = MagicMock(
            return_value=ValuationResult(
                predicted_value=170.0,
                confidence=0.7,
                item_family="accessory_generic",
                model_source="family_fallback",
                feature_completeness=0.5,
            )
        )
        built = scanner_with_mock_rates._build_opportunity(item, query_id="abc123", stale_hours=48.0)
        opportunity, _ = built
        assert "high_ticket_low_confidence" in opportunity.risk_flags


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
        scanner_with_mock_rates.oracle.predict = MagicMock(
            return_value=ValuationResult(40.0, 0.40, "generic", "family_fallback", 0.2)
        )

        opportunities, stats = scanner_with_mock_rates.scan_opportunities(item_class="", max_items=1)

        assert opportunities == []
        assert stats.scan_profile == "open_market"
        assert stats.filtered_open_confidence == 1

    def test_targeted_scan_keeps_item_rejected_by_open_market(self, scanner_with_mock_rates):
        detail = _make_item_detail(base_type="Imbued Wand", price_amount=2.0, explicit_mods=["+#% increased Spell Damage"])
        scanner_with_mock_rates.api_client.fetch_item_details = MagicMock(return_value=[detail])
        scanner_with_mock_rates.oracle.predict = MagicMock(
            return_value=ValuationResult(35.0, 0.55, "wand_caster", "family_fallback", 0.5)
        )

        opportunities, stats = scanner_with_mock_rates.scan_opportunities(item_class="Imbued Wand", max_items=1)

        assert len(opportunities) == 1
        assert stats.scan_profile == "targeted"

    def test_scan_enriches_market_context_and_pricing_position(self, scanner_with_mock_rates):
        details = [
            _make_item_detail(item_id="cheap", base_type="Sadist Garb", price_amount=20.0),
            _make_item_detail(item_id="stable", base_type="Sadist Garb", price_amount=40.0),
        ]
        scanner_with_mock_rates.api_client.search_items = MagicMock(return_value=("abc123", ["cheap", "stable"]))
        scanner_with_mock_rates.api_client.fetch_item_details = MagicMock(return_value=details)
        scanner_with_mock_rates.oracle.predict = MagicMock(
            side_effect=[
                ValuationResult(80.0, 0.82, "body_armour_defense", "family_fallback", 0.67),
                ValuationResult(78.0, 0.82, "body_armour_defense", "family_fallback", 0.67),
            ]
        )

        opportunities, _ = scanner_with_mock_rates.scan_opportunities(item_class="", max_items=2)

        assert len(opportunities) == 2
        assert opportunities[0].market_floor > 0
        assert opportunities[0].comparables_count >= 1
        assert opportunities[0].pricing_position in {"below_floor", "near_market", "outlier"}
        assert opportunities[0].valuation_result["item_family"] == "body_armour_defense"

    def test_safe_buy_uses_dynamic_confidence_threshold_by_price(self, scanner_with_mock_rates):
        details = [
            _make_item_detail(item_id="item_low_price", base_type="Driftwood Wand", price_amount=40.0),
            _make_item_detail(item_id="item_mid_price", base_type="Imbued Wand", price_amount=60.0, explicit_mods=["+#% increased Spell Damage"]),
            _make_item_detail(item_id="item_high_price", base_type="Opal Ring", price_amount=130.0),
        ]
        scanner_with_mock_rates.api_client.search_items = MagicMock(return_value=("query123", ["a", "b", "c"]))
        scanner_with_mock_rates.api_client.fetch_item_details = MagicMock(return_value=details)
        scanner_with_mock_rates.oracle.predict = MagicMock(
            side_effect=[
                ValuationResult(80.0, 0.75, "generic", "family_fallback", 0.4),
                ValuationResult(100.0, 0.79, "wand_caster", "family_fallback", 0.5),
                ValuationResult(200.0, 0.81, "accessory_generic", "family_fallback", 0.5),
            ]
        )

        opportunities, stats = scanner_with_mock_rates.scan_opportunities(max_items=3, anti_fix=False, safe_buy=True)

        returned_ids = {opportunity.item_id for opportunity in opportunities}
        assert returned_ids == {"item_low_price", "item_mid_price"}
        assert stats.filtered_safe_buy_confidence == 1
