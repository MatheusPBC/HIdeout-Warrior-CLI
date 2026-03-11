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

    def test_build_opportunity_includes_family_valuation_and_market_fields(
        self, scanner_with_mock_rates
    ):
        item = _make_item_detail(
            base_type="Imbued Wand",
            explicit_mods=["+#% increased Spell Damage", "+#% increased Cast Speed"],
        )
        scanner_with_mock_rates.oracle.predict = MagicMock(
            return_value=ValuationResult(
                predicted_value=80.0,
                confidence=0.74,
                item_family="wand_caster",
                model_source="family_fallback",
                feature_completeness=0.67,
            )
        )

        built = scanner_with_mock_rates._build_opportunity(
            item, query_id="abc123", stale_hours=48.0
        )
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
        built = scanner_with_mock_rates._build_opportunity(
            item, query_id="abc123", stale_hours=48.0
        )
        opportunity, _ = built
        assert "high_ticket_low_confidence" in opportunity.risk_flags


class TestScanProfiles:
    @pytest.fixture
    def scanner_with_mock_rates(self, mock_ninja_currency_response):
        with patch("core.market_scanner.MarketAPIClient"):
            scanner = OnDemandScanner(league="Standard")
            scanner.currency_rates = mock_ninja_currency_response
            scanner.api_client.league = "Standard"
            scanner.api_client.search_items = MagicMock(
                return_value=("abc123", ["id1", "id2", "id3"])
            )
            return scanner

    def test_open_market_filters_low_confidence(self, scanner_with_mock_rates):
        detail = _make_item_detail(price_amount=7.0)
        scanner_with_mock_rates.api_client.fetch_item_details = MagicMock(
            return_value=[detail]
        )
        scanner_with_mock_rates.oracle.predict = MagicMock(
            return_value=ValuationResult(40.0, 0.40, "generic", "family_fallback", 0.2)
        )

        opportunities, stats = scanner_with_mock_rates.scan_opportunities(
            item_class="", max_items=1
        )

        assert opportunities == []
        assert stats.scan_profile == "open_market"
        assert stats.stage_b_passed == 0

    def test_targeted_scan_keeps_item_rejected_by_open_market(
        self, scanner_with_mock_rates
    ):
        detail = _make_item_detail(
            base_type="Imbued Wand",
            price_amount=2.0,
            explicit_mods=["+#% increased Spell Damage"],
        )
        scanner_with_mock_rates.api_client.fetch_item_details = MagicMock(
            return_value=[detail]
        )
        scanner_with_mock_rates.oracle.predict = MagicMock(
            return_value=ValuationResult(
                3.5, 0.75, "wand_caster", "family_fallback", 0.5
            )
        )

        opportunities, stats = scanner_with_mock_rates.scan_opportunities(
            item_class="Imbued Wand", max_items=1
        )

        assert len(opportunities) == 1
        assert stats.scan_profile == "targeted"

    def test_scan_enriches_market_context_and_pricing_position(
        self, scanner_with_mock_rates
    ):
        details = [
            _make_item_detail(
                item_id="cheap", base_type="Sadist Garb", price_amount=20.0
            ),
            _make_item_detail(
                item_id="stable", base_type="Sadist Garb", price_amount=40.0
            ),
        ]
        scanner_with_mock_rates.api_client.search_items = MagicMock(
            return_value=("abc123", ["cheap", "stable"])
        )
        scanner_with_mock_rates.api_client.fetch_item_details = MagicMock(
            return_value=details
        )
        scanner_with_mock_rates.oracle.predict = MagicMock(
            side_effect=[
                ValuationResult(
                    80.0, 0.82, "body_armour_defense", "family_fallback", 0.67
                ),
                ValuationResult(
                    78.0, 0.82, "body_armour_defense", "family_fallback", 0.67
                ),
            ]
        )

        opportunities, _ = scanner_with_mock_rates.scan_opportunities(
            item_class="", max_items=2
        )

        assert len(opportunities) == 1
        assert opportunities[0].market_floor > 0
        assert opportunities[0].comparables_count >= 1
        assert opportunities[0].pricing_position in {
            "below_floor",
            "near_market",
            "outlier",
        }
        assert opportunities[0].valuation_result["item_family"] == "body_armour_defense"

    def test_safe_buy_uses_dynamic_confidence_threshold_by_price(
        self, scanner_with_mock_rates
    ):
        details = [
            _make_item_detail(
                item_id="item_low_price", base_type="Driftwood Wand", price_amount=40.0
            ),
            _make_item_detail(
                item_id="item_mid_price",
                base_type="Imbued Wand",
                price_amount=60.0,
                explicit_mods=["+#% increased Spell Damage"],
            ),
            _make_item_detail(
                item_id="item_high_price", base_type="Opal Ring", price_amount=130.0
            ),
        ]
        scanner_with_mock_rates.api_client.search_items = MagicMock(
            return_value=("query123", ["a", "b", "c"])
        )
        scanner_with_mock_rates.api_client.fetch_item_details = MagicMock(
            return_value=details
        )
        scanner_with_mock_rates.oracle.predict = MagicMock(
            side_effect=[
                ValuationResult(80.0, 0.75, "generic", "family_fallback", 0.4),
                ValuationResult(100.0, 0.79, "wand_caster", "family_fallback", 0.5),
                ValuationResult(
                    200.0, 0.81, "accessory_generic", "family_fallback", 0.5
                ),
            ]
        )

        opportunities, stats = scanner_with_mock_rates.scan_opportunities(
            max_items=3, anti_fix=False, safe_buy=True
        )

        returned_ids = {opportunity.item_id for opportunity in opportunities}
        assert returned_ids == {"item_low_price", "item_mid_price"}
        assert stats.filtered_safe_buy_confidence == 1

    def test_stage_a_discards_fractured_low_ilvl_before_oracle(
        self, scanner_with_mock_rates
    ):
        detail = _make_item_detail(
            item_id="fractured-low-ilvl",
            base_type="Imbued Wand",
            ilvl=77,
            fractured=True,
            price_amount=50.0,
            explicit_mods=["40% increased Spell Damage"],
        )
        scanner_with_mock_rates.api_client.search_items = MagicMock(
            return_value=("q1", ["fractured-low-ilvl"])
        )
        scanner_with_mock_rates.api_client.fetch_item_details = MagicMock(
            return_value=[detail]
        )
        scanner_with_mock_rates.oracle.predict = MagicMock(
            return_value=ValuationResult(
                150.0, 0.9, "wand_caster", "family_fallback", 0.9
            )
        )

        opportunities, stats = scanner_with_mock_rates.scan_opportunities(
            item_class="Imbued Wand", max_items=2, anti_fix=False
        )

        assert opportunities == []
        assert stats.total_evaluated == 0
        assert stats.filtered_stage_a_fractured_low_ilvl_brick >= 1
        scanner_with_mock_rates.oracle.predict.assert_not_called()

    def test_stage_a_discards_low_ilvl_without_twink_override_before_oracle(
        self, scanner_with_mock_rates
    ):
        detail = _make_item_detail(
            item_id="low-ilvl-no-twink-stage-a",
            base_type="Driftwood Wand",
            ilvl=70,
            price_amount=8.0,
            explicit_mods=["40% increased Spell Damage"],
        )
        scanner_with_mock_rates.api_client.search_items = MagicMock(
            return_value=("q1", ["low-ilvl-no-twink-stage-a"])
        )
        scanner_with_mock_rates.api_client.fetch_item_details = MagicMock(
            return_value=[detail]
        )
        scanner_with_mock_rates.oracle.predict = MagicMock(
            return_value=ValuationResult(90.0, 0.8, "wand_caster", "xgb_family", 0.8)
        )

        opportunities, stats = scanner_with_mock_rates.scan_opportunities(
            item_class="Driftwood Wand", max_items=2, anti_fix=False
        )

        assert opportunities == []
        assert stats.total_evaluated == 0
        assert stats.filtered_stage_a_low_ilvl_no_twink >= 1
        scanner_with_mock_rates.oracle.predict.assert_not_called()

    def test_low_ilvl_without_twink_fails_high_ticket_fallback_with_few_comparables(
        self, scanner_with_mock_rates
    ):
        detail = _make_item_detail(
            item_id="low-ilvl-no-twink",
            base_type="Imbued Wand",
            ilvl=79,
            price_amount=120.0,
            explicit_mods=["45% increased Spell Damage"],
        )
        scanner_with_mock_rates.api_client.search_items = MagicMock(
            return_value=("q1", ["low-ilvl-no-twink"])
        )
        scanner_with_mock_rates.api_client.fetch_item_details = MagicMock(
            return_value=[detail]
        )
        scanner_with_mock_rates.oracle.predict = MagicMock(
            return_value=ValuationResult(
                220.0, 0.82, "wand_caster", "family_fallback", 0.7
            )
        )

        opportunities, stats = scanner_with_mock_rates.scan_opportunities(
            item_class="Imbued Wand", max_items=3, anti_fix=False
        )

        assert opportunities == []
        assert stats.stage_b_passed == 0

    def test_low_ilvl_with_twink_override_bypasses_high_ticket_gate(
        self, scanner_with_mock_rates
    ):
        detail = _make_item_detail(
            item_id="low-ilvl-with-twink",
            base_type="Imbued Wand",
            ilvl=70,
            price_amount=80.0,
            explicit_mods=[
                "+1 to Level of all Spell Skill Gems",
                "45% increased Spell Damage",
            ],
        )
        scanner_with_mock_rates.api_client.search_items = MagicMock(
            return_value=("q1", ["low-ilvl-with-twink"])
        )
        scanner_with_mock_rates.api_client.fetch_item_details = MagicMock(
            return_value=[detail]
        )
        scanner_with_mock_rates.oracle.predict = MagicMock(
            return_value=ValuationResult(
                220.0, 0.86, "wand_caster", "family_fallback", 0.8
            )
        )

        opportunities, stats = scanner_with_mock_rates.scan_opportunities(
            item_class="Imbued Wand", max_items=3, anti_fix=False
        )

        assert len(opportunities) == 1
        assert stats.stage_b_passed == 1
        assert stats.filtered_stage_a_low_ilvl_no_twink == 0
        assert stats.total_evaluated == 1
        assert opportunities[0].twink_override is True

    def test_stage_b_blocks_low_evidence_ml_market_divergence_2x(
        self, scanner_with_mock_rates
    ):
        item = _make_item_detail(
            item_id="divergence-2x",
            base_type="Imbued Wand",
            ilvl=84,
            price_amount=10.0,
            explicit_mods=["40% increased Spell Damage"],
        )
        scanner_with_mock_rates.oracle.predict = MagicMock(
            return_value=ValuationResult(45.0, 0.9, "wand_caster", "xgb_family", 0.8)
        )

        built = scanner_with_mock_rates._build_opportunity(
            item, query_id="q1", stale_hours=48.0
        )
        assert built is not None
        opportunity, _ = built
        opportunity.market_median = 20.0
        opportunity.comparables_count = 1
        opportunity.pricing_position = "near_market"

        consensus, reason = scanner_with_mock_rates._stage_b_consensus_decision(
            opportunity
        )

        assert consensus is False
        assert reason == "low_evidence_ml_market_divergence_2x"

    def test_stage_b_blocks_low_evidence_ml_market_divergence_3x(
        self, scanner_with_mock_rates
    ):
        item = _make_item_detail(
            item_id="divergence-3x",
            base_type="Imbued Wand",
            ilvl=84,
            price_amount=10.0,
            explicit_mods=["40% increased Spell Damage"],
        )
        scanner_with_mock_rates.oracle.predict = MagicMock(
            return_value=ValuationResult(70.0, 0.9, "wand_caster", "xgb_family", 0.8)
        )

        built = scanner_with_mock_rates._build_opportunity(
            item, query_id="q1", stale_hours=48.0
        )
        assert built is not None
        opportunity, _ = built
        opportunity.market_median = 20.0
        opportunity.comparables_count = 2
        opportunity.pricing_position = "near_market"

        consensus, reason = scanner_with_mock_rates._stage_b_consensus_decision(
            opportunity
        )

        assert consensus is False
        assert reason == "low_evidence_ml_market_divergence_3x"

    def test_family_fallback_low_evidence_applies_cap_and_explains(
        self, scanner_with_mock_rates
    ):
        details = [
            _make_item_detail(
                item_id="cap-target",
                base_type="Imbued Wand",
                ilvl=84,
                price_amount=10.0,
                explicit_mods=["40% increased Spell Damage"],
            ),
            _make_item_detail(
                item_id="cap-comparable",
                base_type="Imbued Wand",
                ilvl=84,
                price_amount=20.0,
                explicit_mods=["40% increased Spell Damage"],
            ),
        ]
        scanner_with_mock_rates.api_client.search_items = MagicMock(
            return_value=("q1", ["cap-target", "cap-comparable"])
        )
        scanner_with_mock_rates.api_client.fetch_item_details = MagicMock(
            return_value=details
        )
        scanner_with_mock_rates.oracle.predict = MagicMock(
            side_effect=[
                ValuationResult(500.0, 0.9, "wand_caster", "family_fallback", 0.7),
                ValuationResult(40.0, 0.9, "wand_caster", "family_fallback", 0.7),
            ]
        )

        opportunities, _ = scanner_with_mock_rates.scan_opportunities(
            item_class="Imbued Wand", max_items=3, anti_fix=False
        )

        assert len(opportunities) >= 1
        capped = next(op for op in opportunities if op.item_id == "cap-target")
        assert capped.ml_value == 32.0
        assert "fallback_low_evidence" in capped.risk_flags
        assert capped.valuation_result.get("ml_value_cap_applied") is True
        assert "cap=500.0->32.0" in capped.valuation_explanation


class TestHybridScannerPhase4:
    @pytest.fixture
    def scanner(self, mock_ninja_currency_response):
        with patch("core.market_scanner.MarketAPIClient"):
            scanner = OnDemandScanner(league="Standard")
            scanner.currency_rates = mock_ninja_currency_response
            scanner.api_client.league = "Standard"
            return scanner

    def test_scan_respects_query_budget_per_cycle(self, scanner):
        scanner._query_budget_per_cycle = 2
        scanner._fetch_budget_per_cycle = 1
        scanner._build_micro_queries = MagicMock(return_value=[])
        scanner.api_client.search_items = MagicMock(return_value=("q1", ["id1"]))
        scanner.api_client.fetch_item_details = MagicMock(return_value=[])

        _, stats = scanner.scan_opportunities(max_items=10)

        assert stats.macro_queries == 1
        assert stats.micro_queries == 0
        assert stats.budget_exhausted == 1

    def test_macro_segment_rotation_is_deterministic_between_calls(self, scanner):
        scanner._query_budget_per_cycle = 4
        scanner._build_micro_queries = MagicMock(return_value=[])

        seen_price_mins: list[float] = []

        def _search_side_effect(query):
            price_min = query["query"]["filters"]["trade_filters"]["filters"]["price"][
                "min"
            ]
            seen_price_mins.append(price_min)
            return "", []

        scanner.api_client.search_items = MagicMock(side_effect=_search_side_effect)

        scanner.scan_opportunities(max_items=5)
        first_cycle_first_min = seen_price_mins[0]
        scanner.scan_opportunities(max_items=5)
        second_cycle_first_min = seen_price_mins[2]

        assert first_cycle_first_min != second_cycle_first_min

    def test_dedupe_ttl_avoids_reprocessing_same_item(self, scanner):
        scanner._query_budget_per_cycle = 1
        scanner._fetch_budget_per_cycle = 1
        scanner._build_micro_queries = MagicMock(return_value=[])
        scanner.api_client.search_items = MagicMock(return_value=("q1", ["id-1"]))
        scanner.api_client.fetch_item_details = MagicMock(
            return_value=[_make_item_detail(item_id="id-1", price_amount=10.0)]
        )
        scanner.oracle.predict = MagicMock(
            return_value=ValuationResult(60.0, 0.8, "generic", "family_fallback", 0.8)
        )

        first_opps, first_stats = scanner.scan_opportunities(max_items=3)
        second_opps, second_stats = scanner.scan_opportunities(max_items=3)

        assert len(first_opps) == 1
        assert first_stats.total_evaluated == 1
        assert second_opps == []
        assert second_stats.total_evaluated == 0
        assert second_stats.deduped_ttl >= 1

    def test_stage_a_stage_b_pipeline_keeps_only_consensus(self, scanner):
        scanner._query_budget_per_cycle = 1
        scanner._fetch_budget_per_cycle = 1
        scanner._build_micro_queries = MagicMock(return_value=[])
        scanner.api_client.search_items = MagicMock(
            return_value=("q1", ["cheap", "expensive"])
        )
        scanner.api_client.fetch_item_details = MagicMock(
            return_value=[
                _make_item_detail(
                    item_id="cheap", base_type="Sadist Garb", price_amount=50.0
                ),
                _make_item_detail(
                    item_id="expensive", base_type="Sadist Garb", price_amount=100.0
                ),
            ]
        )
        scanner.oracle.predict = MagicMock(
            side_effect=[
                ValuationResult(
                    130.0, 0.84, "body_armour_defense", "family_fallback", 0.9
                ),
                ValuationResult(
                    115.0, 0.84, "body_armour_defense", "family_fallback", 0.9
                ),
            ]
        )

        opportunities, stats = scanner.scan_opportunities(max_items=5, anti_fix=False)

        returned_ids = {op.item_id for op in opportunities}
        assert returned_ids == {"cheap"}
        assert stats.stage_a_candidates == 2
        assert stats.stage_b_passed == 1

    def test_run_scan_keeps_public_compatibility(self, scanner):
        scanner._query_budget_per_cycle = 1
        scanner._fetch_budget_per_cycle = 1
        scanner._build_micro_queries = MagicMock(return_value=[])
        scanner.api_client.search_items = MagicMock(return_value=("q1", ["id-compat"]))
        scanner.api_client.fetch_item_details = MagicMock(
            return_value=[
                _make_item_detail(
                    item_id="id-compat", base_type="Imbued Wand", price_amount=15.0
                )
            ]
        )
        scanner.oracle.predict = MagicMock(
            return_value=ValuationResult(
                70.0, 0.8, "wand_caster", "family_fallback", 0.8
            )
        )

        results, stats = scanner.run_scan(max_items=3, anti_fix=False)

        assert isinstance(results, list)
        assert isinstance(results[0], dict)
        assert "base_type" in results[0]
        assert "score" in results[0]
        assert "valuation_explanation" in results[0]
        assert results[0]["valuation_explanation"]
        assert hasattr(stats, "total_found")
        assert hasattr(stats, "avg_profit")

    def test_scan_emits_operational_metric_with_error_status(
        self, scanner, monkeypatch
    ):
        captured = {}

        def _capture_metric(**kwargs):
            captured.update(kwargs)

        monkeypatch.setattr("core.market_scanner.append_metric_event", _capture_metric)
        scanner._query_budget_per_cycle = 1
        scanner._build_micro_queries = MagicMock(return_value=[])
        scanner.api_client.search_items = MagicMock(side_effect=RuntimeError("boom"))

        opportunities, stats = scanner.scan_opportunities(max_items=2)

        assert opportunities == []
        assert stats.total_found == 0
        assert captured["component"] == "market_scanner.scan_opportunities"
        assert captured["status"] == "error"
        assert captured["error_count"] >= 1
