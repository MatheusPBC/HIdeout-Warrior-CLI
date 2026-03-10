import pytest
import asyncio
import sys
import os
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.market_scanner import ListingSnapshot, OnDemandScanner, ScanOpportunity
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


class TestHybridPipelineAndMetrics:
    @patch("core.market_scanner.MarketAPIClient")
    @patch("core.market_scanner.PricePredictor")
    def test_dedupe_ttl_cache(self, _mock_oracle_cls, mock_client_cls):
        mock_client = mock_client_cls.return_value
        mock_client.league = "Standard"
        mock_client.sync_ninja_economy.return_value = {"Chaos Orb": 1.0}

        scanner = OnDemandScanner(league="Standard")
        scanner._dedupe_ttl_seconds = 60

        assert scanner._register_item_if_new("item-1", now_ts=100.0) is True
        assert scanner._register_item_if_new("item-1", now_ts=120.0) is False
        assert scanner._register_item_if_new("item-1", now_ts=161.0) is True

    @patch("core.market_scanner.MarketAPIClient")
    @patch("core.market_scanner.PricePredictor")
    def test_stage_b_runs_only_for_stage_a_approved_items(
        self,
        _mock_oracle_cls,
        mock_client_cls,
    ):
        mock_client = mock_client_cls.return_value
        mock_client.league = "Standard"
        mock_client.sync_ninja_economy.return_value = {"Chaos Orb": 1.0}

        scanner = OnDemandScanner(league="Standard")
        scanner._execute_hybrid_ingestion = MagicMock(
            return_value=(
                {
                    "candidates": [
                        {
                            "listing": {
                                "whisper": "@seller hi",
                                "price": {"amount": 45.0, "currency": "chaos"},
                                "account": {"name": "seller1"},
                                "indexed": "2026-03-10T09:00:00Z",
                            },
                            "item": {
                                "id": "item_stage_a_ok",
                                "baseType": "Imbued Wand",
                                "ilvl": 84,
                                "explicitMods": ["+# to maximum Life"],
                                "implicitMods": [],
                                "corrupted": False,
                                "fractured": False,
                                "influences": {},
                            },
                        },
                        {
                            "listing": {
                                "whisper": "",
                                "price": {"amount": 45.0, "currency": "chaos"},
                                "account": {"name": "seller2"},
                                "indexed": "2026-03-10T09:00:00Z",
                            },
                            "item": {
                                "id": "item_stage_a_fail",
                                "baseType": "Imbued Wand",
                                "ilvl": 84,
                                "explicitMods": ["+# to maximum Life"],
                                "implicitMods": [],
                                "corrupted": False,
                                "fractured": False,
                                "influences": {},
                            },
                        },
                    ],
                    "coverage": {"40-120": 1},
                    "query_ids": ["query123"],
                    "total_found": 2,
                },
                {"candidates": [], "query_ids": [], "total_found": 0},
            )
        )

        scanner._stage_b_ml_evaluation = MagicMock(return_value=None)

        opportunities, stats = scanner.scan_opportunities(max_items=10, anti_fix=False)

        assert opportunities == []
        assert scanner._stage_b_ml_evaluation.call_count == 1
        assert stats.stage_a_passed == 1

    @patch("core.market_scanner.MarketAPIClient")
    @patch("core.market_scanner.PricePredictor")
    def test_non_linear_ticket_approval_rules(self, _mock_oracle_cls, mock_client_cls):
        mock_client = mock_client_cls.return_value
        mock_client.league = "Standard"
        mock_client.sync_ninja_economy.return_value = {"Chaos Orb": 1.0}

        scanner = OnDemandScanner(league="Standard")

        low_ticket = ScanOpportunity(
            item_id="a",
            base_type="Imbued Wand",
            ilvl=84,
            listed_price=30.0,
            ml_value=40.0,
            ml_confidence=0.7,
            profit=5.0,
            score=50.0,
            valuation_gap=5.0,
            relative_discount=0.15,
            whisper="@seller hi",
            trade_link="x",
            trade_search_link="x",
            listing_currency="chaos",
            listing_amount=30.0,
            seller="seller",
            indexed_at=None,
            resolved_league="Standard",
            corrupted=False,
            fractured=False,
        )
        mid_ticket = ScanOpportunity(
            **{
                **low_ticket.to_dict(),
                "item_id": "b",
                "listed_price": 70.0,
                "profit": 12.0,
            }
        )
        high_ticket_fail = ScanOpportunity(
            **{
                **low_ticket.to_dict(),
                "item_id": "c",
                "listed_price": 180.0,
                "profit": 30.0,
                "ml_confidence": 0.79,
            }
        )
        high_ticket_ok = ScanOpportunity(
            **{
                **low_ticket.to_dict(),
                "item_id": "d",
                "listed_price": 180.0,
                "profit": 31.0,
                "ml_confidence": 0.85,
            }
        )

        assert scanner._passes_non_linear_ticket_rule(low_ticket) is True
        assert scanner._passes_non_linear_ticket_rule(mid_ticket) is True
        assert scanner._passes_non_linear_ticket_rule(high_ticket_fail) is False
        assert scanner._passes_non_linear_ticket_rule(high_ticket_ok) is True

    @patch("core.market_scanner.MarketAPIClient")
    @patch("core.market_scanner.PricePredictor")
    def test_scan_stats_exposes_new_hybrid_metrics(
        self, _mock_oracle_cls, mock_client_cls
    ):
        mock_client = mock_client_cls.return_value
        mock_client.league = "Standard"
        mock_client.sync_ninja_economy.return_value = {"Chaos Orb": 1.0}

        scanner = OnDemandScanner(league="Standard")
        scanner._execute_hybrid_ingestion = MagicMock(
            return_value=(
                {
                    "candidates": [
                        {
                            "listing": {
                                "whisper": "@seller hi",
                                "price": {"amount": 60.0, "currency": "chaos"},
                                "account": {"name": "seller1"},
                                "indexed": "2026-03-10T09:00:00Z",
                            },
                            "item": {
                                "id": "same-id",
                                "baseType": "Imbued Wand",
                                "ilvl": 84,
                                "explicitMods": ["+# to maximum Life"],
                                "implicitMods": [],
                                "corrupted": False,
                                "fractured": False,
                                "influences": {},
                            },
                        }
                    ],
                    "coverage": {"40-120": 1},
                    "query_ids": ["query123"],
                    "total_found": 1,
                },
                {
                    "candidates": [
                        {
                            "listing": {
                                "whisper": "@seller hi",
                                "price": {"amount": 60.0, "currency": "chaos"},
                                "account": {"name": "seller1"},
                                "indexed": "2026-03-10T09:00:00Z",
                            },
                            "item": {
                                "id": "same-id",
                                "baseType": "Imbued Wand",
                                "ilvl": 84,
                                "explicitMods": ["+# to maximum Life"],
                                "implicitMods": [],
                                "corrupted": False,
                                "fractured": False,
                                "influences": {},
                            },
                        }
                    ],
                    "query_ids": ["query999"],
                    "total_found": 1,
                },
            )
        )
        scanner._stage_b_ml_evaluation = MagicMock(return_value=None)

        _, stats = scanner.scan_opportunities(max_items=10, anti_fix=False)

        assert isinstance(stats.coverage_by_bucket, dict)
        assert hasattr(stats, "candidates_macro")
        assert hasattr(stats, "candidates_micro")
        assert hasattr(stats, "deduped")
        assert hasattr(stats, "stage_a_passed")
        assert hasattr(stats, "stage_b_passed")
        assert hasattr(stats, "final_approval_rate")
        assert stats.candidates_macro == 1
        assert stats.candidates_micro == 1
        assert stats.deduped == 1


class TestHybridHotfixes:
    @patch("core.market_scanner.MarketAPIClient")
    @patch("core.market_scanner.PricePredictor")
    def test_build_trade_query_avoids_invalid_flag_filters(
        self, _mock_oracle_cls, mock_client_cls
    ):
        mock_client = mock_client_cls.return_value
        mock_client.league = "Mirage"
        mock_client.sync_ninja_economy.return_value = {"Chaos Orb": 1.0}

        scanner = OnDemandScanner(league="Mirage")
        query = scanner.build_trade_query(
            item_class="Imbued Wand",
            ilvl_min=84,
            is_influenced=True,
            fractured_only=True,
        )
        misc_filters = query["query"]["filters"]["misc_filters"]["filters"]

        assert "influence" not in misc_filters
        assert "fractured" not in misc_filters

    @patch("core.market_scanner.MarketAPIClient")
    @patch("core.market_scanner.PricePredictor")
    def test_safe_search_retries_without_invalid_flag_filters(
        self, _mock_oracle_cls, mock_client_cls
    ):
        mock_client = mock_client_cls.return_value
        mock_client.league = "Mirage"
        mock_client.sync_ninja_economy.return_value = {"Chaos Orb": 1.0}
        mock_client.search_items.side_effect = [
            ("", []),
            ("query-ok", ["fetch-id"]),
        ]
        mock_client.fetch_item_details.return_value = [
            {
                "id": "top-id",
                "listing": {
                    "whisper": "@seller hi",
                    "price": {"amount": 10.0, "currency": "chaos"},
                    "account": {"name": "seller"},
                    "indexed": "2026-03-10T09:00:00Z",
                },
                "item": {
                    "id": "nested-id",
                    "baseType": "Imbued Wand",
                    "ilvl": 84,
                    "explicitMods": ["+# to maximum Life"],
                    "implicitMods": [],
                    "corrupted": False,
                    "fractured": False,
                    "influences": {},
                },
            }
        ]

        scanner = OnDemandScanner(league="Mirage")
        query = scanner.build_trade_query(item_class="Imbued Wand", ilvl_min=84)
        query["query"]["filters"]["misc_filters"]["filters"]["influence"] = {
            "option": "true"
        }
        query["query"]["filters"]["misc_filters"]["filters"]["fractured"] = {
            "option": "true"
        }

        query_id, details, total = scanner._safe_search_and_fetch(query, max_items=5)

        assert query_id == "query-ok"
        assert len(details) == 1
        assert total == 1
        assert mock_client.search_items.call_count == 2
        first_payload = mock_client.search_items.call_args_list[0][0][0]
        second_payload = mock_client.search_items.call_args_list[1][0][0]
        assert (
            "influence" in first_payload["query"]["filters"]["misc_filters"]["filters"]
        )
        assert (
            "fractured" in first_payload["query"]["filters"]["misc_filters"]["filters"]
        )
        assert (
            "influence"
            not in second_payload["query"]["filters"]["misc_filters"]["filters"]
        )
        assert (
            "fractured"
            not in second_payload["query"]["filters"]["misc_filters"]["filters"]
        )

    @patch("core.market_scanner.MarketAPIClient")
    @patch("core.market_scanner.PricePredictor")
    def test_macro_sweep_uses_segment_budget_and_cursor_rotation(
        self, _mock_oracle_cls, mock_client_cls
    ):
        mock_client = mock_client_cls.return_value
        mock_client.league = "Mirage"
        mock_client.sync_ninja_economy.return_value = {"Chaos Orb": 1.0}

        scanner = OnDemandScanner(league="Mirage")
        seen_segments_first_run = []

        def fake_safe_search(query, _max_items):
            signature = (
                query["query"].get("type"),
                query["query"]["filters"]["misc_filters"]["filters"]["ilvl"].get("min"),
                query["query"]["filters"]["trade_filters"]["filters"]["price"].get(
                    "min"
                ),
            )
            seen_segments_first_run.append(signature)
            return "q", [], 0

        scanner._safe_search_and_fetch = MagicMock(side_effect=fake_safe_search)

        asyncio.run(
            scanner._run_macro_sweep(
                item_class="",
                ilvl_min=75,
                rarity="rare",
                max_items=8,
                min_listed_price=1.0,
            )
        )

        budget = scanner._macro_query_budget(max_items=8, total_segments=180)
        assert len(seen_segments_first_run) == budget

        seen_segments_second_run = []

        def fake_safe_search_second(query, _max_items):
            signature = (
                query["query"].get("type"),
                query["query"]["filters"]["misc_filters"]["filters"]["ilvl"].get("min"),
                query["query"]["filters"]["trade_filters"]["filters"]["price"].get(
                    "min"
                ),
            )
            seen_segments_second_run.append(signature)
            return "q", [], 0

        scanner._safe_search_and_fetch = MagicMock(side_effect=fake_safe_search_second)

        asyncio.run(
            scanner._run_macro_sweep(
                item_class="",
                ilvl_min=75,
                rarity="rare",
                max_items=8,
                min_listed_price=1.0,
            )
        )

        assert len(seen_segments_second_run) == budget
        assert seen_segments_first_run[0] != seen_segments_second_run[0]

    @patch("core.market_scanner.MarketAPIClient")
    @patch("core.market_scanner.PricePredictor")
    def test_dedupe_prefers_top_level_id_over_nested_item_id(
        self, _mock_oracle_cls, mock_client_cls
    ):
        mock_client = mock_client_cls.return_value
        mock_client.league = "Mirage"
        mock_client.sync_ninja_economy.return_value = {"Chaos Orb": 1.0}

        scanner = OnDemandScanner(league="Mirage")
        scanner._execute_hybrid_ingestion = MagicMock(
            return_value=(
                {
                    "candidates": [
                        {
                            "id": "top-id-1",
                            "listing": {
                                "whisper": "@seller hi",
                                "price": {"amount": 40.0, "currency": "chaos"},
                                "account": {"name": "seller1"},
                                "indexed": "2026-03-10T09:00:00Z",
                            },
                            "item": {
                                "id": "nested-a",
                                "baseType": "Imbued Wand",
                                "ilvl": 84,
                                "explicitMods": ["+# to maximum Life"],
                                "implicitMods": [],
                                "corrupted": False,
                                "fractured": False,
                                "influences": {},
                            },
                        }
                    ],
                    "coverage": {"10-40": 1},
                    "query_ids": ["query-a"],
                    "total_found": 1,
                },
                {
                    "candidates": [
                        {
                            "id": "top-id-1",
                            "listing": {
                                "whisper": "@seller hi",
                                "price": {"amount": 42.0, "currency": "chaos"},
                                "account": {"name": "seller2"},
                                "indexed": "2026-03-10T09:00:00Z",
                            },
                            "item": {
                                "id": "nested-b",
                                "baseType": "Imbued Wand",
                                "ilvl": 84,
                                "explicitMods": ["+# to maximum Life"],
                                "implicitMods": [],
                                "corrupted": False,
                                "fractured": False,
                                "influences": {},
                            },
                        }
                    ],
                    "query_ids": ["query-b"],
                    "total_found": 1,
                },
            )
        )
        scanner._stage_b_ml_evaluation = MagicMock(return_value=None)

        _, stats = scanner.scan_opportunities(max_items=10, anti_fix=False)

        assert stats.deduped == 1
