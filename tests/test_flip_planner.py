from unittest.mock import patch

from core.flip_planner import FlipAdvisor
from core.market_scanner import ScanOpportunity, ScanStats


class TestFlipAdvisor:
    @patch("core.flip_planner.OnDemandScanner")
    def test_recommend_plans_returns_ranked_profitable_plans(self, mock_scanner_cls):
        scanner = mock_scanner_cls.return_value
        scanner.scan_opportunities.return_value = (
            [
                ScanOpportunity(
                    item_id="wand-1",
                    base_type="Imbued Wand",
                    ilvl=84,
                    listed_price=40.0,
                    ml_value=95.0,
                    ml_confidence=0.72,
                    profit=55.0,
                    score=88.0,
                    valuation_gap=55.0,
                    relative_discount=1.38,
                    whisper="@seller one",
                    trade_link="https://trade/1",
                    trade_search_link="https://trade/search/1",
                    listing_currency="chaos",
                    listing_amount=40.0,
                    seller="seller1",
                    indexed_at="2024-01-15T10:30:00Z",
                    resolved_league="Standard",
                    corrupted=False,
                    fractured=False,
                    influences=[],
                    explicit_mods=["+#% increased Spell Damage"],
                    implicit_mods=[],
                    risk_flags=[],
                ),
                ScanOpportunity(
                    item_id="armour-1",
                    base_type="Sadist Garb",
                    ilvl=86,
                    listed_price=55.0,
                    ml_value=100.0,
                    ml_confidence=0.65,
                    profit=45.0,
                    score=70.0,
                    valuation_gap=45.0,
                    relative_discount=0.82,
                    whisper="@seller two",
                    trade_link="https://trade/2",
                    trade_search_link="https://trade/search/2",
                    listing_currency="chaos",
                    listing_amount=55.0,
                    seller="seller2",
                    indexed_at="2024-01-15T10:30:00Z",
                    resolved_league="Standard",
                    corrupted=False,
                    fractured=True,
                    influences=[],
                    explicit_mods=["+# to maximum Life"],
                    implicit_mods=[],
                    risk_flags=["fractured"],
                ),
            ],
            ScanStats(resolved_league="Standard"),
        )

        advisor = FlipAdvisor(league="Standard")
        plans, _ = advisor.recommend_plans(budget=160.0)

        assert len(plans) >= 1
        assert plans[0].expected_profit > 0
        assert plans[0].opportunity.base_type == "Imbued Wand"
        assert plans[0].stop_condition

    @patch("core.flip_planner.OnDemandScanner")
    def test_build_plans_rejects_budget_overflow(self, mock_scanner_cls):
        scanner = mock_scanner_cls.return_value
        scanner.scan_opportunities.return_value = (
            [
                ScanOpportunity(
                    item_id="wand-1",
                    base_type="Imbued Wand",
                    ilvl=84,
                    listed_price=40.0,
                    ml_value=95.0,
                    ml_confidence=0.72,
                    profit=55.0,
                    score=88.0,
                    valuation_gap=55.0,
                    relative_discount=1.38,
                    whisper="@seller one",
                    trade_link="https://trade/1",
                    trade_search_link="https://trade/search/1",
                    listing_currency="chaos",
                    listing_amount=40.0,
                    seller="seller1",
                    indexed_at="2024-01-15T10:30:00Z",
                    resolved_league="Standard",
                    corrupted=False,
                    fractured=False,
                    influences=[],
                    explicit_mods=[],
                    implicit_mods=[],
                    risk_flags=[],
                )
            ],
            ScanStats(resolved_league="Standard"),
        )

        advisor = FlipAdvisor(league="Standard")
        plans, _ = advisor.recommend_plans(budget=20.0)

        assert plans == []
