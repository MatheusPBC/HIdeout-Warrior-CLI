from unittest.mock import patch

from core.flip_planner import FlipAdvisor
from core.market_scanner import ScanOpportunity, ScanStats


def _sample_opportunity(**overrides):
    payload = dict(
        item_id="wand-1",
        base_type="Imbued Wand",
        item_family="wand_caster",
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
        prefix_count=1,
        suffix_count=0,
        open_prefixes=2,
        open_suffixes=3,
        mod_tokens=["SpellDamage1"],
        tag_tokens=["wand", "caster", "spell"],
        trusted_profit=39.6,
        valuation_result={
            "predicted_value": 95.0,
            "confidence": 0.72,
            "item_family": "wand_caster",
            "model_source": "family_fallback",
            "feature_completeness": 0.67,
        },
        market_floor=42.0,
        market_median=58.0,
        comparables_count=5,
        market_spread=20.0,
        pricing_position="below_floor",
        risk_flags=[],
    )
    payload.update(overrides)
    return ScanOpportunity(**payload)


class TestFlipAdvisor:
    @patch("core.flip_planner.OnDemandScanner")
    def test_recommend_plans_returns_ranked_profitable_plans(self, mock_scanner_cls):
        scanner = mock_scanner_cls.return_value
        scanner.scan_opportunities.return_value = (
            [
                _sample_opportunity(),
                _sample_opportunity(
                    item_id="armour-1",
                    base_type="Sadist Garb",
                    item_family="body_armour_defense",
                    listed_price=55.0,
                    ml_value=100.0,
                    ml_confidence=0.65,
                    profit=45.0,
                    score=70.0,
                    trusted_profit=29.3,
                    valuation_gap=45.0,
                    relative_discount=0.82,
                    seller="seller2",
                    explicit_mods=["+# to maximum Life"],
                    mod_tokens=["Life1"],
                    valuation_result={
                        "predicted_value": 100.0,
                        "confidence": 0.65,
                        "item_family": "body_armour_defense",
                        "model_source": "family_fallback",
                        "feature_completeness": 0.5,
                    },
                    market_floor=60.0,
                    market_median=78.0,
                    comparables_count=4,
                    market_spread=18.0,
                    pricing_position="below_floor",
                    risk_flags=["fractured"],
                    fractured=True,
                ),
            ],
            ScanStats(resolved_league="Standard"),
        )

        advisor = FlipAdvisor(league="Standard")
        plans, _ = advisor.recommend_plans(budget=160.0)

        assert len(plans) >= 1
        assert plans[0].expected_profit > 0
        assert plans[0].opportunity.base_type == "Imbued Wand"
        assert plans[0].target.label == "Caster Wand Flip"
        assert plans[0].stop_condition

    @patch("core.flip_planner.OnDemandScanner")
    def test_build_plans_rejects_budget_overflow(self, mock_scanner_cls):
        scanner = mock_scanner_cls.return_value
        scanner.scan_opportunities.return_value = (
            [_sample_opportunity()],
            ScanStats(resolved_league="Standard"),
        )

        advisor = FlipAdvisor(league="Standard")
        plans, _ = advisor.recommend_plans(budget=20.0)

        assert plans == []
