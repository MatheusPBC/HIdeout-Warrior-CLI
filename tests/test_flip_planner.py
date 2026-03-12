from unittest.mock import patch

from core.flip_planner import FlipAdvisor, FlipTargetRecommendation
from core.market_scanner import ScanOpportunity, ScanStats


def _sample_opportunity(**overrides):
    payload = dict(
        item_id="armour-1",
        base_type="Sadist Garb",
        item_family="body_armour_defense",
        ilvl=84,
        listed_price=40.0,
        ml_value=92.0,
        ml_confidence=0.78,
        profit=52.0,
        score=84.0,
        valuation_gap=52.0,
        relative_discount=0.57,
        whisper="@seller one",
        trade_link="https://trade/1",
        trade_search_link="https://trade/search/1",
        listing_currency="chaos",
        listing_amount=40.0,
        seller="seller1",
        indexed_at="2026-03-11T10:30:00Z",
        resolved_league="Mirage",
        corrupted=False,
        fractured=False,
        influences=[],
        explicit_mods=["+# to maximum Life"],
        implicit_mods=[],
        prefix_count=1,
        suffix_count=0,
        open_prefixes=2,
        open_suffixes=3,
        mod_tokens=["Life1"],
        tag_tokens=["body_armour", "life"],
        trusted_profit=40.6,
        valuation_result={
            "predicted_value": 92.0,
            "confidence": 0.78,
            "item_family": "body_armour_defense",
            "model_source": "family_model",
            "feature_completeness": 0.72,
        },
        market_floor=45.0,
        market_median=60.0,
        comparables_count=4,
        market_spread=20.0,
        pricing_position="below_floor",
        risk_flags=[],
        defence_profile="evasion_energy_shield",
        attribute_profile="dex_int",
        socket_count=6,
        link_count=0,
        socket_colour_profile="G:3,R:2,B:1",
    )
    payload.update(overrides)
    return ScanOpportunity(**payload)


class TestFlipAdvisor:
    @patch("core.flip_planner.OnDemandScanner")
    def test_recommend_plans_returns_market_validated_plan(self, mock_scanner_cls):
        scanner = mock_scanner_cls.return_value
        scanner.scan_opportunities.return_value = ([
            _sample_opportunity(),
            _sample_opportunity(item_id="armour-2", listed_price=55.0, link_count=6, market_floor=120.0, market_median=140.0, comparables_count=3),
        ], ScanStats(resolved_league="Mirage"))
        scanner.currency_rates = {"Orb of Fusing": 0.05}

        advisor = FlipAdvisor(league="Mirage")
        plans, _ = advisor.recommend_plans(item_class="Sadist Garb", budget=80.0)

        assert len(plans) >= 1
        assert plans[0].expected_profit > 0
        assert plans[0].trusted_profit > 0
        assert plans[0].confidence_breakdown.overall_confidence >= 0.45
        assert "Saída validada por mercado" in plans[0].plan_explanation

    @patch("core.flip_planner.OnDemandScanner")
    def test_build_plan_rejects_suppression_on_armour_only_base(self, mock_scanner_cls):
        scanner = mock_scanner_cls.return_value
        scanner.scan_opportunities.return_value = ([], ScanStats(resolved_league="Mirage"))
        scanner.currency_rates = {"Orb of Fusing": 0.05}

        advisor = FlipAdvisor(league="Mirage")
        opportunity = _sample_opportunity(base_type="Astral Plate", defence_profile="armour", attribute_profile="str", comparables_count=5, market_floor=55.0, market_median=75.0)
        forced_target = FlipTargetRecommendation(label="Impossible Suppress Armour Flip", goal_mods=["Life1", "SpellSuppress1", "Resist1"], expected_value=110.0, confidence=0.7, rationale="forced target", requires_suppression=True, required_link_count=0)
        advisor._recommend_target = lambda *_args, **_kwargs: forced_target

        plans = advisor.build_plans_from_opportunities([opportunity], budget=200.0)

        assert plans == []

    @patch("core.flip_planner.OnDemandScanner")
    def test_build_plan_embeds_linking_cost_when_exit_requires_six_link(self, mock_scanner_cls):
        scanner = mock_scanner_cls.return_value
        scanner.scan_opportunities.return_value = ([], ScanStats(resolved_league="Mirage"))
        scanner.currency_rates = {"Orb of Fusing": 0.05}

        advisor = FlipAdvisor(league="Mirage")
        opportunities = [
            _sample_opportunity(item_id="base", listed_price=35.0, link_count=0, market_floor=40.0, market_median=50.0, comparables_count=4),
            _sample_opportunity(item_id="six-link", listed_price=180.0, link_count=6, market_floor=175.0, market_median=220.0, comparables_count=3, pricing_position="near_market"),
        ]

        plans = advisor.build_plans_from_opportunities(opportunities, budget=80.0)

        assert any(plan.opportunity.item_id == "base" and plan.exit_estimate.required_structure["link_count"] == 6 for plan in plans)
        plan = next(plan for plan in plans if plan.opportunity.item_id == "base" and plan.exit_estimate.required_structure["link_count"] == 6)
        assert any(step.action_type == "socket_linking" for step in plan.steps)
        linking_step = next(step for step in plan.steps if step.action_type == "socket_linking")
        assert linking_step.expected_cost == 60.0

    @patch("core.flip_planner.OnDemandScanner")
    def test_build_plan_rejects_six_link_exit_without_budget(self, mock_scanner_cls):
        scanner = mock_scanner_cls.return_value
        scanner.scan_opportunities.return_value = ([], ScanStats(resolved_league="Mirage"))
        scanner.currency_rates = {"Orb of Fusing": 0.1}

        advisor = FlipAdvisor(league="Mirage")
        opportunities = [
            _sample_opportunity(item_id="base", listed_price=35.0, link_count=0, market_floor=40.0, market_median=50.0, comparables_count=4),
            _sample_opportunity(item_id="six-link", listed_price=150.0, link_count=6, market_floor=145.0, market_median=160.0, comparables_count=3, pricing_position="near_market"),
        ]

        plans = advisor.build_plans_from_opportunities(opportunities, budget=50.0)

        assert all(plan.exit_estimate.required_structure["link_count"] <= plan.opportunity.link_count for plan in plans)
