import pandas as pd

from core.market_intelligence import (
    MarketSegmentMetrics,
    build_market_segments,
    build_segment_key,
    score_market_segment,
)


def test_strong_candidate_requires_safety_liquidity_and_margin() -> None:
    metrics = MarketSegmentMetrics(
        sample_count=80,
        fresh_ratio=0.72,
        stale_ratio=0.08,
        price_floor=70.0,
        price_median=120.0,
        price_spread=0.42,
        volume_score=0.80,
        liquidity_score=0.74,
        safety_score=0.82,
        margin_score=0.68,
        trend_score=0.55,
    )

    result = score_market_segment(metrics)

    assert result.status == "strong_candidate"
    assert result.market_score > 0.70
    assert "safe" in result.explanation.lower()
    assert "liquid" in result.explanation.lower()


def test_low_sample_segment_is_not_promoted() -> None:
    metrics = MarketSegmentMetrics(
        sample_count=4,
        fresh_ratio=0.90,
        stale_ratio=0.0,
        price_floor=40.0,
        price_median=100.0,
        price_spread=0.60,
        volume_score=0.10,
        liquidity_score=0.20,
        safety_score=0.80,
        margin_score=0.80,
        trend_score=0.80,
    )

    result = score_market_segment(metrics)

    assert result.status == "avoid"
    assert "low evidence" in result.explanation.lower()


def test_low_sample_with_large_margin_is_evaluation_candidate() -> None:
    metrics = MarketSegmentMetrics(
        sample_count=4,
        fresh_ratio=1.0,
        stale_ratio=0.0,
        price_floor=80.0,
        price_median=140.0,
        price_spread=0.4286,
        volume_score=0.08,
        liquidity_score=0.68,
        safety_score=0.75,
        margin_score=0.75,
        trend_score=0.08,
    )

    result = score_market_segment(metrics)

    assert result.status == "evaluation_candidate"
    assert "manual evaluation" in result.explanation.lower()


def test_emerging_segment_can_have_moderate_evidence_and_high_trend() -> None:
    metrics = MarketSegmentMetrics(
        sample_count=24,
        fresh_ratio=0.58,
        stale_ratio=0.12,
        price_floor=30.0,
        price_median=75.0,
        price_spread=0.48,
        volume_score=0.52,
        liquidity_score=0.56,
        safety_score=0.63,
        margin_score=0.71,
        trend_score=0.84,
    )

    result = score_market_segment(metrics)

    assert result.status == "emerging"
    assert "trend" in result.explanation.lower()


def test_unsafe_segment_never_becomes_strong_candidate() -> None:
    metrics = MarketSegmentMetrics(
        sample_count=100,
        fresh_ratio=0.75,
        stale_ratio=0.40,
        price_floor=20.0,
        price_median=100.0,
        price_spread=0.80,
        volume_score=0.90,
        liquidity_score=0.85,
        safety_score=0.32,
        margin_score=0.95,
        trend_score=0.90,
    )

    result = score_market_segment(metrics)

    assert result.status == "avoid"
    assert "unsafe" in result.explanation.lower()


def test_build_segment_key_groups_similar_items() -> None:
    row = {
        "league": "Mirage",
        "item_family": "body_armour_defense",
        "base_type": "Vaal Regalia",
        "ilvl_band": "high",
        "price_chaos": 120.0,
        "tag_tokens": ["defence", "life", "resistance"],
        "mod_tokens": ["Life", "SpellSuppress", "Resist"],
    }

    key = build_segment_key(row)

    assert key == "Mirage|body_armour_defense|Vaal Regalia|high|51-150|Life+Resist+SpellSuppress|defence+life+resistance"


def test_build_market_segments_aggregates_rows_by_segment_key() -> None:
    frame = pd.DataFrame(
        [
            {
                "league": "Mirage",
                "item_family": "wand_caster",
                "base_type": "Imbued Wand",
                "ilvl_band": "high",
                "price_chaos": 90.0,
                "tag_tokens": ["caster", "spell"],
                "mod_tokens": ["SpellDamage", "CastSpeed"],
                "freshness_band": "fresh",
            },
            {
                "league": "Mirage",
                "item_family": "wand_caster",
                "base_type": "Imbued Wand",
                "ilvl_band": "high",
                "price_chaos": 120.0,
                "tag_tokens": ["spell", "caster"],
                "mod_tokens": ["CastSpeed", "SpellDamage"],
                "freshness_band": "active",
            },
            {
                "league": "Mirage",
                "item_family": "wand_caster",
                "base_type": "Imbued Wand",
                "ilvl_band": "high",
                "price_chaos": 240.0,
                "tag_tokens": ["caster", "spell"],
                "mod_tokens": ["SpellDamage", "CastSpeed"],
                "freshness_band": "fresh",
            },
        ]
    )

    segments = build_market_segments(frame)

    assert len(segments) == 2
    assert segments[0].metrics.sample_count == 2
    assert segments[0].segment.price_band == "51-150"
    assert segments[1].metrics.sample_count == 1
    assert segments[1].segment.price_band == "151-500"


def test_build_market_segments_includes_evaluation_opportunities() -> None:
    frame = pd.DataFrame(
        [
            {
                "item_id": "expensive_reference",
                "league": "Mirage",
                "item_family": "wand_caster",
                "base_type": "Imbued Wand",
                "ilvl_band": "high",
                "price_chaos": 140.0,
                "tag_tokens": ["caster", "spell"],
                "mod_tokens": ["SpellDamage", "CastSpeed"],
                "freshness_band": "fresh",
            },
            {
                "item_id": "cheap_candidate",
                "league": "Mirage",
                "item_family": "wand_caster",
                "base_type": "Imbued Wand",
                "ilvl_band": "high",
                "price_chaos": 80.0,
                "tag_tokens": ["caster", "spell"],
                "mod_tokens": ["SpellDamage", "CastSpeed"],
                "freshness_band": "fresh",
            },
            {
                "item_id": "stale_candidate",
                "league": "Mirage",
                "item_family": "wand_caster",
                "base_type": "Imbued Wand",
                "ilvl_band": "high",
                "price_chaos": 70.0,
                "tag_tokens": ["caster", "spell"],
                "mod_tokens": ["SpellDamage", "CastSpeed"],
                "freshness_band": "stale",
            },
        ]
    )

    segment = build_market_segments(frame)[0]

    assert segment.opportunities
    assert segment.opportunities[0].item_id == "cheap_candidate"
    assert segment.opportunities[0].mode == "evaluation"
    assert segment.opportunities[0].listed_price == 80.0
    assert segment.opportunities[0].reference_price == 110.0
    assert segment.opportunities[0].estimated_upside == 0.375


def test_market_opportunities_preserve_family_evidence_fields() -> None:
    frame = pd.DataFrame(
        [
            {
                "item_id": "cluster-expensive",
                "league": "Mirage",
                "item_family": "jewel_cluster",
                "base_type": "Large Cluster Jewel",
                "ilvl": 84,
                "ilvl_band": "high",
                "price_chaos": 140.0,
                "cluster_size": "large",
                "cluster_passives": 8,
                "cluster_enchant": "Minion Damage",
                "notables": ["Renewal"],
                "mod_tokens": ["ClusterPassive"],
                "tag_tokens": ["jewel"],
                "freshness_band": "fresh",
            },
            {
                "item_id": "cluster-cheap",
                "league": "Mirage",
                "item_family": "jewel_cluster",
                "base_type": "Large Cluster Jewel",
                "ilvl": 84,
                "ilvl_band": "high",
                "price_chaos": 80.0,
                "cluster_size": "large",
                "cluster_passives": 8,
                "cluster_enchant": "Minion Damage",
                "notables": ["Renewal"],
                "mod_tokens": ["ClusterPassive"],
                "tag_tokens": ["jewel"],
                "freshness_band": "fresh",
            },
        ]
    )

    opportunity = build_market_segments(frame)[0].opportunities[0].to_dict()

    assert opportunity["item_id"] == "cluster-cheap"
    assert opportunity["ilvl"] == 84
    assert opportunity["cluster_size"] == "large"
    assert opportunity["cluster_passives"] == 8
    assert opportunity["cluster_enchant"] == "Minion Damage"
    assert opportunity["notables"] == ["Renewal"]
    assert opportunity["mod_tokens"] == ["ClusterPassive"]


def test_market_segments_support_gold_schema_without_item_tokens() -> None:
    frame = pd.DataFrame(
        [
            {
                "event_key": "gold-event-1",
                "league": "Standard",
                "item_family": "accessory_generic",
                "base_type": "Diamond Ring",
                "ilvl_band": "high",
                "price_chaos": 20.0,
                "has_life": 1.0,
                "has_resist": 1.0,
                "has_attributes": 0.0,
                "freshness_band": "fresh",
            },
            {
                "event_key": "gold-event-2",
                "league": "Standard",
                "item_family": "accessory_generic",
                "base_type": "Diamond Ring",
                "ilvl_band": "high",
                "price_chaos": 40.0,
                "has_life": 1.0,
                "has_resist": 1.0,
                "has_attributes": 0.0,
                "freshness_band": "active",
            },
        ]
    )

    segment = build_market_segments(frame)[0]

    assert "Life" in segment.segment.mod_signature
    assert "Resist" in segment.segment.mod_signature
    assert segment.opportunities[0].item_id == "gold-event-1"
