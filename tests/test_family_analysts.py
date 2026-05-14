from core.family_analysts import analyze_family_candidate, get_family_analyst


def test_registry_routes_jewel_cluster_to_conservative_specialist() -> None:
    analyst = get_family_analyst("jewel_cluster")

    result = analyst.analyze(
        segment={"item_family": "jewel_cluster", "base_type": "Crimson Jewel"},
        metrics={"sample_count": 4},
        opportunity={"listed_price": 20.0, "reference_price": 60.0},
        model={"mae": 12.0},
    )

    assert result.family == "jewel_cluster"
    assert result.analyst == "JewelClusterAnalyst"
    assert result.decision == "needs_domain_rules"
    assert result.score == 0.0
    assert "cluster_jewel_rules_pending" in result.risks


def test_unknown_family_uses_generic_market_context_analysis() -> None:
    result = analyze_family_candidate(
        family="unknown_family",
        segment={"base_type": "Unknown Base"},
        metrics={"sample_count": 8},
        opportunity={"listed_price": 10.0, "reference_price": 25.0},
        model={"mae": 4.0},
    )

    assert result.family == "unknown_family"
    assert result.analyst == "GenericFamilyAnalyst"
    assert result.score > 0.0
    assert "positive_market_edge" in result.reasons
    assert "domain_rules_missing" in result.risks
