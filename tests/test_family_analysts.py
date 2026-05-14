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
    assert result.decision == "needs_more_evidence"
    assert result.score == 0.0
    assert "missing_cluster_evidence" in result.risks


def test_jewel_cluster_excludes_inefficient_large_low_ilvl_base() -> None:
    result = analyze_family_candidate(
        family="jewel_cluster",
        segment={
            "base_type": "Large Cluster Jewel",
            "cluster_size": "large",
            "cluster_passives": 10,
            "ilvl": 83,
            "cluster_enchant": "Minion Damage",
        },
        metrics={"sample_count": 12},
        opportunity={"listed_price": 20.0, "reference_price": 60.0},
        model={"mae": 12.0},
    )

    assert result.decision == "exclude"
    assert result.score == 0.0
    assert "large_cluster_too_many_passives_below_ilvl_84" in result.risks


def test_jewel_cluster_flags_premium_large_craft_base() -> None:
    result = analyze_family_candidate(
        family="jewel_cluster",
        segment={
            "base_type": "Large Cluster Jewel",
            "cluster_size": "large",
            "cluster_passives": 8,
            "ilvl": 84,
            "cluster_enchant": "Minion Damage",
        },
        metrics={"sample_count": 16},
        opportunity={"listed_price": 90.0, "reference_price": 140.0},
        model={"mae": 20.0},
    )

    assert result.decision == "valid_for_manual_review"
    assert result.score >= 70.0
    assert "premium_large_8_passives" in result.reasons
    assert "premium_cluster_enchant" in result.reasons


def test_jewel_cluster_excludes_medium_six_passives() -> None:
    result = analyze_family_candidate(
        family="jewel_cluster",
        segment={
            "base_type": "Medium Cluster Jewel",
            "cluster_size": "medium",
            "cluster_passives": 6,
            "ilvl": 84,
            "cluster_enchant": "Aura Effect",
        },
        metrics={"sample_count": 12},
        opportunity={"listed_price": 15.0, "reference_price": 40.0},
        model={"mae": 8.0},
    )

    assert result.decision == "exclude"
    assert "medium_cluster_six_passives" in result.risks


def test_jewel_cluster_marks_known_bad_meta_notable_as_risk() -> None:
    result = analyze_family_candidate(
        family="jewel_cluster",
        segment={
            "base_type": "Large Cluster Jewel",
            "cluster_size": "large",
            "cluster_passives": 8,
            "ilvl": 84,
            "cluster_enchant": "Minion Damage",
            "notables": ["Primordial Bond"],
        },
        metrics={"sample_count": 12},
        opportunity={"listed_price": 15.0, "reference_price": 50.0},
        model={"mae": 8.0},
    )

    assert result.decision == "exclude"
    assert "bad_meta_notable_primordial_bond" in result.risks


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
