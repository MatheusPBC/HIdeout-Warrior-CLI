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


def test_accessory_analyst_flags_cord_belt_for_review() -> None:
    result = analyze_family_candidate(
        family="accessory_generic",
        segment={"base_type": "Cord Belt", "ilvl": 86, "open_suffixes": 1},
        metrics={"sample_count": 10},
        opportunity={"listed_price": 80.0, "reference_price": 140.0},
        model={"mae": 20.0},
    )

    assert result.analyst == "AccessoryAnalyst"
    assert result.decision == "valid_for_manual_review"
    assert "premium_accessory_base" in result.reasons
    assert "ilvl_85_plus" in result.reasons


def test_accessory_analyst_excludes_low_value_amulet_without_core_stats() -> None:
    result = analyze_family_candidate(
        family="accessory_generic",
        segment={"base_type": "Coral Amulet", "ilvl": 82, "mod_tokens": ["Resist"]},
        metrics={"sample_count": 10},
        opportunity={"listed_price": 5.0, "reference_price": 8.0},
        model={"mae": 3.0},
    )

    assert result.decision == "exclude"
    assert "amulet_missing_life_es_or_gem_levels" in result.risks


def test_wand_caster_analyst_flags_plus_gem_open_suffix_wand() -> None:
    result = analyze_family_candidate(
        family="wand_caster",
        segment={
            "base_type": "Profane Wand",
            "ilvl": 84,
            "mod_tokens": ["PlusAllSpellGems", "CastSpeed"],
            "open_suffixes": 1,
        },
        metrics={"sample_count": 14},
        opportunity={"listed_price": 120.0, "reference_price": 220.0},
        model={"mae": 35.0},
    )

    assert result.analyst == "WandCasterAnalyst"
    assert result.decision == "valid_for_manual_review"
    assert "plus_spell_gem_level" in result.reasons
    assert "open_suffix_for_trigger" in result.reasons


def test_wand_caster_analyst_excludes_attack_damage_wand_without_caster_core() -> None:
    result = analyze_family_candidate(
        family="wand_caster",
        segment={"base_type": "Imbued Wand", "mod_tokens": ["AddedAttackDamage"]},
        metrics={"sample_count": 10},
        opportunity={"listed_price": 10.0, "reference_price": 20.0},
        model={"mae": 5.0},
    )

    assert result.decision == "exclude"
    assert "attack_damage_wand_without_caster_core" in result.risks


def test_body_armour_analyst_flags_ilvl86_suppression_base() -> None:
    result = analyze_family_candidate(
        family="body_armour_defense",
        segment={
            "base_type": "Saint's Hauberk",
            "ilvl": 86,
            "mod_tokens": ["SpellSuppress", "Life"],
            "open_prefixes": 1,
        },
        metrics={"sample_count": 15},
        opportunity={"listed_price": 100.0, "reference_price": 180.0},
        model={"mae": 25.0},
    )

    assert result.analyst == "BodyArmourDefenseAnalyst"
    assert result.decision == "valid_for_manual_review"
    assert "scarce_ilvl_86_base" in result.reasons
    assert "spell_suppression" in result.reasons


def test_body_armour_analyst_excludes_low_ilvl_without_defensive_core() -> None:
    result = analyze_family_candidate(
        family="body_armour_defense",
        segment={"base_type": "Full Plate", "ilvl": 80, "mod_tokens": ["StunRecovery"]},
        metrics={"sample_count": 10},
        opportunity={"listed_price": 4.0, "reference_price": 6.0},
        model={"mae": 2.0},
    )

    assert result.decision == "exclude"
    assert "low_ilvl_without_defensive_core" in result.risks
