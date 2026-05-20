import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.item_normalizer import classify_item_family, normalize_trade_item


def test_normalize_trade_item_builds_shared_contract_and_counts_affixes():
    raw = {
        "listing": {
            "whisper": "@seller hi",
            "indexed": "2026-03-11T10:00:00Z",
            "account": {"name": "seller"},
            "price": {"amount": 25.0, "currency": "chaos"},
        },
        "item": {
            "id": "wand-1",
            "baseType": "Imbued Wand",
            "ilvl": 84,
            "explicitMods": [
                "+#% increased Spell Damage",
                "+#% increased Cast Speed",
                "+#% to Global Critical Strike Chance for Spells",
            ],
            "implicitMods": [],
            "fractured": False,
            "corrupted": False,
            "influences": {},
        },
    }

    normalized = normalize_trade_item(
        raw,
        listed_price=25.0,
        listing_currency="chaos",
        listing_amount=25.0,
    )

    assert normalized is not None
    assert normalized.item_family == "wand_caster"
    assert normalized.prefix_count + normalized.suffix_count <= len(
        normalized.explicit_mods
    )
    assert normalized.open_prefixes >= 0
    assert normalized.open_suffixes >= 0
    assert "SpellDamage" in normalized.mod_tokens
    assert normalized.tier_source in {"fallback_numeric", "none"}
    assert normalized.to_item_state().base_type == "Imbued Wand"


def test_classify_item_family_prefers_accessory_and_jewel_groups():
    assert (
        classify_item_family("Opal Ring", ["accessory", "resistance"])
        == "accessory_generic"
    )
    assert classify_item_family("Large Cluster Jewel", ["jewel"]) == "jewel_cluster"
    assert classify_item_family("Cobalt Jewel", ["jewel"]) == "jewel_regular"


def test_classify_item_family_splits_market_commodity_noise():
    assert classify_item_family("Map (Tier 16)", []) == "map"
    assert classify_item_family("Blade Vortex", ["gem"]) == "gem"
    assert classify_item_family("Empower Support", []) == "gem"
    assert classify_item_family("Quicksilver Flask", []) == "flask"
    assert classify_item_family("Basalt Flask", []) == "flask"
    assert classify_item_family("Forbidden Tome", []) == "market_misc"
    assert classify_item_family("Inscribed Ultimatum", []) == "market_misc"
    assert classify_item_family("Provisioning Wombgift", []) == "market_misc"


def test_normalize_trade_item_extracts_cluster_jewel_evidence():
    raw = {
        "listing": {
            "whisper": "@seller hi",
            "indexed": "2026-05-14T10:00:00Z",
            "account": {"name": "seller"},
            "price": {"amount": 90.0, "currency": "chaos"},
        },
        "item": {
            "id": "cluster-1",
            "baseType": "Large Cluster Jewel",
            "ilvl": 84,
            "enchantMods": [
                "Adds 8 Passive Skills",
                "Added Small Passive Skills grant: 12% increased Minion Damage",
            ],
            "explicitMods": [
                "1 Added Passive Skill is Renewal",
                "1 Added Passive Skill is Feasting Fiends",
            ],
            "implicitMods": [],
        },
    }

    normalized = normalize_trade_item(
        raw,
        listed_price=90.0,
        listing_currency="chaos",
        listing_amount=90.0,
    )

    assert normalized is not None
    assert normalized.cluster_size == "large"
    assert normalized.cluster_passives == 8
    assert normalized.cluster_enchant == "Minion Damage"
    assert normalized.notables == ["Renewal", "Feasting Fiends"]


def test_normalize_trade_item_keeps_regular_jewels_out_of_cluster_evidence():
    raw = {
        "listing": {
            "whisper": "@seller hi",
            "indexed": "2026-05-14T10:00:00Z",
            "account": {"name": "seller"},
            "price": {"amount": 90.0, "currency": "chaos"},
        },
        "item": {
            "id": "jewel-1",
            "baseType": "Cobalt Jewel",
            "ilvl": 84,
            "enchantMods": [],
            "explicitMods": ["+#% increased Maximum Life"],
            "implicitMods": [],
        },
    }

    normalized = normalize_trade_item(
        raw,
        listed_price=90.0,
        listing_currency="chaos",
        listing_amount=90.0,
    )

    assert normalized is not None
    assert normalized.item_family == "jewel_regular"
    assert normalized.cluster_size == ""
    assert normalized.cluster_passives is None
    assert normalized.cluster_enchant == ""
    assert normalized.notables == []


def test_normalize_trade_item_cleans_multiline_cluster_enchant_and_socket_noise():
    raw = {
        "listing": {
            "whisper": "@seller hi",
            "indexed": "2026-05-14T10:00:00Z",
            "account": {"name": "seller"},
            "price": {"amount": 90.0, "currency": "chaos"},
        },
        "item": {
            "id": "cluster-2",
            "baseType": "Medium Cluster Jewel",
            "ilvl": 84,
            "enchantMods": [
                "Life Recovery from Flasks\nAdded Small Passive Skills grant: 10% increased Mana Recovery from Flasks",
                "Adds 4 Passive Skills",
            ],
            "explicitMods": [
                "1 Added Passive Skill is a Jewel Socket",
                "1 Added Passive Skill is Spiked Concoction",
            ],
            "implicitMods": [],
        },
    }

    normalized = normalize_trade_item(
        raw,
        listed_price=90.0,
        listing_currency="chaos",
        listing_amount=90.0,
    )

    assert normalized is not None
    assert normalized.cluster_size == "medium"
    assert normalized.cluster_passives == 4
    assert normalized.cluster_enchant == "Mana Recovery from Flasks"
    assert normalized.notables == ["Spiked Concoction"]


def test_normalize_trade_item_extracts_native_tier_metadata():
    raw = {
        "listing": {
            "whisper": "@seller hi",
            "indexed": "2026-03-11T10:00:00Z",
            "account": {"name": "seller"},
            "price": {"amount": 50.0, "currency": "chaos"},
        },
        "item": {
            "id": "wand-native-tier",
            "baseType": "Imbued Wand",
            "ilvl": 84,
            "explicitMods": ["40% increased Spell Damage"],
            "implicitMods": [],
            "extended": {
                "mods": {
                    "explicit": [
                        {
                            "name": "Spell Damage",
                            "tier": 1,
                            "magnitudes": [{"min": 35, "max": 42}],
                        }
                    ]
                }
            },
        },
    }

    normalized = normalize_trade_item(
        raw,
        listed_price=50.0,
        listing_currency="chaos",
        listing_amount=50.0,
    )

    assert normalized is not None
    assert normalized.tier_source == "native"
    assert normalized.native_tier_count >= 1
    assert "SpellDamage_T1" in normalized.mod_tokens
    assert normalized.tier_ilvl_mismatch is False


def test_normalize_trade_item_sets_twink_override_for_plus_one_all_spell_gems():
    raw = {
        "listing": {
            "whisper": "@seller hi",
            "indexed": "2026-03-11T10:00:00Z",
            "account": {"name": "seller"},
            "price": {"amount": 10.0, "currency": "chaos"},
        },
        "item": {
            "id": "twink-1",
            "baseType": "Driftwood Wand",
            "ilvl": 40,
            "explicitMods": ["+1 to Level of all Spell Skill Gems"],
            "implicitMods": [],
        },
    }

    normalized = normalize_trade_item(
        raw,
        listed_price=10.0,
        listing_currency="chaos",
        listing_amount=10.0,
    )

    assert normalized is not None
    assert normalized.twink_override is True
    assert normalized.numeric_mod_features["plus_all_spell_gems"] == 1.0


def test_normalize_trade_item_marks_tier_ilvl_mismatch_and_approx_token():
    raw = {
        "listing": {
            "whisper": "@seller hi",
            "indexed": "2026-03-11T10:00:00Z",
            "account": {"name": "seller"},
            "price": {"amount": 65.0, "currency": "chaos"},
        },
        "item": {
            "id": "wand-low-ilvl-tier",
            "baseType": "Imbued Wand",
            "ilvl": 70,
            "explicitMods": ["40% increased Spell Damage"],
            "implicitMods": [],
            "extended": {
                "mods": {
                    "explicit": [
                        {
                            "name": "Spell Damage",
                            "tier": 1,
                            "magnitudes": [{"min": 35, "max": 42}],
                        }
                    ]
                }
            },
            "fractured": False,
        },
    }

    normalized = normalize_trade_item(
        raw,
        listed_price=65.0,
        listing_currency="chaos",
        listing_amount=65.0,
    )

    assert normalized is not None
    assert normalized.tier_ilvl_mismatch is True
    assert "SpellDamage_T1_approx" in normalized.mod_tokens
    assert "SpellDamage_T1" not in normalized.mod_tokens
    assert normalized.low_ilvl_context is True


def test_normalize_trade_item_marks_fractured_low_ilvl_brick():
    raw = {
        "listing": {
            "whisper": "@seller hi",
            "indexed": "2026-03-11T10:00:00Z",
            "account": {"name": "seller"},
            "price": {"amount": 20.0, "currency": "chaos"},
        },
        "item": {
            "id": "fractured-low-ilvl",
            "baseType": "Opal Ring",
            "ilvl": 70,
            "explicitMods": ["+# to maximum Life"],
            "implicitMods": [],
            "fractured": True,
        },
    }

    normalized = normalize_trade_item(
        raw,
        listed_price=20.0,
        listing_currency="chaos",
        listing_amount=20.0,
    )

    assert normalized is not None
    assert normalized.low_ilvl_context is True
    assert normalized.fractured_low_ilvl_brick is True


def test_normalize_trade_item_extracts_native_tiers_from_nested_hashes_and_strings():
    raw = {
        "listing": {
            "whisper": "@seller hi",
            "indexed": "2026-03-11T10:00:00Z",
            "account": {"name": "seller"},
            "price": {"amount": 35.0, "currency": "chaos"},
        },
        "item": {
            "id": "wand-nested-native-tier",
            "baseType": "Imbued Wand",
            "ilvl": 84,
            "explicitMods": [
                "40% increased Spell Damage",
                "15% increased Cast Speed",
            ],
            "implicitMods": [],
            "extended": {
                "mods": {
                    "explicit": [
                        {
                            "name": "Spell Damage",
                            "tier": "Tier 1",
                        },
                        {
                            "name": "Cast Speed",
                            "tier": "P3",
                        },
                    ]
                },
                "hashes": {
                    "explicit": [
                        {
                            "hash": "spell_damage_hash",
                            "explicit": "Spell Damage",
                            "tier": "S2",
                        }
                    ]
                },
            },
        },
    }

    normalized = normalize_trade_item(
        raw,
        listed_price=35.0,
        listing_currency="chaos",
        listing_amount=35.0,
    )

    assert normalized is not None
    assert normalized.tier_source == "native"
    assert normalized.native_tier_count >= 3
    assert "SpellDamage_T1" in normalized.mod_tokens
    assert "CastSpeed_T3" in normalized.mod_tokens
