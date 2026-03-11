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
    assert normalized.prefix_count + normalized.suffix_count <= len(normalized.explicit_mods)
    assert normalized.open_prefixes >= 0
    assert normalized.open_suffixes >= 0
    assert "SpellDamage1" in normalized.mod_tokens
    assert normalized.to_item_state().base_type == "Imbued Wand"


def test_classify_item_family_prefers_accessory_and_jewel_groups():
    assert classify_item_family("Opal Ring", ["accessory", "resistance"]) == "accessory_generic"
    assert classify_item_family("Large Cluster Jewel", ["jewel"]) == "jewel_cluster"
