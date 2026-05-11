import json

import pandas as pd
from typer.testing import CliRunner

from scripts.build_market_intelligence import app


def test_build_market_intelligence_writes_ranked_snapshot(tmp_path) -> None:
    gold_path = tmp_path / "gold.parquet"
    output_path = tmp_path / "market_intelligence.json"
    pd.DataFrame(
        [
            {
                "item_id": "cheap_wand",
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
                "item_id": "reference_wand",
                "league": "Mirage",
                "item_family": "wand_caster",
                "base_type": "Imbued Wand",
                "ilvl_band": "high",
                "price_chaos": 130.0,
                "tag_tokens": ["caster", "spell"],
                "mod_tokens": ["SpellDamage", "CastSpeed"],
                "freshness_band": "active",
            },
            {
                "league": "Mirage",
                "item_family": "generic",
                "base_type": "Unknown Base",
                "ilvl_band": "low",
                "price_chaos": 5.0,
                "tag_tokens": [],
                "mod_tokens": [],
                "freshness_band": "stale",
            },
        ]
    ).to_parquet(gold_path, index=False)

    result = CliRunner().invoke(
        app,
        [
            "--gold-path",
            str(gold_path),
            "--output",
            str(output_path),
            "--top",
            "5",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["risk_profile"] == "balanced"
    assert payload["segments"]
    assert payload["top_segments"]
    assert payload["top_segments"][0]["score"]["market_score"] >= payload["top_segments"][-1]["score"]["market_score"]
    assert "explanation" in payload["top_segments"][0]["score"]
    assert payload["top_segments"][0]["opportunities"]
    assert payload["top_segments"][0]["opportunities"][0]["mode"] == "evaluation"


def test_build_market_intelligence_filters_by_league(tmp_path) -> None:
    gold_path = tmp_path / "gold.parquet"
    output_path = tmp_path / "market_intelligence.json"
    pd.DataFrame(
        [
            {
                "event_key": "standard-ring",
                "league": "Standard",
                "item_family": "accessory_generic",
                "base_type": "Diamond Ring",
                "ilvl_band": "high",
                "price_chaos": 20.0,
                "has_life": 1.0,
                "has_resist": 1.0,
                "freshness_band": "fresh",
            },
            {
                "event_key": "mirage-ring",
                "league": "Mirage",
                "item_family": "accessory_generic",
                "base_type": "Diamond Ring",
                "ilvl_band": "high",
                "price_chaos": 40.0,
                "has_life": 1.0,
                "has_resist": 1.0,
                "freshness_band": "active",
            },
        ]
    ).to_parquet(gold_path, index=False)

    result = CliRunner().invoke(
        app,
        [
            "--gold-path",
            str(gold_path),
            "--output",
            str(output_path),
            "--league",
            "Standard",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["league"] == "Standard"
    assert {segment["segment"]["league"] for segment in payload["segments"]} == {"Standard"}
