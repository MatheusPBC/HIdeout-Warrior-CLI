import json

from typer.testing import CliRunner

from cli import app


def test_market_dashboard_outputs_json_snapshot(tmp_path) -> None:
    snapshot = tmp_path / "market_intelligence.json"
    snapshot.write_text(
        json.dumps(
            {
                "generated_at": "2026-05-11T00:00:00Z",
                "risk_profile": "balanced",
                "top_segments": [
                    {
                        "segment": {
                            "key": "Mirage|wand_caster|Imbued Wand|high|51-150|CastSpeed+SpellDamage|caster+spell",
                            "league": "Mirage",
                            "item_family": "wand_caster",
                            "base_type": "Imbued Wand",
                            "ilvl_band": "high",
                            "price_band": "51-150",
                            "mod_signature": "CastSpeed+SpellDamage",
                            "tag_signature": "caster+spell",
                        },
                        "metrics": {
                            "sample_count": 42,
                            "fresh_ratio": 0.66,
                            "stale_ratio": 0.05,
                            "price_floor": 70.0,
                            "price_median": 120.0,
                            "price_spread": 0.41,
                            "volume_score": 0.84,
                            "liquidity_score": 0.72,
                            "safety_score": 0.80,
                            "margin_score": 0.65,
                            "trend_score": 0.58,
                        },
                        "score": {
                            "market_score": 0.713,
                            "status": "strong_candidate",
                            "explanation": "strong_candidate: safe market, liquid market",
                        },
                        "opportunities": [
                            {
                                "item_id": "cheap_wand",
                                "base_type": "Imbued Wand",
                                "listed_price": 80.0,
                                "reference_price": 110.0,
                                "estimated_upside": 0.375,
                                "freshness_band": "fresh",
                                "mode": "evaluation",
                            }
                        ],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        app,
        ["market-dashboard", "--snapshot", str(snapshot), "--format", "json"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["top_segments"][0]["score"]["status"] == "strong_candidate"
    assert payload["top_segments"][0]["opportunities"][0]["mode"] == "evaluation"


def test_market_dashboard_table_shows_evaluation_opportunity(tmp_path) -> None:
    snapshot = tmp_path / "market_intelligence.json"
    snapshot.write_text(
        json.dumps(
            {
                "generated_at": "2026-05-11T00:00:00Z",
                "risk_profile": "balanced",
                "top_segments": [
                    {
                        "segment": {
                            "item_family": "wand_caster",
                            "base_type": "Imbued Wand",
                            "price_band": "51-150",
                        },
                        "metrics": {
                            "safety_score": 0.80,
                            "liquidity_score": 0.72,
                            "margin_score": 0.65,
                            "trend_score": 0.58,
                        },
                        "score": {
                            "market_score": 0.713,
                            "status": "strong_candidate",
                            "explanation": "strong_candidate: safe market, liquid market",
                        },
                        "opportunities": [
                            {
                                "item_id": "cheap_wand",
                                "base_type": "Imbued Wand",
                                "listed_price": 80.0,
                                "reference_price": 110.0,
                                "estimated_upside": 0.375,
                                "mode": "evaluation",
                            }
                        ],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    result = CliRunner().invoke(app, ["market-dashboard", "--snapshot", str(snapshot)])

    assert result.exit_code == 0
    assert "cheap_wand" in result.output
    assert "evaluation" in result.output


def test_market_dashboard_fails_clearly_when_snapshot_is_missing(tmp_path) -> None:
    missing_snapshot = tmp_path / "missing.json"

    result = CliRunner().invoke(
        app,
        ["market-dashboard", "--snapshot", str(missing_snapshot)],
    )

    assert result.exit_code != 0
    assert "Snapshot not found" in result.output
