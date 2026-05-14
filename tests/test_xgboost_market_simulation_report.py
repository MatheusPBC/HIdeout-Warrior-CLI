import json

import pandas as pd

from scripts.xgboost_market_simulation_report import build_simulation_report


def test_build_simulation_report_saves_ranked_json_and_log(tmp_path) -> None:
    metadata_path = tmp_path / "data" / "model_metadata" / "oracle_training_20260511.json"
    registry_path = tmp_path / "data" / "model_registry" / "registry.json"
    latest_path = tmp_path / "data" / "market_intelligence" / "latest.json"
    gold_path = tmp_path / "data" / "training_snapshots" / "gold"
    logs_dir = tmp_path / "logs"
    metadata_path.parent.mkdir(parents=True)
    registry_path.parent.mkdir(parents=True)
    latest_path.parent.mkdir(parents=True)
    gold_path.mkdir(parents=True)

    metadata_path.write_text(
        json.dumps(
            {
                "models": [
                    {
                        "family": "jewel_cluster",
                        "rows_total": 120,
                        "model_path": "data/price_oracle_jewel_cluster.xgb",
                        "metrics": {
                            "rmse": 20.0,
                            "mae": 12.0,
                            "baseline_rmse": 40.0,
                        },
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    registry_path.write_text(
        json.dumps({"families": {"jewel_cluster": {"active_version": "run-1"}}}),
        encoding="utf-8",
    )
    latest_path.write_text(
        json.dumps(
            {
                "segments": [
                    {
                        "segment": {"item_family": "jewel_cluster", "base_type": "Crimson Jewel"},
                        "score": {"status": "evaluation_candidate", "market_score": 0.55},
                        "metrics": {"sample_count": 4},
                        "opportunities": [
                            {
                                "item_id": "candidate-1",
                                "listed_price": 20.0,
                                "reference_price": 60.0,
                                "estimated_upside": 2.0,
                            }
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    pd.DataFrame([{"league": "Mirage", "price_chaos": 20.0}]).to_parquet(gold_path / "part.parquet")

    result = build_simulation_report(
        metadata_path=metadata_path,
        registry_path=registry_path,
        latest_path=latest_path,
        gold_path=gold_path,
        logs_dir=logs_dir,
        league="Mirage",
        timestamp="20260511T000000Z",
    )

    payload = json.loads(result.json_path.read_text(encoding="utf-8"))
    log_text = result.log_path.read_text(encoding="utf-8")
    assert payload["candidate_counts"] == {"valid_for_manual_review": 1}
    assert payload["top_candidates"][0]["item_id"] == "candidate-1"
    assert payload["top_candidates"][0]["confidence_margin_after_mae"] == 28.0
    assert payload["top_candidates"][0]["family_analyst"] == "JewelClusterAnalyst"
    assert "missing_cluster_evidence" in payload["top_candidates"][0]["analysis_risks"]
    assert "valid_for_manual_review | candidate-1" in log_text


def test_build_simulation_report_excludes_commodity_families_from_manual_review(tmp_path) -> None:
    metadata_path = tmp_path / "data" / "model_metadata" / "oracle_training_20260511.json"
    registry_path = tmp_path / "data" / "model_registry" / "registry.json"
    latest_path = tmp_path / "data" / "market_intelligence" / "latest.json"
    gold_path = tmp_path / "data" / "training_snapshots" / "gold"
    logs_dir = tmp_path / "logs"
    metadata_path.parent.mkdir(parents=True)
    registry_path.parent.mkdir(parents=True)
    latest_path.parent.mkdir(parents=True)
    gold_path.mkdir(parents=True)

    metadata_path.write_text(
        json.dumps(
            {
                "models": [
                    {
                        "family": "map",
                        "rows_total": 100,
                        "model_path": "data/price_oracle_map.xgb",
                        "metrics": {"rmse": 5.0, "mae": 2.0, "baseline_rmse": 20.0},
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    registry_path.write_text(json.dumps({"families": {}}), encoding="utf-8")
    latest_path.write_text(
        json.dumps(
            {
                "segments": [
                    {
                        "segment": {"item_family": "map", "base_type": "Map (Tier 16)"},
                        "score": {"status": "strong_candidate", "market_score": 0.9},
                        "metrics": {"sample_count": 50},
                        "opportunities": [
                            {
                                "item_id": "map-1",
                                "listed_price": 1.0,
                                "reference_price": 20.0,
                                "estimated_upside": 19.0,
                            }
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    pd.DataFrame([{"league": "Mirage", "item_family": "map", "price_chaos": 1.0}]).to_parquet(gold_path / "part.parquet")

    result = build_simulation_report(
        metadata_path=metadata_path,
        registry_path=registry_path,
        latest_path=latest_path,
        gold_path=gold_path,
        logs_dir=logs_dir,
        league="Mirage",
        timestamp="20260511T010000Z",
    )

    payload = json.loads(result.json_path.read_text(encoding="utf-8"))
    assert payload["candidate_counts"] == {"skip_excluded_family": 1}
    assert payload["top_candidates"][0]["decision"] == "skip_excluded_family"
