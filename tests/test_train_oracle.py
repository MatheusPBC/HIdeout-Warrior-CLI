import numpy as np
import pandas as pd
import pytest
import sqlite3
import json
from pathlib import Path

from scripts.train_oracle import (
    ILVL_BANDS,
    _train_family_band_models,
    classify_ilvl_band,
    train_xgboost_oracle,
    calculate_feature_overlap,
    calculate_rmse_by_bucket,
    fetch_training_data_from_parquet,
    fetch_training_data_from_sqlite,
    load_training_dataframe,
    persist_model_metadata,
    run_quality_gates,
    split_dataset_for_training,
    TrainingGateError,
)


def test_classify_ilvl_band_boundaries() -> None:
    assert classify_ilvl_band(60) == "low"
    assert classify_ilvl_band(74) == "low"
    assert classify_ilvl_band(75) == "mid"
    assert classify_ilvl_band(83) == "mid"
    assert classify_ilvl_band(84) == "high"


def test_train_family_band_models_trains_when_band_has_minimum_rows(
    monkeypatch, tmp_path
) -> None:
    family_df = pd.DataFrame(
        {
            "item_family": ["wand_caster"] * 50,
            "ilvl": [70] * 10 + [78] * 20 + [86] * 20,
            "price_chaos": np.linspace(10.0, 200.0, 50),
            "has_spell_damage": [1.0] * 50,
            "has_cast_speed": [1.0] * 50,
            "has_spell_crit": [0.0] * 50,
            "open_affixes": [2.0] * 50,
            "is_influenced": [0.0] * 50,
            "mod_count": [2.0] * 50,
            "meta_utility_score": [0.0] * 50,
        }
    )

    def _fake_train_family_model(
        family,
        subset_df,
        output_dir,
        model_suffix="",
        report_context=None,
    ):
        payload = {
            "family": family,
            "rows_total": int(len(subset_df)),
            "model_path": str(output_dir / f"price_oracle_{family}{model_suffix}.xgb"),
            "metrics": {"rmse": 1.0, "mae": 1.0},
        }
        if report_context:
            payload.update(report_context)
        return payload

    monkeypatch.setattr(
        "scripts.train_oracle._train_family_model", _fake_train_family_model
    )

    reports = _train_family_band_models(
        family="wand_caster",
        family_df=family_df,
        output_dir=tmp_path,
        min_rows_per_band=20,
    )

    by_band = {report["ilvl_band"]: report for report in reports}
    assert set(by_band.keys()) == set(ILVL_BANDS.keys())
    assert by_band["low"]["fallback_to_family"] is True
    assert by_band["mid"]["trained"] is True
    assert by_band["high"]["trained"] is True


def test_split_dataset_for_training_uses_temporal_order_without_inversion() -> None:
    timestamps = pd.date_range("2024-01-01T00:00:00Z", periods=10, freq="h")
    df = pd.DataFrame(
        {
            "is_influenced": [0, 1] * 5,
            "ilvl": list(range(70, 80)),
            "base_type": ["Imbued Wand"] * 10,
            "tier_life": [1] * 10,
            "tier_speed": [0] * 10,
            "tier_resist": [0] * 10,
            "tier_crit": [0] * 10,
            "open_affixes": [2] * 10,
            "listed_at": [ts.isoformat().replace("+00:00", "Z") for ts in timestamps],
            "meta_utility_score": np.linspace(0.1, 1.0, 10),
            "price_chaos": np.linspace(10.0, 100.0, 10),
        }
    )

    x_train, x_test, y_train, y_test, strategy = split_dataset_for_training(df)

    assert strategy == "temporal"
    assert len(x_train) == 8
    assert len(x_test) == 2
    assert len(y_train) == 8
    assert len(y_test) == 2

    train_max_ts = pd.to_datetime(x_train["listed_at"], utc=True).max()
    test_min_ts = pd.to_datetime(x_test["listed_at"], utc=True).min()
    assert train_max_ts <= test_min_ts
    assert calculate_feature_overlap(x_train, x_test) == 0


def test_calculate_rmse_by_bucket_with_synthetic_data() -> None:
    y_true = pd.Series([20.0, 45.0, 80.0, 120.0, 180.0, 220.0])
    y_pred = np.array([25.0, 35.0, 70.0, 150.0, 210.0, 200.0])

    result = calculate_rmse_by_bucket(y_true=y_true, y_pred=y_pred)

    assert result["<=50"] == pytest.approx(7.9056941504, rel=1e-6)
    assert result["50-150"] == pytest.approx(22.360679775, rel=1e-6)
    assert result[">150"] == pytest.approx(25.4950975679, rel=1e-6)


def test_fetch_training_data_from_sqlite_builds_features(tmp_path) -> None:
    db_path = tmp_path / "firehose.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE stash_events (
            raw_item_json TEXT,
            price_chaos REAL,
            price_currency TEXT,
            price_amount REAL,
            account_name TEXT,
            indexed TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE trade_bucket_events (
            raw_item_json TEXT,
            price_chaos REAL,
            price_currency TEXT,
            price_amount REAL,
            account_name TEXT,
            indexed TEXT
        )
        """
    )
    raw_item = {
        "id": "item-1",
        "baseType": "Imbued Wand",
        "ilvl": 84,
        "explicitMods": ["+#% increased Spell Damage", "+#% increased Cast Speed"],
        "implicitMods": [],
        "influences": {},
        "corrupted": False,
        "fractured": False,
    }
    conn.execute(
        "INSERT INTO stash_events VALUES (?, ?, ?, ?, ?, ?)",
        (
            pd.Series(raw_item).to_json(),
            40.0,
            "chaos",
            40.0,
            "seller",
            "2026-03-11T10:00:00Z",
        ),
    )
    conn.execute(
        "INSERT INTO trade_bucket_events VALUES (?, ?, ?, ?, ?, ?)",
        (
            pd.Series(
                {
                    "id": "item-2",
                    "baseType": "Opal Ring",
                    "ilvl": 85,
                    "explicitMods": ["+# to maximum Life"],
                    "implicitMods": [],
                    "influences": {},
                    "corrupted": False,
                    "fractured": False,
                }
            ).to_json(),
            25.0,
            "chaos",
            25.0,
            "seller-2",
            "2026-03-11T10:03:00Z",
        ),
    )
    conn.commit()
    conn.close()

    df = fetch_training_data_from_sqlite(
        str(db_path), apply_outlier_filter=False, apply_stale_filter=False
    )

    assert not df.empty
    assert "price_chaos" in df.columns
    assert "item_family" in df.columns
    assert len(df) >= 2


def test_load_training_dataframe_keeps_api_default_flow(monkeypatch) -> None:
    expected = pd.DataFrame([{"item_family": "generic", "price_chaos": 10.0}])

    def _fake_fetch_training_data(*_args, **_kwargs):
        return expected

    monkeypatch.setattr(
        "scripts.train_oracle.fetch_training_data", _fake_fetch_training_data
    )
    result = load_training_dataframe(
        source="api",
        league="Standard",
        items_per_base=10,
        target_bases=["Imbued Wand"],
        sqlite_path="data/firehose.db",
        parquet_path="data/firehose.parquet",
    )

    assert result.equals(expected)


def test_fetch_training_data_from_parquet_accepts_partitioned_directory(
    tmp_path, monkeypatch
) -> None:
    parquet_dir = tmp_path / "gold"
    parquet_dir.mkdir(parents=True, exist_ok=True)

    expected = pd.DataFrame(
        [
            {
                "ilvl": 84,
                "has_life": 0.0,
                "has_resist": 1.0,
                "has_crit": 0.0,
                "mod_count": 2.0,
                "open_affixes": 4.0,
                "meta_utility_score": 0.0,
                "base_type": "Opal Ring",
                "listed_at": "2026-03-11T10:00:00Z",
                "item_family": "accessory_generic",
                "price_chaos": 25.0,
            }
        ]
    )
    captured_path = {"value": ""}

    def _fake_read_parquet(path):
        captured_path["value"] = str(path)
        return expected.copy()

    monkeypatch.setattr("scripts.train_oracle.pd.read_parquet", _fake_read_parquet)

    result = fetch_training_data_from_parquet(
        str(parquet_dir), apply_outlier_filter=False, apply_stale_filter=False
    )

    assert captured_path["value"] == str(parquet_dir)
    assert result.equals(expected)


def test_run_quality_gates_fails_with_invalid_dataset() -> None:
    df = pd.DataFrame(
        {
            "item_family": ["generic"] * 50,
            "price_chaos": [10.0] * 50,
        }
    )

    with pytest.raises(TrainingGateError):
        run_quality_gates(df)


def test_run_quality_gates_passes_with_valid_dataset() -> None:
    rows = 50
    df = pd.DataFrame(
        {
            "item_family": ["wand_caster"] * 25 + ["generic"] * 25,
            "price_chaos": np.linspace(10.0, 120.0, rows),
            "ilvl": np.arange(rows),
            "has_life": [0.0, 1.0] * 25,
        }
    )

    report = run_quality_gates(df)

    assert report["rows"] == 50
    assert report["target_unique"] >= 10
    assert report["max_family_rows"] >= 20


def test_persist_model_metadata_writes_expected_structure(tmp_path) -> None:
    df = pd.DataFrame(
        {
            "snapshot_date": ["2026-03-11"] * 3,
            "item_family": ["wand_caster", "wand_caster", "generic"],
            "price_chaos": [12.0, 18.0, 22.0],
        }
    )
    audit = {"rows": 3, "exact_duplicates": 0}
    reports = [
        {
            "family": "wand_caster",
            "model_path": "data/price_oracle_wand_caster.xgb",
            "model_sha256": "abc123",
            "feature_schema": ["ilvl", "has_spell_damage"],
            "feature_schema_hash": "def456",
            "split_strategy": "temporal",
            "rows_total": 30,
            "rows_train": 24,
            "rows_test": 6,
            "metrics": {"rmse": 10.1, "mae": 7.2},
        }
    ]

    metadata_path = persist_model_metadata(
        source="parquet",
        league="Standard",
        items_per_base=500,
        trained_at_utc="2026-03-11T12:00:00Z",
        dataset_df=df,
        dataset_audit=audit,
        model_reports=reports,
        output_dir=tmp_path / "model_metadata",
    )

    payload = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert metadata_path.exists()
    assert payload["run"]["source"] == "parquet"
    assert payload["dataset"]["rows"] == 3
    assert payload["dataset"]["snapshot_date"] == "2026-03-11"
    assert "dataset_hash" in payload["dataset"]
    assert payload["models"][0]["family"] == "wand_caster"
    assert "model_sha256" in payload["models"][0]


def test_train_registers_registry_decision_in_metadata(monkeypatch) -> None:
    dataset = pd.DataFrame(
        {
            "item_family": ["generic"] * 30,
            "price_chaos": np.linspace(10.0, 40.0, 30),
            "ilvl": np.arange(30),
            "has_life": [0.0, 1.0] * 15,
        }
    )
    captured = {"models": []}
    captured_registry_kwargs = {}

    monkeypatch.setattr("scripts.train_oracle.ITEM_FAMILIES", ["generic"])
    monkeypatch.setattr(
        "scripts.train_oracle.load_training_dataframe", lambda **_: dataset
    )
    monkeypatch.setattr(
        "scripts.train_oracle.run_quality_gates",
        lambda _df: {
            "duplicate_ratio": 0.0,
            "target_unique": 20,
            "target_std": 1.0,
            "max_family_rows": 30,
            "audit": {"rows": 30, "exact_duplicates": 0},
        },
    )
    monkeypatch.setattr(
        "scripts.train_oracle._train_family_model",
        lambda *_args, **_kwargs: {
            "family": "generic",
            "rows_total": 30,
            "rows_train": 24,
            "rows_test": 6,
            "feature_schema": ["ilvl", "has_life"],
            "feature_schema_hash": "schema-hash",
            "split_strategy": "random",
            "metrics": {"rmse": 5.0, "mae": 4.0, "baseline_rmse": 7.0},
            "model_path": "data/price_oracle_generic.xgb",
            "model_sha256": "model-hash",
        },
    )

    def _fake_register_and_evaluate_candidate(**kwargs):
        captured_registry_kwargs.update(kwargs)
        return {
            "family": "generic",
            "status": "active",
            "promoted": True,
            "reason": "promotion_policy_satisfied",
            "decision_reason": "promotion_policy_satisfied",
            "policy": {
                "max_rmse_ratio": kwargs.get("max_rmse_ratio"),
                "min_abs_improvement": kwargs.get("min_abs_improvement"),
            },
        }

    monkeypatch.setattr(
        "scripts.train_oracle.register_and_evaluate_candidate",
        _fake_register_and_evaluate_candidate,
    )

    def _capture_metadata(**kwargs):
        captured["models"] = kwargs["model_reports"]
        return Path("data/model_metadata/fake.json")

    monkeypatch.setattr(
        "scripts.train_oracle.persist_model_metadata", _capture_metadata
    )

    train_xgboost_oracle(
        source="api",
        promotion_max_rmse_ratio=0.95,
        promotion_min_abs_improvement=0.2,
        registry_path="custom/registry.json",
    )

    assert captured["models"]
    assert captured["models"][0]["registry_decision"]["status"] == "active"
    assert captured_registry_kwargs["max_rmse_ratio"] == 0.95
    assert captured_registry_kwargs["min_abs_improvement"] == 0.2
    assert str(captured_registry_kwargs["registry_path"]) == "custom/registry.json"
