import numpy as np
import pandas as pd
import pytest
import sqlite3

from scripts.train_oracle import (
    calculate_feature_overlap,
    calculate_rmse_by_bucket,
    fetch_training_data_from_sqlite,
    load_training_dataframe,
    split_dataset_for_training,
)


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
    conn.commit()
    conn.close()

    df = fetch_training_data_from_sqlite(
        str(db_path), apply_outlier_filter=False, apply_stale_filter=False
    )

    assert not df.empty
    assert "price_chaos" in df.columns
    assert "item_family" in df.columns


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
