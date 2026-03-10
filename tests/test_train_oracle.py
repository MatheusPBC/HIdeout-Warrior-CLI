import os
import numpy as np
import pandas as pd
import pytest

from scripts.train_oracle import (
    append_jsonl_records,
    allocate_items_across_price_buckets,
    calculate_feature_overlap,
    calculate_rmse_by_bucket,
    collect_training_data_incremental,
    drop_leakage_columns,
    invert_target_log1p,
    load_checkpoint,
    load_dataset_for_training,
    load_jsonl_records,
    save_checkpoint,
    sample_result_ids_stratified,
    split_dataset_for_training,
    train_xgboost_oracle,
    transform_target_log1p,
    validate_dataset_distribution_or_raise,
    validate_training_schema,
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


def test_sample_result_ids_stratified_spreads_across_rank_deterministically() -> None:
    result_ids = [f"id_{i}" for i in range(100)]

    sample_a = sample_result_ids_stratified(result_ids, sample_size=20, n_strata=10)
    sample_b = sample_result_ids_stratified(result_ids, sample_size=20, n_strata=10)

    assert sample_a == sample_b
    assert len(sample_a) == 20
    assert sample_a == sorted(sample_a, key=lambda item_id: int(item_id.split("_")[1]))

    sampled_positions = [int(item_id.split("_")[1]) for item_id in sample_a]
    assert min(sampled_positions) <= 1
    assert max(sampled_positions) >= 98


def test_overlap_is_computed_after_leakage_drop_and_schema_is_validated() -> None:
    raw_train = pd.DataFrame(
        {
            "listed_at": ["2024-01-01T00:00:00Z"],
            "base_type": ["Imbued Wand"],
            "is_influenced": [1],
            "ilvl": [84],
            "tier_life": [1],
        }
    )
    raw_test = pd.DataFrame(
        {
            "listed_at": ["2024-01-02T00:00:00Z"],
            "base_type": ["Vaal Regalia"],
            "is_influenced": [1],
            "ilvl": [84],
            "tier_life": [1],
        }
    )

    leakage_columns = ["listed_at", "base_type"]
    train_x = drop_leakage_columns(raw_train, leakage_columns)
    test_x = drop_leakage_columns(raw_test, leakage_columns)

    validate_training_schema(train_x, test_x, forbidden_columns=leakage_columns)
    assert calculate_feature_overlap(train_x, test_x) == 1


def test_target_log1p_transform_roundtrip() -> None:
    y = pd.Series([0.0, 1.0, 10.0, 250.0])

    transformed = transform_target_log1p(y)
    restored = invert_target_log1p(transformed.to_numpy())

    assert np.allclose(restored, y.to_numpy(), rtol=1e-9, atol=1e-9)


def test_allocate_items_across_price_buckets_is_deterministic() -> None:
    allocation = allocate_items_across_price_buckets(items_per_base=11, bucket_count=4)
    assert allocation == [3, 3, 3, 2]
    assert sum(allocation) == 11


def test_validate_dataset_distribution_or_raise_detects_degenerate_dataset() -> None:
    df = pd.DataFrame(
        {
            "price_chaos": [10.0, 10.0, 10.0, 10.0, 10.0, 10.0],
        }
    )

    with pytest.raises(ValueError, match="Dataset degenerado"):
        validate_dataset_distribution_or_raise(df)


def test_validate_dataset_distribution_or_raise_accepts_non_degenerate_dataset() -> (
    None
):
    df = pd.DataFrame(
        {
            "price_chaos": [10.0, 25.0, 55.0, 90.0, 180.0, 260.0],
        }
    )

    summary = validate_dataset_distribution_or_raise(df)
    assert summary["nunique"] == 6
    assert summary["bucket_counts"]["<=50"] == 2
    assert summary["bucket_counts"]["50-150"] == 2
    assert summary["bucket_counts"][">150"] == 2


def test_incremental_jsonl_and_dedup_loading(tmp_path) -> None:
    dataset_path = tmp_path / "dataset.jsonl"
    append_jsonl_records(
        str(dataset_path),
        [
            {"item_id": "a", "price_chaos": 10, "base_type": "Imbued Wand"},
            {"item_id": "a", "price_chaos": 10, "base_type": "Imbued Wand"},
            {"item_id": "b", "price_chaos": 25, "base_type": "Spine Bow"},
        ],
    )

    loaded = load_jsonl_records(str(dataset_path))
    assert len(loaded) == 3

    dedup_df = load_dataset_for_training(
        dataset_path=str(dataset_path),
        apply_outlier_filter=False,
        apply_stale_filter=False,
    )
    assert len(dedup_df) == 2
    assert set(dedup_df["item_id"].tolist()) == {"a", "b"}


def test_checkpoint_roundtrip(tmp_path) -> None:
    checkpoint_path = tmp_path / "checkpoint.json"
    payload = {
        "base_index": 2,
        "bucket_index": 1,
        "batch_offset": 20,
        "base_seen_ids": ["x", "y"],
    }
    save_checkpoint(str(checkpoint_path), payload)
    restored = load_checkpoint(str(checkpoint_path))
    assert restored == payload


def test_collect_incremental_resume_avoids_refetching_known_items(
    tmp_path, monkeypatch
) -> None:
    dataset_path = tmp_path / "dataset.jsonl"
    checkpoint_path = tmp_path / "checkpoint.json"

    append_jsonl_records(
        str(dataset_path),
        [{"item_id": "known-1", "price_chaos": 12, "base_type": "Imbued Wand"}],
    )
    save_checkpoint(
        str(checkpoint_path),
        {
            "base_index": 0,
            "bucket_index": 0,
            "batch_offset": 0,
            "base_seen_ids": [],
        },
    )

    class DummyMeta:
        scores = {}

    class DummyAnalyzer:
        def __init__(self, league: str):
            self.league = league

        def fetch_meta_weights(self, force_refresh: bool = False):
            return DummyMeta()

    class DummyClient:
        def __init__(self, league: str):
            self.league = league

        def sync_ninja_economy(self):
            return {}

        def search_items(self, query):
            return "q1", ["new-1", "known-1"]

        def fetch_item_details(self, item_ids, query_id):
            return [
                {
                    "id": item_id,
                    "listing": {
                        "price": {"currency": "chaos", "amount": 20},
                        "indexed": "2024-01-01T00:00:00Z",
                    },
                    "item": {"ilvl": 84, "baseType": "Imbued Wand", "explicitMods": []},
                }
                for item_id in item_ids
            ]

    monkeypatch.setattr("scripts.train_oracle.MarketAPIClient", DummyClient)
    monkeypatch.setattr("scripts.train_oracle.LadderAnalyzer", DummyAnalyzer)

    stats = collect_training_data_incremental(
        target_bases=["Imbued Wand"],
        items_per_base=2,
        league="Standard",
        dataset_path=str(dataset_path),
        checkpoint_path=str(checkpoint_path),
        resume=True,
        chunk_size=1,
    )

    rows = load_jsonl_records(str(dataset_path))
    ids = [row.get("item_id") for row in rows]
    assert stats["seen_total"] >= 2
    assert ids.count("known-1") == 1
    assert "new-1" in ids
    assert not os.path.exists(checkpoint_path)


def test_train_offline_uses_dataset_without_trade_api(tmp_path, monkeypatch) -> None:
    dataset_path = tmp_path / "offline.jsonl"
    prices = [10, 25, 55, 90, 180, 260]
    for i in range(1, 61):
        price = prices[(i - 1) % len(prices)]
        append_jsonl_records(
            str(dataset_path),
            [
                {
                    "item_id": f"id-{i}",
                    "is_influenced": 0,
                    "ilvl": 80 + i,
                    "base_type": "Imbued Wand",
                    "tier_life": 1,
                    "tier_speed": 0,
                    "tier_resist": 0,
                    "tier_crit": 0,
                    "open_affixes": 2,
                    "listed_at": f"2024-01-{(i % 28) + 1:02d}T00:00:00Z",
                    "meta_utility_score": 0.1 * i,
                    "price_chaos": float(price),
                }
            ],
        )

    class DummyModel:
        def predict(self, dmatrix):
            labels = dmatrix.get_label()
            return labels

        def get_score(self, importance_type="gain"):
            return {}

        def set_attr(self, **kwargs):
            return None

        def save_model(self, path):
            with open(path, "w", encoding="utf-8") as handle:
                handle.write("dummy")

    def fake_train(
        params, dtrain, num_boost_round, evals, early_stopping_rounds, verbose_eval
    ):
        return DummyModel()

    def fail_if_called(*args, **kwargs):
        raise AssertionError("Trade API should not be called in offline train mode")

    monkeypatch.setattr("scripts.train_oracle.xgb.train", fake_train)
    monkeypatch.setattr("scripts.train_oracle.fetch_training_data", fail_if_called)

    train_xgboost_oracle(
        league="Standard",
        items_per_base=10,
        dataset_path=str(dataset_path),
    )

    assert os.path.exists("data/price_oracle.xgb")
