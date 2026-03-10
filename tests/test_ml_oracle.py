import sys
import types
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import numpy as np
import pytest

from core.ml_oracle import PRICE_ORACLE_FEATURE_SCHEMA, PricePredictor


@dataclass(frozen=True)
class DummyItemState:
    base_type: str
    ilvl: int
    prefixes: frozenset[str]
    suffixes: frozenset[str]
    is_fractured: bool = False

    @property
    def open_prefixes(self) -> int:
        return 3 - len(self.prefixes)

    @property
    def open_suffixes(self) -> int:
        return 3 - len(self.suffixes)


def test_price_oracle_feature_schema_and_inference_frame_columns(monkeypatch) -> None:
    expected_schema = (
        "is_influenced",
        "ilvl",
        "tier_life",
        "tier_speed",
        "tier_resist",
        "tier_crit",
        "open_affixes",
        "meta_utility_score",
    )
    assert PRICE_ORACLE_FEATURE_SCHEMA == expected_schema

    monkeypatch.setattr(PricePredictor, "_load_xgboost", lambda self: None)
    predictor = PricePredictor()
    item = DummyItemState(
        base_type="Imbued Wand",
        ilvl=84,
        prefixes=frozenset({"Life1"}),
        suffixes=frozenset({"CastSpeed1"}),
    )

    frame = predictor._build_inference_dataframe(cast(Any, item))
    assert tuple(frame.columns) == expected_schema
    assert frame.shape == (1, len(expected_schema))


def test_model_load_path_failure_keeps_fallback_inference_working(
    monkeypatch, tmp_path
) -> None:
    model_file = tmp_path / "data" / "price_oracle.xgb"
    model_file.parent.mkdir(parents=True, exist_ok=True)
    model_file.write_text("corrupted-model")

    class BrokenBooster:
        def load_model(self, _path: str) -> None:
            raise RuntimeError("invalid model")

    fake_xgb = types.SimpleNamespace(Booster=BrokenBooster)
    monkeypatch.setitem(sys.modules, "xgboost", fake_xgb)
    monkeypatch.setattr(
        PricePredictor, "_resolve_model_path", lambda self: Path(model_file)
    )

    predictor = PricePredictor()
    assert predictor.model is None

    item = DummyItemState(
        base_type="Imbued Wand",
        ilvl=84,
        prefixes=frozenset({"SpellDamage1"}),
        suffixes=frozenset(),
    )
    price, confidence = predictor.predict_value(cast(Any, item))

    assert price >= 10.0
    assert 0.3 <= confidence <= 1.0


def test_predict_value_applies_expm1_when_model_has_log1p_metadata(monkeypatch) -> None:
    class FakeBooster:
        def predict(self, _dmatrix):
            return np.array([np.log1p(123.4)])

        def attr(self, key: str):
            if key == "target_transform":
                return "log1p"
            return None

    class FakeDMatrix:
        def __init__(self, _frame):
            pass

    fake_xgb = types.SimpleNamespace(DMatrix=FakeDMatrix)
    monkeypatch.setitem(sys.modules, "xgboost", fake_xgb)
    monkeypatch.setattr(PricePredictor, "_load_xgboost", lambda self: None)

    predictor = PricePredictor()
    setattr(predictor, "model", cast(Any, FakeBooster()))

    item = DummyItemState(
        base_type="Imbued Wand",
        ilvl=84,
        prefixes=frozenset({"Life1"}),
        suffixes=frozenset({"CastSpeed1"}),
    )

    price, confidence = predictor.predict_value(cast(Any, item))

    assert price == pytest.approx(123.4, rel=1e-9)
    assert 0.3 <= confidence <= 1.0
