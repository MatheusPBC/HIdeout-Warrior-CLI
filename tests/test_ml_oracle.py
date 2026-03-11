import os
import sys
import types
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.item_normalizer import NormalizedMarketItem
from core.ml_oracle import (
    FAMILY_FEATURE_SCHEMAS,
    PRICE_ORACLE_FEATURE_SCHEMA,
    PricePredictor,
    ValuationResult,
)


def _normalized_wand() -> NormalizedMarketItem:
    return NormalizedMarketItem(
        item_id="wand-1",
        base_type="Imbued Wand",
        item_family="wand_caster",
        ilvl=84,
        listed_price=40.0,
        listing_currency="chaos",
        listing_amount=40.0,
        seller="seller",
        listed_at="2026-03-11T10:00:00Z",
        whisper="@seller hi",
        corrupted=False,
        fractured=False,
        influences=[],
        explicit_mods=["+#% increased Spell Damage", "+#% increased Cast Speed"],
        implicit_mods=[],
        prefix_count=2,
        suffix_count=0,
        open_prefixes=1,
        open_suffixes=3,
        mod_tokens=["SpellDamage1", "CastSpeed1"],
        tag_tokens=["wand", "caster", "spell"],
    )


def test_family_feature_schema_and_inference_frame_columns():
    assert PRICE_ORACLE_FEATURE_SCHEMA == FAMILY_FEATURE_SCHEMAS["generic"]

    predictor = PricePredictor()
    frame = predictor._build_inference_dataframe(_normalized_wand(), family="wand_caster")

    assert tuple(frame.columns) == FAMILY_FEATURE_SCHEMAS["wand_caster"]
    assert frame.shape == (1, len(FAMILY_FEATURE_SCHEMAS["wand_caster"]))


def test_predict_routes_to_family_fallback_and_returns_structured_result(monkeypatch):
    monkeypatch.setattr(PricePredictor, "_load_xgboost_models", lambda self: None)
    predictor = PricePredictor()

    result = predictor.predict(_normalized_wand())

    assert isinstance(result, ValuationResult)
    assert result.item_family == "wand_caster"
    assert result.model_source == "family_fallback"
    assert result.predicted_value > 0
    assert 0.3 <= result.confidence <= 0.95


def test_model_load_path_failure_keeps_fallback_inference_working(monkeypatch, tmp_path):
    model_file = tmp_path / "data" / "price_oracle_wand_caster.xgb"
    model_file.parent.mkdir(parents=True, exist_ok=True)
    model_file.write_text("corrupted-model")

    class BrokenBooster:
        def load_model(self, _path: str) -> None:
            raise RuntimeError("invalid model")

    fake_xgb = types.SimpleNamespace(Booster=BrokenBooster)
    monkeypatch.setitem(sys.modules, "xgboost", fake_xgb)
    monkeypatch.setattr(
        PricePredictor,
        "_resolve_model_path",
        lambda self, family: Path(model_file if family == "wand_caster" else tmp_path / f"price_oracle_{family}.xgb"),
    )

    predictor = PricePredictor()
    assert predictor.models == {}

    value, confidence = predictor.predict_value(_normalized_wand())
    assert value > 0
    assert 0.3 <= confidence <= 0.95
