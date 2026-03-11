from __future__ import annotations
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, Iterable, Optional, Set, Tuple, TYPE_CHECKING

import pandas as pd

from core.item_normalizer import NormalizedMarketItem, normalized_item_from_item_state

FAMILY_FEATURE_SCHEMAS: Dict[str, tuple[str, ...]] = {
    "wand_caster": (
        "ilvl",
        "has_spell_damage",
        "has_cast_speed",
        "has_spell_crit",
        "open_affixes",
        "is_influenced",
        "mod_count",
    ),
    "body_armour_defense": (
        "ilvl",
        "has_life",
        "has_suppress",
        "has_resist",
        "open_prefixes",
        "open_suffixes",
        "is_influenced",
    ),
    "jewel_cluster": (
        "ilvl",
        "has_life",
        "has_crit",
        "has_resist",
        "mod_count",
        "open_affixes",
    ),
    "accessory_generic": (
        "ilvl",
        "has_life",
        "has_resist",
        "has_attributes",
        "has_mana",
        "mod_count",
    ),
    "generic": (
        "ilvl",
        "has_life",
        "has_resist",
        "has_crit",
        "mod_count",
        "open_affixes",
    ),
}

PRICE_ORACLE_FEATURE_SCHEMA = FAMILY_FEATURE_SCHEMAS["generic"]
MODEL_FAMILIES = tuple(FAMILY_FEATURE_SCHEMAS.keys())

if TYPE_CHECKING:
    from core.graph_engine import ItemState


@dataclass(frozen=True)
class ValuationResult:
    predicted_value: float
    confidence: float
    item_family: str
    model_source: str
    feature_completeness: float

    def to_dict(self) -> dict:
        return asdict(self)


class PricePredictor:
    """Family-aware valuation engine with explicit fallbacks per item family."""

    _FAMILY_SYNERGIES: Dict[str, Dict[frozenset[str], float]] = {
        "wand_caster": {
            frozenset({"SpellDamage1", "CastSpeed1"}): 220.0,
            frozenset({"SpellDamage1", "CritChanceSpells1"}): 260.0,
            frozenset({"SpellDamage1", "CastSpeed1", "CritChanceSpells1"}): 420.0,
        },
        "body_armour_defense": {
            frozenset({"Life1", "SpellSuppress1"}): 180.0,
            frozenset({"Life1", "Resist1"}): 130.0,
            frozenset({"Life1", "SpellSuppress1", "Resist1"}): 340.0,
        },
        "jewel_cluster": {
            frozenset({"Life1", "CritChanceSpells1"}): 120.0,
            frozenset({"SpellDamage1", "CritChanceSpells1"}): 160.0,
        },
        "accessory_generic": {
            frozenset({"Life1", "Resist1"}): 110.0,
            frozenset({"Attributes1", "Resist1"}): 90.0,
        },
        "generic": {
            frozenset({"Life1", "Resist1"}): 85.0,
        },
    }

    _TOKEN_WEIGHTS: Dict[str, Dict[str, float]] = {
        "wand_caster": {
            "SpellDamage1": 70.0,
            "CastSpeed1": 55.0,
            "CritChanceSpells1": 65.0,
            "Mana1": 12.0,
        },
        "body_armour_defense": {
            "Life1": 50.0,
            "SpellSuppress1": 60.0,
            "Resist1": 30.0,
        },
        "jewel_cluster": {
            "Life1": 40.0,
            "SpellDamage1": 50.0,
            "CritChanceSpells1": 42.0,
            "Resist1": 25.0,
        },
        "accessory_generic": {
            "Life1": 45.0,
            "Resist1": 35.0,
            "Attributes1": 25.0,
            "Mana1": 15.0,
        },
        "generic": {
            "Life1": 30.0,
            "Resist1": 20.0,
            "CritChanceSpells1": 25.0,
            "SpellDamage1": 28.0,
        },
    }

    def __init__(self):
        self.models: Dict[str, object] = {}
        self._load_xgboost_models()

    def _resolve_model_path(self, family: str) -> Path:
        project_root = Path(__file__).resolve().parents[1]
        registry_path = project_root / "data" / "model_registry" / "registry.json"
        if registry_path.exists():
            try:
                registry = json.loads(registry_path.read_text(encoding="utf-8"))
                families = (
                    registry.get("families", {}) if isinstance(registry, dict) else {}
                )
                family_entry = (
                    families.get(family, {}) if isinstance(families, dict) else {}
                )
                active_version = family_entry.get("active_version")
                versions = family_entry.get("versions", [])
                if active_version and isinstance(versions, list):
                    for version in versions:
                        if isinstance(version, dict) and str(
                            version.get("run_id", "")
                        ) == str(active_version):
                            candidate_path = version.get("model_path")
                            if (
                                not isinstance(candidate_path, str)
                                or not candidate_path
                            ):
                                break
                            candidate_model_path = Path(candidate_path)
                            if not candidate_model_path.is_absolute():
                                candidate_model_path = (
                                    project_root / candidate_model_path
                                )
                            if candidate_model_path.exists():
                                return candidate_model_path
                            break
            except Exception:
                pass
        return project_root / "data" / f"price_oracle_{family}.xgb"

    def _load_xgboost_models(self) -> None:
        try:
            import xgboost as xgb
        except ImportError:
            return

        for family in MODEL_FAMILIES:
            model_path = self._resolve_model_path(family)
            if not model_path.exists():
                continue
            try:
                booster = xgb.Booster()
                booster.load_model(str(model_path))
                self.models[family] = booster
            except Exception:
                continue

    def _coerce_normalized_item(
        self, item: NormalizedMarketItem | "ItemState"
    ) -> NormalizedMarketItem:
        if isinstance(item, NormalizedMarketItem):
            return item
        return normalized_item_from_item_state(item)

    def _feature_map(self, item: NormalizedMarketItem) -> Dict[str, float]:
        tokens = set(item.mod_tokens)
        return {
            "ilvl": float(item.ilvl),
            "has_spell_damage": 1.0 if "SpellDamage1" in tokens else 0.0,
            "has_cast_speed": 1.0 if "CastSpeed1" in tokens else 0.0,
            "has_spell_crit": 1.0 if "CritChanceSpells1" in tokens else 0.0,
            "has_life": 1.0 if "Life1" in tokens else 0.0,
            "has_suppress": 1.0 if "SpellSuppress1" in tokens else 0.0,
            "has_resist": 1.0 if "Resist1" in tokens else 0.0,
            "has_crit": 1.0 if "CritChanceSpells1" in tokens else 0.0,
            "has_attributes": 1.0 if "Attributes1" in tokens else 0.0,
            "has_mana": 1.0 if "Mana1" in tokens else 0.0,
            "mod_count": float(len(item.mod_tokens)),
            "open_affixes": float(item.open_prefixes + item.open_suffixes),
            "open_prefixes": float(item.open_prefixes),
            "open_suffixes": float(item.open_suffixes),
            "is_influenced": 1.0 if (item.fractured or item.influences) else 0.0,
            "meta_utility_score": 0.0,
        }

    def _build_inference_dataframe(
        self, item: NormalizedMarketItem | "ItemState", family: Optional[str] = None
    ) -> pd.DataFrame:
        normalized = self._coerce_normalized_item(item)
        chosen_family = family or normalized.item_family
        schema = FAMILY_FEATURE_SCHEMAS.get(
            chosen_family, FAMILY_FEATURE_SCHEMAS["generic"]
        )
        feature_map = self._feature_map(normalized)
        row = {column: feature_map.get(column, 0.0) for column in schema}
        return pd.DataFrame([row], columns=schema)

    def _feature_completeness(self, item: NormalizedMarketItem) -> float:
        tokens = set(item.mod_tokens)
        if item.item_family == "wand_caster":
            required = {"SpellDamage1", "CastSpeed1", "CritChanceSpells1"}
        elif item.item_family == "body_armour_defense":
            required = {"Life1", "SpellSuppress1", "Resist1"}
        elif item.item_family == "jewel_cluster":
            required = {"Life1", "CritChanceSpells1"}
        elif item.item_family == "accessory_generic":
            required = {"Life1", "Resist1", "Attributes1"}
        else:
            required = {"Life1", "Resist1"}
        hits = len(required.intersection(tokens))
        return round(max(0.2, hits / max(len(required), 1)), 2)

    def _fallback_value(self, item: NormalizedMarketItem) -> float:
        weights = self._TOKEN_WEIGHTS.get(
            item.item_family, self._TOKEN_WEIGHTS["generic"]
        )
        token_value = sum(weights.get(token, 8.0) for token in item.mod_tokens)
        synergy_bonus = 0.0
        for token_set, bonus in self._FAMILY_SYNERGIES.get(
            item.item_family, {}
        ).items():
            if token_set.issubset(set(item.mod_tokens)):
                synergy_bonus = max(synergy_bonus, bonus)
        ilvl_bonus = max(0.0, item.ilvl - 75) * 1.5
        openness_bonus = (item.open_prefixes + item.open_suffixes) * 4.0
        influence_bonus = 18.0 if (item.fractured or item.influences) else 0.0
        base_floor = {
            "wand_caster": 18.0,
            "body_armour_defense": 24.0,
            "jewel_cluster": 12.0,
            "accessory_generic": 15.0,
            "generic": 10.0,
        }.get(item.item_family, 10.0)
        return round(
            base_floor
            + token_value
            + synergy_bonus
            + ilvl_bonus
            + openness_bonus
            + influence_bonus,
            1,
        )

    def _fallback_confidence(
        self, item: NormalizedMarketItem, model_loaded: bool
    ) -> float:
        confidence = 0.35 if not model_loaded else 0.55
        confidence += self._feature_completeness(item) * 0.25
        if item.item_family != "generic":
            confidence += 0.1
        if item.fractured or item.influences:
            confidence += 0.05
        if item.ilvl >= 84:
            confidence += 0.05
        return round(max(0.3, min(0.95, confidence)), 2)

    def predict(self, item: NormalizedMarketItem | "ItemState") -> ValuationResult:
        normalized = self._coerce_normalized_item(item)
        family = normalized.item_family or "generic"
        model = self.models.get(family)
        feature_completeness = self._feature_completeness(normalized)

        if model is not None:
            try:
                import xgboost as xgb

                frame = self._build_inference_dataframe(normalized, family=family)
                model_feature_names = list(getattr(model, "feature_names", []) or [])
                if model_feature_names:
                    for column in model_feature_names:
                        if column not in frame.columns:
                            frame[column] = 0.0
                    frame = frame.reindex(columns=model_feature_names, fill_value=0.0)
                prediction = model.predict(xgb.DMatrix(frame))
                predicted_value = max(0.0, float(prediction[0]))
                confidence = self._fallback_confidence(normalized, model_loaded=True)
                return ValuationResult(
                    predicted_value=round(predicted_value, 1),
                    confidence=confidence,
                    item_family=family,
                    model_source="family_model",
                    feature_completeness=feature_completeness,
                )
            except Exception:
                pass

        return ValuationResult(
            predicted_value=self._fallback_value(normalized),
            confidence=self._fallback_confidence(normalized, model_loaded=False),
            item_family=family,
            model_source="family_fallback",
            feature_completeness=feature_completeness,
        )

    def predict_value(
        self, item: NormalizedMarketItem | "ItemState"
    ) -> Tuple[float, float]:
        result = self.predict(item)
        return (result.predicted_value, result.confidence)


class CraftingHeuristic:
    """
    Inteligência de poda heurística para o grafo de craft.
    """

    def __init__(self):
        pass

    def should_prune(
        self, item_state: "ItemState", action_name: str, target_mods: Set[str]
    ) -> bool:
        if "Metallic Fossil" in action_name:
            if any("phys" in mod.lower() for mod in target_mods):
                return True

        if "Corroded Fossil" in action_name:
            if any(
                "elemental" in mod.lower() or "fire" in mod.lower()
                for mod in target_mods
            ):
                return True

        if "Slam Exalted Orb" in action_name:
            if item_state.open_prefixes == 0 and item_state.open_suffixes == 0:
                return True

        return False
