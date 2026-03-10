from pathlib import Path
from typing import Set, Tuple, TYPE_CHECKING

import numpy as np
import pandas as pd

from core.meta_analyzer import LadderAnalyzer, MetaScores, calculate_meta_utility_score

PRICE_ORACLE_FEATURE_SCHEMA = (
    "is_influenced",
    "ilvl",
    "tier_life",
    "tier_speed",
    "tier_resist",
    "tier_crit",
    "open_affixes",
    "meta_utility_score",
)

if TYPE_CHECKING:
    from core.graph_engine import ItemState


class PricePredictor:
    """
    Oráculo de Previsão de Preços (Fase 5/6 - Módulo ML).
    Tenta carregar um modelo XGBoost treinado em 'data/price_oracle.xgb'.
    Faz fallback para avaliação preditiva hardcoded se o modelo não existir.
    """

    def __init__(self):
        self.model = None
        self._meta_scores: MetaScores | None = None
        self._meta_cache_loaded = False
        self._load_xgboost()

        # Dicionários de sinergias (Mock Fallback).
        self.synergies = {
            "Wand": [
                {"mods": {"SpellDamage1", "CastSpeed1"}, "value_chaos": 500.0},
                {"mods": {"SpellDamage1", "CritChanceSpells1"}, "value_chaos": 400.0},
            ],
            "Body Armour": [
                {"mods": {"Life1", "SpellSuppress1", "Resist1"}, "value_chaos": 1200.0}
            ],
        }

    def _load_xgboost(self):
        model_path = self._resolve_model_path()
        try:
            import xgboost as xgb

            if model_path.exists():
                self.model = xgb.Booster()
                self.model.load_model(str(model_path))
        except ImportError:
            pass
        except Exception:
            self.model = None

    def _resolve_model_path(self) -> Path:
        project_root = Path(__file__).resolve().parents[1]
        return project_root / "data" / "price_oracle.xgb"

    def _extract_features(self, item_state: "ItemState") -> list:
        """Extrai as 8 features usadas no nosso dataset de treino XGBoost."""
        current_mods = set(item_state.prefixes).union(set(item_state.suffixes))

        is_influenced = (
            1 if item_state.is_fractured else 0
        )  # Simplificação: usando fracture como influence flag pro mock
        ilvl = item_state.ilvl

        # Analisa Tiers simplificados para o modelo
        tier_life = 0
        tier_speed = 0
        tier_resist = 0
        tier_crit = 0

        for mod in current_mods:
            mod_lower = mod.lower()
            if "life" in mod_lower:
                tier_life = 1 if "1" in mod_lower else 2
            if "speed" in mod_lower:
                tier_speed = 1 if "1" in mod_lower else 2
            if "resistance" in mod_lower or "resist" in mod_lower:
                tier_resist = 1
            if "critical" in mod_lower or "crit" in mod_lower:
                tier_crit = 1

        open_affixes = item_state.open_prefixes + item_state.open_suffixes

        item_tags = self._extract_item_tags(current_mods, item_state.is_fractured)
        meta_utility_score = self._calculate_meta_utility_score(item_tags)

        return [
            [
                is_influenced,
                ilvl,
                tier_life,
                tier_speed,
                tier_resist,
                tier_crit,
                open_affixes,
                meta_utility_score,
            ]
        ]

    def _extract_item_tags(
        self, current_mods: Set[str], is_fractured: bool
    ) -> list[str]:
        tags: list[str] = []
        for mod in current_mods:
            mod_lower = mod.lower()
            if "life" in mod_lower:
                tags.append("life")
            if "speed" in mod_lower:
                tags.append("speed")
            if "resistance" in mod_lower or "resist" in mod_lower:
                tags.append("resistance")
            if "critical" in mod_lower or "crit" in mod_lower:
                tags.append("crit")
            if "fire" in mod_lower:
                tags.append("fire")
            if "cold" in mod_lower:
                tags.append("cold")
            if "lightning" in mod_lower:
                tags.append("lightning")
            if "physical" in mod_lower:
                tags.append("physical")
            if "chaos" in mod_lower:
                tags.append("chaos")
            if "attack" in mod_lower:
                tags.append("attack")
            if "spell" in mod_lower:
                tags.append("spell")

        if is_fractured:
            tags.append("fractured")

        return sorted(set(tags))

    def _load_meta_scores_cache(self) -> None:
        if self._meta_cache_loaded:
            return
        self._meta_cache_loaded = True
        try:
            analyzer = LadderAnalyzer()
            cached_scores = analyzer.get_cached_scores()
            if cached_scores and cached_scores.scores:
                self._meta_scores = cached_scores
        except Exception:
            self._meta_scores = None

    def _calculate_meta_utility_score(self, item_tags: list[str]) -> float:
        if not item_tags:
            return 0.0

        self._load_meta_scores_cache()
        if self._meta_scores is None:
            return 0.0

        return float(
            calculate_meta_utility_score(
                item_tags=item_tags,
                meta_scores=self._meta_scores,
                aggregation="mean",
            )
        )

    def _apply_target_inverse_transform(self, prediction_value: float) -> float:
        if not self.model:
            return max(0.0, prediction_value)

        try:
            target_transform = self.model.attr("target_transform")
        except Exception:
            target_transform = None

        if target_transform == "log1p":
            prediction_value = float(np.expm1(prediction_value))

        return max(0.0, prediction_value)

    def _build_inference_dataframe(self, item_state: "ItemState") -> pd.DataFrame:
        features = self._extract_features(item_state)
        frame = pd.DataFrame.from_records(features)
        frame.columns = pd.Index(PRICE_ORACLE_FEATURE_SCHEMA)
        return frame

    def predict_value(self, item_state: "ItemState") -> Tuple[float, float]:
        """
        Calcula o valor estimado de venda do item atual.
        Usa o XGBoost se carregado, senão cai na heurística base.

        Returns:
            Tuple[float, float]: (preco_previsto, confianca)
                - confianca: float entre 0.0 e 1.0
        """
        current_mods = set(item_state.prefixes).union(set(item_state.suffixes))
        if not current_mods:
            return (0.0, 0.3)

        confianca = self._calculate_confidence(item_state, current_mods)

        # -- XGBoost Inference --
        if self.model:
            import xgboost as xgb

            df = self._build_inference_dataframe(item_state)
            dmatrix = xgb.DMatrix(df)

            prediction = self.model.predict(dmatrix)
            preco = self._apply_target_inverse_transform(float(prediction[0]))
            return (preco, confianca)

        # -- Fallback Heuristic --
        best_value = 0.0

        # Checamos sinergias genéricas baseadas na classificação bruta do nome por simplicidade
        item_class = "Wand" if "Wand" in item_state.base_type else "Unknown"

        for rule in self.synergies.get(item_class, []):
            if rule["mods"].issubset(current_mods):
                if rule["value_chaos"] > best_value:
                    best_value = rule["value_chaos"]

        # Valor inerente de cada mod isolado (Tier 1 = ~10c)
        base_mod_value = len(current_mods) * 10.0

        preco = max(best_value, base_mod_value)
        return (preco, confianca)

    def _calculate_confidence(
        self, item_state: "ItemState", current_mods: Set[str]
    ) -> float:
        """
        Calcula a confiança da previsão baseada em múltiplos fatores.

        Fatores:
            - Modelo XGBoost carregado: +0.7 (confiança base)
            - Item com mods conhecidos (sinergia): +0.15
            - Item com influence ou fractured: +0.1
            - Item com ilvl > 80: +0.05
            - Mínimo: 0.3 (fallback heurística)

        Returns:
            float: Confiança entre 0.0 e 1.0
        """
        confidence = 0.0

        # Se o modelo XGBoost está carregado, confiança base de 0.7
        if self.model:
            confidence += 0.7
        else:
            # Fallback sem modelo, confiança mais baixa mas ainda válida
            confidence += 0.3

        # Verifica se o item tem mods conhecidos (está no dicionário de sinergias)
        item_class = "Wand" if "Wand" in item_state.base_type else "Unknown"
        known_synergy = False
        for rule in self.synergies.get(item_class, []):
            if rule["mods"].issubset(current_mods):
                known_synergy = True
                break

        if known_synergy:
            confidence += 0.15

        # Verifica se o item tem influence ou fractured
        if item_state.is_fractured:
            confidence += 0.1

        # Verifica ilvl alto (> 80)
        if item_state.ilvl > 80:
            confidence += 0.05

        # Confiança mínima de 0.3 (fallback heurística)
        return max(0.3, min(1.0, confidence))


class CraftingHeuristic:
    """
    Inteligência de Poda Heurística.
    Analisa semanticamente a Ação x Objetivo antes de executar a matemática pesada do Evaluator.
    """

    def __init__(self):
        pass

    def should_prune(
        self, item_state: "ItemState", action_name: str, target_mods: Set[str]
    ) -> bool:
        """
        Heurística Direcional. Retorna True se o branch de busca deve ser ASSASSINADO instantaneamente.
        Evita a explosão combinatória do A* testar Fossils ou Orbs inúteis pro alvo de forma cega.
        """
        # Exemplo 1: Target quer Physical, mas o algoritmo sugeriu "Metallic Fossil". O Metallic BLOQUEIA Physical.
        # Logo essa ação nasce morta e a heurística não gasta ciclos da CPU verificando o Evaluator.
        if "Metallic Fossil" in action_name:
            if any("phys" in mod.lower() for mod in target_mods):
                return True  # Prune it!

        if "Corroded Fossil" in action_name:
            if any(
                "elemental" in mod.lower() or "fire" in mod.lower()
                for mod in target_mods
            ):
                return True

        # Exemplo 2: Bloqueio Lógico. Se queremos adicionar Prefixos, e sugerimos Harvest, mas
        # o item já está com 3 prefixos e não usamos Annul... Morre aqui.
        if "Slam Exalted Orb" in action_name:
            if item_state.open_prefixes == 0 and item_state.open_suffixes == 0:
                return True

        return False
