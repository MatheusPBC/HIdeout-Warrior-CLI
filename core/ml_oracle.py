from typing import Set, List, Any, Tuple, TYPE_CHECKING

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
        try:
            import xgboost as xgb
            import os

            model_path = os.path.join("data", "price_oracle.xgb")
            if os.path.exists(model_path):
                self.model = xgb.Booster()
                self.model.load_model(model_path)
        except ImportError:
            pass

    def _extract_features(self, item_state: "ItemState") -> list:
        """Extrai as 5 features usadas no nosso dataset de treino XGBoost."""
        current_mods = set(item_state.prefixes).union(set(item_state.suffixes))

        is_influenced = (
            1 if item_state.is_fractured else 0
        )  # Simplificação: usando fracture como influence flag pro mock
        ilvl = item_state.ilvl

        # Analisa Tiers simplificados para o modelo
        tier_life = 0
        tier_speed = 0

        for mod in current_mods:
            if "life" in mod.lower():
                tier_life = 1 if "1" in mod else 2
            if "speed" in mod.lower():
                tier_speed = 1 if "1" in mod else 2

        open_affixes = item_state.open_prefixes + item_state.open_suffixes

        return [[is_influenced, ilvl, tier_life, tier_speed, open_affixes]]

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
            import pandas as pd
            import xgboost as xgb

            features = self._extract_features(item_state)
            df = pd.DataFrame(
                features,
                columns=[
                    "is_influenced",
                    "ilvl",
                    "tier_life",
                    "tier_speed",
                    "open_affixes",
                ],
            )
            dmatrix = xgb.DMatrix(df)

            prediction = self.model.predict(dmatrix)
            preco = max(0.0, float(prediction[0]))
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
