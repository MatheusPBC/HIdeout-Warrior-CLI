from typing import Set, List, Any, TYPE_CHECKING

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
                {"mods": {"SpellDamage1", "CritChanceSpells1"}, "value_chaos": 400.0}
            ],
            "Body Armour": [
                {"mods": {"Life1", "SpellSuppress1", "Resist1"}, "value_chaos": 1200.0}
            ]
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

    def _extract_features(self, item_state: 'ItemState') -> list:
         """Extrai as 5 features usadas no nosso dataset de treino XGBoost."""
         current_mods = set(item_state.prefixes).union(set(item_state.suffixes))
         
         is_influenced = 1 if item_state.is_fractured else 0 # Simplificação: usando fracture como influence flag pro mock
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

    def predict_value(self, item_state: 'ItemState') -> float:
        """
        Calcula o valor estimado de venda do item atual.
        Usa o XGBoost se carregado, senão cai na heurística base.
        """
        current_mods = set(item_state.prefixes).union(set(item_state.suffixes))
        if not current_mods:
             return 0.0

        # -- XGBoost Inference --
        if self.model:
             import pandas as pd
             import xgboost as xgb
             
             features = self._extract_features(item_state)
             df = pd.DataFrame(features, columns=["is_influenced", "ilvl", "tier_life", "tier_speed", "open_affixes"])
             dmatrix = xgb.DMatrix(df)
             
             prediction = self.model.predict(dmatrix)
             return max(0.0, float(prediction[0]))


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
        
        return max(best_value, base_mod_value)


class CraftingHeuristic:
    """
    Inteligência de Poda Heurística.
    Analisa semanticamente a Ação x Objetivo antes de executar a matemática pesada do Evaluator.
    """
    def __init__(self):
        pass

    def should_prune(self, item_state: 'ItemState', action_name: str, target_mods: Set[str]) -> bool:
        """
        Heurística Direcional. Retorna True se o branch de busca deve ser ASSASSINADO instantaneamente.
        Evita a explosão combinatória do A* testar Fossils ou Orbs inúteis pro alvo de forma cega.
        """
        # Exemplo 1: Target quer Physical, mas o algoritmo sugeriu "Metallic Fossil". O Metallic BLOQUEIA Physical.
        # Logo essa ação nasce morta e a heurística não gasta ciclos da CPU verificando o Evaluator.
        if "Metallic Fossil" in action_name:
             if any("phys" in mod.lower() for mod in target_mods):
                 return True # Prune it!
                 
        if "Corroded Fossil" in action_name:
             if any("elemental" in mod.lower() or "fire" in mod.lower() for mod in target_mods):
                 return True

        # Exemplo 2: Bloqueio Lógico. Se queremos adicionar Prefixos, e sugerimos Harvest, mas 
        # o item já está com 3 prefixos e não usamos Annul... Morre aqui.
        if "Slam Exalted Orb" in action_name:
             if item_state.open_prefixes == 0 and item_state.open_suffixes == 0:
                 return True

        return False
