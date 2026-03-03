from typing import Set, List, Any, TYPE_CHECKING

if TYPE_CHECKING:
    from core.graph_engine import ItemState

class PricePredictor:
    """
    Oráculo de Previsão de Preços (Fase 5 - Módulo B Avançado).
    Simula um modelo de regressão/avaliação preditiva para descobrir se o estado atual do item
    possui alto valor de mercado mesmo que não seja o "objetivo final".
    Preparado para receber integração com XGBoost treinado no poe.ninja.
    """
    def __init__(self):
        # Dicionários de sinergias (Mock). Exemplo: Spell Damage e Cast Speed valem muito juntos em Wands.
        self.synergies = {
            "Wand": [
                {"mods": {"SpellDamage1", "CastSpeed1"}, "value_chaos": 500.0},
                {"mods": {"SpellDamage1", "CritChanceSpells1"}, "value_chaos": 400.0}
            ],
            "Body Armour": [
                {"mods": {"Life1", "SpellSuppress1", "Resist1"}, "value_chaos": 1200.0}
            ]
        }

    def predict_value(self, item_state: 'ItemState') -> float:
        """
        Calcula o valor estimado de venda do item atual baseado nas sinergias e meta atual.
        """
        current_mods = set(item_state.prefixes).union(set(item_state.suffixes))
        if not current_mods:
             return 0.0

        best_value = 0.0
        
        # Heurística de Base: se for Wand e a sinergia for satisfeita, o preço reflete o mockup.
        # Numa DAG real isso seria: return self.xgb_model.predict([encoded_state])[0]
        
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
