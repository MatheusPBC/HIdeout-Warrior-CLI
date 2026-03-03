import numpy as np
from typing import List, Dict, Set, Any

class RecombinatorEngine:
    """
    Sub-Engine (Módulo B) matemática baseada na mecânica de Recombinators (Sentinel/Settlers).
    Calcula a chance de sobrevivência de modificadores quando dois itens são fundidos.
    """
    def __init__(self):
        # Tabela oficial de retenção de slots (Pool Size -> Probabilities[1, 2, 3 slots])
        self.retention_table = {
            1: np.array([1.0, 0.0, 0.0]),
            2: np.array([0.33, 0.66, 0.0]),
            3: np.array([0.20, 0.50, 0.30]),
            # Resto de downgrade ignorado para fins práticos matemáticos normalizados.
            4: np.array([0.0, 0.3888, 0.6111]),
            5: np.array([0.0, 0.20, 0.80]),
            6: np.array([0.0, 0.0, 1.0])
        }

    def _resolve_exclusive_groups(self, mods_a: List[Dict[str, Any]], mods_b: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Regras de Exclusão (Shared Mod Groups):
        Se os dois itens partilharem o mesmo `mod_group` (ex: '# to maximum Life'),
        eles colapsam num só durante a fusão, ou o sistema escolhe apenas um console
        as regras pre-definidas.
        Aqui agrupamos mods e consideramos apenas 1 peso combinatório se colidirem no mesmo grupo.
        """
        pool = []
        seen_groups = set()
        
        # Juntamos ambos
        all_mods = mods_a + mods_b
        
        # Filtramos collisões de grupo mantendo apenas a ocorrência pra roleta. 
        # (Se for um grupo válido e duplicado entre A e B).
        for mod in all_mods:
            mod_group = mod.get("mod_group")
            # Se não tem group, entra limpo.
            if not mod_group:
                 pool.append(mod)
                 continue
                 
            # Se o mod for Non-Native ou exclusivo sem stack, trataremos a multiplicidade.
            if mod_group not in seen_groups:
                 seen_groups.add(mod_group)
                 pool.append(mod)
                 
        return pool

    def _calculate_pool_success(self, merged_pool: List[Dict[str, Any]], target_mods_in_pool: List[str]) -> float:
        """
        Calcula a chance de sobrevivência para os target_mods dentro do pool atual 
        (Prefixos ou Sufixos segregados).
        Retorna Rota Estatística (P).
        """
        pool_size = len(merged_pool)
        if pool_size == 0 or pool_size > 6:
            return 0.0 
            
        points_in_pool = 0
        for mod in merged_pool:
            if mod.get("mod_id") in target_mods_in_pool:
                # O modificador target real bateu com um peso
                points_in_pool += 1
                
        if points_in_pool == 0:
            return 0.0
            
        draw_chances = self.retention_table.get(pool_size, np.array([0.0, 0.0, 0.0]))
        success_chance = 0.0
        
        for slots_drawn, prob_of_this_draw in enumerate(draw_chances, start=1):
            if prob_of_this_draw > 0.0:
                # Odds puras: X slots_drawn / pool_size
                odds = (slots_drawn / pool_size) * points_in_pool
                
                # Non-Native Natural (NNN) e exclusividades geram penalties escondidos 
                # (ex: Drop-only Essence ou Incursion). Simplificamos no clip de teto da ODD:
                clamped_odds = float(np.clip(odds, 0.0, 1.0))
                success_chance += clamped_odds * prob_of_this_draw
                
        return success_chance

    def calculate_recombination_chance(self, item_a: Dict[str, Any], item_b: Dict[str, Any], desired_mods: List[str]) -> float:
        """
        Calcula a probabilidade Global de uma Recombinação atingir o item alvo (Desired Mods).
        
        O 'item_a' e 'item_b' contêm as listas completas de seus afixos mapeados com 
        tags Pydantic e mod_group.
        """
        # Sorteio Base: 50/50 para Escolher a Base de 'Item A' (Ignorando iLvl biasing complexo)
        base_retention_chance = 0.50
        
        # Filtros Lógicos: O PoE divide a roleta em Prefixes vs Suffixes.
        a_prefixes = [m for m in item_a.get("mods", []) if m.get("type") == "prefix"]
        b_prefixes = [m for m in item_b.get("mods", []) if m.get("type") == "prefix"]
        
        a_suffixes = [m for m in item_a.get("mods", []) if m.get("type") == "suffix"]
        b_suffixes = [m for m in item_b.get("mods", []) if m.get("type") == "suffix"]
        
        # Pool Resolve com Exclusões e Stack Limit
        prefix_pool = self._resolve_exclusive_groups(a_prefixes, b_prefixes)
        suffix_pool = self._resolve_exclusive_groups(a_suffixes, b_suffixes)
        
        # O Modificador desejado possui propriedades 'type' = prefix ou suffix?
        # Num cenário real leríamos do Schema. Aqui inferimos com base nos afixos contidos na base pra teste matemático
        desired_prefixes = [mod for mod in desired_mods if any(p.get("mod_id") == mod for p in prefix_pool)]
        desired_suffixes = [mod for mod in desired_mods if any(s.get("mod_id") == mod for s in suffix_pool)]
        
        odds_prefix = 1.0
        if desired_prefixes:
            odds_prefix = self._calculate_pool_success(prefix_pool, desired_prefixes)
            
        odds_suffix = 1.0
        if desired_suffixes:
            odds_suffix = self._calculate_pool_success(suffix_pool, desired_suffixes)
        
        return base_retention_chance * odds_prefix * odds_suffix

if __name__ == "__main__":
    print("--- Teste de Stress/Recombinator Engine ---")
    
    # Exemplo: Item A tem Vida Alta (prefix). Item B tem Vida Alta (prefix).
    item_a_test = {
        "mods": [
            {"mod_id": "maximum_life_1", "type": "prefix", "mod_group": "Life"}
        ]
    }
    item_b_test = {
        "mods": [
            {"mod_id": "movement_speed_1", "type": "prefix", "mod_group": "MovementVelocity"},
            {"mod_id": "fire_res_1", "type": "suffix", "mod_group": "FireResist"}
        ]
    }
    
    desired = ["maximum_life_1", "movement_speed_1"]
    
    engine = RecombinatorEngine()
    chance = engine.calculate_recombination_chance(item_a_test, item_b_test, desired)
    
    print(f"Probabilidade Certa Combinatória de Extrair Desired Mods: {chance:.4f} ({(chance * 100):.2f}%)")
