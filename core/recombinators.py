import numpy as np
from typing import List, Dict

# Assumindo que o ItemA/ItemB cheguem como dicionarios baseados no Schema
# ou instâncias provindas das rotinas do scraper/broker.
from .models import TargetStats, AffixTarget

class RecombinatorEngine:
    """
    Sub-Engine (Módulo B) matemática baseada na mecânica de Recombinators (Sentinel/Settlers).
    
    A regra do Recombinator:
    - Combina dois itens (Item A e Item B).
    - Processa Prefixos separados dos Sufixos.
    - O número de afixos retidos no Item final depende do tamanho do "Pool" (Afixos A + Afixos B).
    - Tabela de Retenção:
        Pool 1: 100% de manter 1 afixo.
        Pool 2: 33% (1 afixo), 66% (2 afixos).
        Pool 3: 20% (1 afixo), 50% (2 afixos), 30% (3 afixos).
        Pool 4: 35% (2 afixos), 55% (3 afixos)  [10% falha ignorada -> Normalizada]
        Pool 5: 20% (2 afixos), 80% (3 afixos).
        Pool 6: 100% (3 afixos).
    """

    def __init__(self):
        # Tabela oficial de retenção de slots (Pool Size -> Probabilities[1, 2, 3 slots])
        # [Chance 1 Slot, Chance 2 Slots, Chance 3 Slots]
        self.retention_table = {
            # Pool 1: 100% = 1 afixo
            1: np.array([1.0, 0.0, 0.0]),
            # Pool 2: 33% = 1 afixo, 66% = 2 afixos
            2: np.array([0.33, 0.66, 0.0]),
            # Pool 3: 20% = 1 afixo, 50% = 2 afixos, 30% = 3 afixos
            3: np.array([0.20, 0.50, 0.30]),
            # Pool 4: 35% = 2 afixos, 55% = 3 afixos. (Resto de downgrade ignorado ~10%)
            # Normalizado pra dar 1.0 -> (0.388 pra 2 afixos, 0.611 pra 3 afixos)
            4: np.array([0.0, 0.3888, 0.6111]),
            # Pool 5: 20% = 2 afixos, 80% = 3 afixos.
            5: np.array([0.0, 0.20, 0.80]),
            # Pool 6: 100% = 3 afixos
            6: np.array([0.0, 0.0, 1.0])
        }

    def _calculate_pool_success(self, item_a_mods: List[str], item_b_mods: List[str], target_mod: str) -> float:
        """
        Calcula a chance de um afixo específico sobreviver baseado no tamanho da pool gerada (Prefix ou Suffix isolation).
        """
        pool_size = len(item_a_mods) + len(item_b_mods)
        if pool_size == 0 or pool_size > 6:
            return 0.0 # Sem mods, sem chance ou Pool excede limite do motor (ex: Itens corrompidos absurdos).
        
        # O quão frequente o mod é no pool?
        weight_in_pool = 0
        if target_mod in item_a_mods:
            weight_in_pool += 1
        if target_mod in item_b_mods:
            weight_in_pool += 1
            
        if weight_in_pool == 0:
            return 0.0 # Mod não existe em nenhum item.
        
        # Puxamos as chances de puxar N slots daquela pool (Ex: Pool 4 = [Chance de puxar 1, Chance puxar 2, Chance puxar 3])
        draw_chances = self.retention_table[pool_size]
        
        success_chance = 0.0
        
        # Simulação Estatística:
        # Array `[1, 2, 3]` representa quantos mods o algoritmo do Recombinator puxou do saco.
        # Nós usamos hipergeométrica base matemática simplificada para calcular as odds daquele TARGET
        # ser um dos escolhdos dados "K" slots sacados de "N" totais onde "W" são "meu mod target"
        for slots_drawn, prob_of_this_draw in enumerate(draw_chances, start=1):
            if prob_of_this_draw > 0.0:
                # Qual a chance do meu target mod estar dentro desses `slots_drawn`?
                # C(TargetWeight, 1) * C(Pool - TargetWeight, slots_drawn - 1) / C(Pool, slots_drawn)
                # Mais simplificado pro motor de craft da CLI:
                
                # Se eu puxo X slots de um pool Y, a chance em branco do mod estar dentro é X / Y
                # Multiplicado por quantas vezes ele está no pot (weight_in_pool).
                # Isso impede draws de > 100%, portanto usamos numpy clip.
                odds = (slots_drawn / pool_size) * weight_in_pool
                clamped_odds = float(np.clip(odds, 0.0, 1.0))
                
                # Somamos isso pro success final ponderado pela chance de acontecer AQUELE draw de slots.
                success_chance += clamped_odds * prob_of_this_draw
                
        return success_chance

    def calculate_fusion_probability(self, item_a: Dict, item_b: Dict, target: TargetStats) -> float:
        """
        Calcula a chance estatística global da fusão Retornar a Base escolhida COM todos
        os afixos desejados intactos baseado na matriz de probabilidade da GGG.
        
        `item_a`/`item_b` assumem Formato Dict bruto contendo listas ["prefixes"] e ["suffixes"] 
        com as strings dos "trade_api_id" (ex: ["pseudo.pseudo_total_mana"]).
        """
        # 1. Base Resolution: 50% de chance para qualquer uma das bases
        # Na engine real do Poe, teríamos que checar ILVL e Base classes iguais.
        base_retention_chance = 0.50
        
        # Isolamos Prefixes de ambos
        a_prefixes = item_a.get("prefixes", [])
        b_prefixes = item_b.get("prefixes", [])
        
        # Isolamos Sufixos
        a_suffixes = item_a.get("suffixes", [])
        b_suffixes = item_b.get("suffixes", [])
        
        # 2. Iteramos nos Targets Exigidos
        total_odds = base_retention_chance
        
        # Para cada Prefixo Desejado, testamos a sobrevivência do Pool
        for prefix_target in target.prefixes:
            target_id = prefix_target.trade_api_id
            odds_prefix = self._calculate_pool_success(a_prefixes, b_prefixes, target_id)
            total_odds *= odds_prefix # Mutualmente Exclusivo (Chance Composta: A AND B)
            
        # Para cada Sufixo Desejado, testamos a sobrevivência do Pool Adjacente
        for suffix_target in target.suffixes:
            target_id = suffix_target.trade_api_id
            odds_suffix = self._calculate_pool_success(a_suffixes, b_suffixes, target_id)
            total_odds *= odds_suffix
            
        return float(total_odds)
