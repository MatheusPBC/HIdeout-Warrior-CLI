import numpy as np

class RecombinatorMath:
    """
    Módulo B (Sub-engine): Motor de Matemática de Recombinadores.
    
    A mecânica de Recombinators no jogo combina dois itens (doador 1 e doador 2) 
    para forjar um item final, retendo propriedades de ambos.
    """

    def __init__(self):
        pass

    def calculate_affix_survival_chance(self, item1_affixes: dict, item2_affixes: dict, target_affix: str) -> float:
        """
        Calcula a probabilidade de um afixo específico sobreviver à fusão.
        
        Logica de probabilidade de Recombinators (Oversimplificada para a demonstração):
        1. A base do item final tem ~50% de chance de ser do item 1 ou item 2.
        2. O número de afixos retidos no pool depende do total de prefixos/sufixos combinados 
           (quanto maior o pool total, menor a chance de guiar o craft para exatos afixos).
        3. Um afixo presente em ambos os itens (Pool Compartilhado) tem uma grande vantagem
           (frequentemente >60-70% dependendo do tamanho bruto pool).
        4. Um afixo exclusivo (Pool Único) concorre contra a entropia da fusão,
           tendo uma chance base próxima a ~35% que degrada conforme a base de lixo aumenta.
        
        Args:
            item1_affixes: Dict contendo afixos do primeiro item.
            item2_affixes: Dict contendo afixos do segundo item.
            target_affix: O afixo alvo que esperamos reter.
            
        Returns:
            Float representando a chance de sucesso (0.0 a 1.0).
        """
        count_item1 = len(item1_affixes)
        count_item2 = len(item2_affixes)
        total_pool_size = count_item1 + count_item2
        
        in_item1 = target_affix in item1_affixes
        in_item2 = target_affix in item2_affixes
        
        if not in_item1 and not in_item2:
            return 0.0
            
        # Cálculo de base
        if in_item1 and in_item2:
            # Afixo dobrado! Peso extremamente alto, resiste bem contra um pool maior.
            # Começa em 65% de chance, e degrada levemente conforme o tamanho da pool aumenta (penalidade de ~5% por afixo total).
            base_chance = 0.65 - (total_pool_size * 0.03) 
        else:
            # Afixo exclusivo. Disputa vagas com o resíduo do outro item.
            # Começa em ~35%, degradando bruscamente (~7% por afixo).
            base_chance = 0.35 - (total_pool_size * 0.05)
            
        # Utilizando numpy para fazer um clip da probabilidade matemática para sempre residir
        # dentro da margem de 1% a 99% (evitando absurdos negativos no caso de muito lixo).
        survival_chance = float(np.clip(base_chance, 0.01, 0.99))
        
        return survival_chance
