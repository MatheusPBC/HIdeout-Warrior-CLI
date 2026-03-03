from typing import Dict, List, Any, Tuple
from core.data_parser import RePoeParser

class CraftingEvaluator:
    """
    Fase 2: The Simulation Engine - Oráculo de Probabilidades.
    Este módulo atua como a base de cálculo de Weights estritos para simular a matemática
    do Craft of Exile em O(1), abstraindo de RePoeParser.
    """
    
    def __init__(self, data_parser: RePoeParser):
        self.parser = data_parser

    def _apply_fossil_math(self, local_weights: Dict[str, Dict[str, Any]], fossils: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Matemática de Fossils: 
        Fósseis aplicam multiplicadores (normalmente 10x) em tags correspondentes 
        e anulam (0x) tags bloqueadas.
        Exemplo: 'Metallic Fossil' -> tag 'lightning' ganha 10x, tag 'physical' zera.
        """
        # Exemplo Hardcoded das principais mecânicas Fossils do core game.
        fossil_rules = {
            "Metallic Fossil": {"buff": ["lightning"], "block": ["physical"]},
            "Jagged Fossil": {"buff": ["physical"], "block": []},
            "Corroded Fossil": {"buff": [], "block": ["elemental", "fire", "cold", "lightning"]},
            "Dense Fossil": {"buff": ["defences"], "block": ["life"]},
            "Pristine Fossil": {"buff": ["life"], "block": ["defences"]},
        }

        buffed_tags = set()
        blocked_tags = set()

        for fossil in fossils:
            rule = fossil_rules.get(fossil, {})
            buffed_tags.update(rule.get("buff", []))
            blocked_tags.update(rule.get("block", []))

        adjusted_weights = {}
        for mod_id, mod_data in local_weights.items():
            mod_tags = mod_data.get("tags", [])
            new_weight = mod_data.get("weight", 0)

            # Block rule overrides buffs
            if any(bt in mod_tags for bt in blocked_tags):
                new_weight = 0
            elif any(bt in mod_tags for bt in buffed_tags):
                # 10x multiplier rule for Fossil crafting
                new_weight *= 10
            
            adjusted_weights[mod_id] = {
                "mod_id": mod_data.get("mod_id"),
                "tier": mod_data.get("tier"),
                "tags": mod_data.get("tags"),
                "weight": new_weight
            }

        return adjusted_weights

    def _apply_catalyst_math(self, base_weight: int, item_quality: int, mod_tags: List[str], catalyst_tags: List[str]) -> int:
        """
        Matemática de Catalysts (Exalted / Annulment Orbs logic):
        Se o item tiver Quality proveniente de Catalysts (máx 20%),
        adiciona +1% de weight por cada 1% de quality para tags correspondentes.
        """
        if item_quality <= 0:
            return base_weight
            
        clamped_quality = min(item_quality, 20)
        
        # Se as tags baterem com o Catalyst, ganha Bonus % Weight.
        if any(cat in mod_tags for cat in catalyst_tags):
            multiplier = 1.0 + (clamped_quality / 100.0)
            return int(base_weight * multiplier)
            
        return base_weight

    def calculate_mod_chance(self, base_type: str, current_mods: List[str], target_mod_id: str, action: str, **kwargs) -> float:
        """
        Cálculo Base de Mod: Probabilidade (P) = TargetWeight / TotalPoolWeight
        'action' determina o scope do cálculo (Ex: "Exalt", "Chaos", "Fossil").
        Kwargs opcionais: 'fossils' (list), 'catalyst_quality' (int), 'catalyst_tags' (list).
        """
        # Num cenário do mundo real filtraríamos self.parser.db primeiro pelo base_type.
        # Aqui simularemos em cima do DB Global para velocidade da PoC.
        all_mods = self.parser.db
        
        # Filtro de Mods Globais base (simplificado iterando keys brutas)
        local_pool = {}
        target_mod_info = None

        for mod_key, mod_data in all_mods.items():
            # Simplificação: assume que os weights globais do RePoe já tratam spawn chance>0 pra nossa base
            if mod_data.get("weights"):
                current_weight = 0
                # O mod de base extraído tem uma lista de weights que precisam bater com as tags.
                # Considerando a default chance local:
                if len(mod_data["weights"]) > 0:
                    current_weight = mod_data["weights"][0].get("weight", 0)
                
                local_pool[mod_key] = {
                    "mod_id": mod_data.get("mod_id"),
                    "tier": mod_data.get("tier"),
                    "tags": mod_data.get("tags", []),
                    "weight": current_weight,
                    "mod_group": mod_data.get("mod_group", "")
                }
                
                if mod_key == target_mod_id:
                    target_mod_info = local_pool[mod_key]

        # Tratamento especial de Fossils
        if action == "Fossil":
            fossils = kwargs.get("fossils", [])
            local_pool = self._apply_fossil_math(local_pool, fossils)
            
        if not target_mod_info or local_pool.get(target_mod_id, {}).get("weight", 0) <= 0:
            return 0.0

        target_w = local_pool[target_mod_id]["weight"]
        total_w = 0

        # Lógica de remoção de Constraints. (Itens não podem dar double-roll num mod_group que já exista)
        current_mod_groups = []
        for cmod in current_mods:
             if cmod in all_mods:
                 current_mod_groups.append(all_mods[cmod].get("mod_group"))

        for k, v in local_pool.items():
             # Exalted/Chaos não jogam mods de um grupo que o item já possui
             if v.get("mod_group") in current_mod_groups:
                 continue
                 
             mod_weight_local = v.get("weight", 0)
             
             # Aplica Catalysts no caso de Exalt
             if action == "Exalt":
                q = kwargs.get("catalyst_quality", 0)
                ct = kwargs.get("catalyst_tags", [])
                mod_weight_local = self._apply_catalyst_math(mod_weight_local, q, v.get("tags", []), ct)
                
             total_w += mod_weight_local

        if total_w == 0:
            return 0.0

        # Atualizando o TargetW com o Catalyst também
        if action == "Exalt":
            q = kwargs.get("catalyst_quality", 0)
            ct = kwargs.get("catalyst_tags", [])
            target_w = self._apply_catalyst_math(target_w, q, local_pool[target_mod_id].get("tags", []), ct)

        return target_w / total_w

    def calculate_veiled_orb_chance(self, current_prefixes: int, current_suffixes: int, meta_mods: List[str]) -> Dict[str, float]:
        """
        Lógica de Veiled Orbs:
        Calcula a probabilidade do Orb adicionar um Prefixo X ou Sulfixo Y respeitando as regras nativas de Meta-Mods da Workbench.
        """
        can_add_prefix = current_prefixes < 3
        can_add_suffix = current_suffixes < 3
        
        # Meta-Mod Blocking Rules
        if "Prefixes Cannot Be Changed" in meta_mods:
            # Protege Prefixes. O Orb VAI TENTAR remover um mod DESPROTEGIDO (Suffix)
            # E VAI adicionar o Veiled Mod num local DESPROTEGIDO (Sufixo).
            can_add_prefix = False
            
        if "Suffixes Cannot Be Changed" in meta_mods:
            can_add_suffix = False
            
        # O Modificador só roleta se houver espaço válido para ele entrar.
        p_prefix = 0.0
        p_suffix = 0.0
        
        # Lógica padrão (50/50 na GGG, caso ambos estejam abertos depois da remoção de 1 mod aleatório válido)
        if can_add_prefix and can_add_suffix:
            # Há uma pequena chance de pender mais pra Sufixos baseado em weight bruto no RePoE,
            # mas simplificamos a formulação do Veiled Orb tradicional GGG standard 50/50:
            p_prefix = 0.50
            p_suffix = 0.50
        elif can_add_prefix:
            p_prefix = 1.0
        elif can_add_suffix:
            p_suffix = 1.0
            
        return {
            "prefix_chance": p_prefix,
            "suffix_chance": p_suffix
        }
