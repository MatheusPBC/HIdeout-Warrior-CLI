"""
Probability Engine para craft-plan MVP.

Motor de cálculo de EV (Expected Value) para comparação de métodos de crafting.

Segurança:
- Todos os resultados incluem data_source e used_fallback
- RePoEParser tentado primeiro; fallback explícito quando indisponível
- Arredondamento conservativo para evitar falsa precisão

Nichos suportados:
- es_influence_shield: Influenced ES Shield (Spell Suppression suffix)
- es_body_armour_influenced: Influenced Body Armour (ES% prefix)
- suppress_evasion_chest: Evasion Chest (Spell Suppression suffix)
- wand_plus_gems: Wands (+1 Spell Gems / +1 Int Gems)
"""

import logging
from dataclasses import dataclass
from typing import Optional

from core.data_parser import RePoeParser, REPOE_MOD_IDS, DENSE_FOSSIL_POSITIVE_TAG

logger = logging.getLogger(__name__)


# ============================================================================
# NICHE CONFIGS - Configuração centralizada de todos os nichos
# ============================================================================

NICHE_CONFIGS = {
    "es_influence_shield": {
        "description": "Influenced Energy Shield (ES Shield + Dex/Int)",
        "item_tag": "dex_int_armour",
        "base_cost": 50.0,  # Custo base influenciada
        "target_sale_value": 350.0,
        "target_mods": {
            "dense_fossil": {
                "mod_ids": REPOE_MOD_IDS["spell_suppression"]["mod_ids"],
                "generation_type": "suffix",
            },
            "harvest_reforge": {
                "mod_ids": REPOE_MOD_IDS["spell_suppression"]["mod_ids"],
                "generation_type": "suffix",
            },
            "essence": {
                "mod_ids": REPOE_MOD_IDS["energy_shield_percent"]["mod_ids"],
                "generation_type": "prefix",
            },
        },
        "pool_groups": ["ChanceToSuppressSpells"],
    },
    "es_body_armour_influenced": {
        "description": "Influenced Body Armour (ES% prefix)",
        "item_tag": "body_armour",
        "base_cost": 80.0,  # Custo base body armour influenciada
        "target_sale_value": 400.0,
        "target_mods": {
            "dense_fossil": {
                "mod_ids": REPOE_MOD_IDS["es_percent_body"]["mod_ids"],
                "generation_type": "prefix",
            },
            "harvest_reforge": {
                "mod_ids": REPOE_MOD_IDS["es_percent_body"]["mod_ids"],
                "generation_type": "prefix",
            },
            "essence": {
                "mod_ids": REPOE_MOD_IDS["es_percent_body"]["mod_ids"],
                "generation_type": "prefix",
            },
        },
        "pool_groups": ["DefencesPercent"],
    },
    "suppress_evasion_chest": {
        "description": "Evasion Chest with Spell Suppression",
        "item_tag": "dex_armour",
        "base_cost": 30.0,  # Custo base evasion chest
        "target_sale_value": 280.0,
        "target_mods": {
            "dense_fossil": {
                "mod_ids": REPOE_MOD_IDS["spell_suppression_dex"]["mod_ids"],
                "generation_type": "suffix",
            },
            "harvest_reforge": {
                "mod_ids": REPOE_MOD_IDS["spell_suppression_dex"]["mod_ids"],
                "generation_type": "suffix",
            },
            "essence": {
                "mod_ids": REPOE_MOD_IDS["spell_suppression_dex"]["mod_ids"],
                "generation_type": "suffix",
            },
        },
        "pool_groups": ["ChanceToSuppressSpells"],
    },
    "wand_plus_gems": {
        "description": "Wand with +1 Spell/Intelligence Gems",
        "item_tag": "wand",
        "base_cost": 15.0,  # Custo base wand
        "target_sale_value": 250.0,
        "target_mods": {
            "dense_fossil": {
                "mod_ids": REPOE_MOD_IDS["spell_skill_gem_level"]["mod_ids"],
                "generation_type": "prefix",
            },
            "harvest_reforge": {
                "mod_ids": REPOE_MOD_IDS["intelligence_gem_level"]["mod_ids"],
                "generation_type": "suffix",
            },
            "essence": {
                "mod_ids": REPOE_MOD_IDS["spell_skill_gem_level"]["mod_ids"],
                "generation_type": "prefix",
            },
        },
        "pool_groups": None,  # Sem filtro de grupo - pool completo
    },
}


# ============================================================================
# PARÂMETROS PROBABILÍSTICOS - FALLBACKS POR NICHO
# ============================================================================

# Fallbacks padrão (usados quando RePoE não disponível)
_DENSE_FOSSIL_PARAMS_FALLBACK = {
    "base_cost": 120.0,
    "hit_probability": 0.18,
    "brick_risk": 0.12,
    "value_delta": 180.0,
}

_HARVEST_REFORGE_PARAMS_FALLBACK = {
    "base_cost": 80.0,
    "hit_probability": 0.24,
    "brick_risk": 0.08,
    "value_delta": 150.0,
}

_ESSENCE_PARAMS_FALLBACK = {
    "base_cost": 45.0,
    "hit_probability": 0.35,
    "brick_risk": 0.05,
    "value_delta": 120.0,
}

# Fallbacks específicos por nicho
_NICHE_FALLBACK_PARAMS = {
    "es_influence_shield": {
        "dense_fossil": {
            "base_cost": 120.0,
            "hit_probability": 0.18,
            "brick_risk": 0.12,
            "value_delta": 180.0,
        },
        "harvest_reforge": {
            "base_cost": 80.0,
            "hit_probability": 0.24,
            "brick_risk": 0.08,
            "value_delta": 150.0,
        },
        "essence": {
            "base_cost": 45.0,
            "hit_probability": 0.35,
            "brick_risk": 0.05,
            "value_delta": 120.0,
        },
    },
    "es_body_armour_influenced": {
        "dense_fossil": {
            "base_cost": 150.0,
            "hit_probability": 0.15,
            "brick_risk": 0.10,
            "value_delta": 200.0,
        },
        "harvest_reforge": {
            "base_cost": 100.0,
            "hit_probability": 0.20,
            "brick_risk": 0.08,
            "value_delta": 170.0,
        },
        "essence": {
            "base_cost": 60.0,
            "hit_probability": 0.30,
            "brick_risk": 0.05,
            "value_delta": 140.0,
        },
    },
    "suppress_evasion_chest": {
        "dense_fossil": {
            "base_cost": 100.0,
            "hit_probability": 0.20,
            "brick_risk": 0.12,
            "value_delta": 150.0,
        },
        "harvest_reforge": {
            "base_cost": 70.0,
            "hit_probability": 0.26,
            "brick_risk": 0.08,
            "value_delta": 130.0,
        },
        "essence": {
            "base_cost": 40.0,
            "hit_probability": 0.38,
            "brick_risk": 0.05,
            "value_delta": 110.0,
        },
    },
    "wand_plus_gems": {
        "dense_fossil": {
            "base_cost": 80.0,
            "hit_probability": 0.12,
            "brick_risk": 0.15,
            "value_delta": 140.0,
        },
        "harvest_reforge": {
            "base_cost": 60.0,
            "hit_probability": 0.18,
            "brick_risk": 0.10,
            "value_delta": 120.0,
        },
        "essence": {
            "base_cost": 35.0,
            "hit_probability": 0.25,
            "brick_risk": 0.08,
            "value_delta": 100.0,
        },
    },
}

_HARVEST_REFORGE_PARAMS_FALLBACK = {
    "base_cost": 80.0,
    "hit_probability": 0.24,
    "brick_risk": 0.08,
    "value_delta": 150.0,
}

_ESSENCE_PARAMS_FALLBACK = {
    "base_cost": 45.0,
    "hit_probability": 0.35,
    "brick_risk": 0.05,
    "value_delta": 120.0,
}

# Fallbacks específicos por nicho
_NICHE_FALLBACK_PARAMS = {
    "es_influence_shield": {
        "dense_fossil": {
            "base_cost": 120.0,
            "hit_probability": 0.18,
            "brick_risk": 0.12,
            "value_delta": 180.0,
        },
        "harvest_reforge": {
            "base_cost": 80.0,
            "hit_probability": 0.24,
            "brick_risk": 0.08,
            "value_delta": 150.0,
        },
        "essence": {
            "base_cost": 45.0,
            "hit_probability": 0.35,
            "brick_risk": 0.05,
            "value_delta": 120.0,
        },
    },
    "es_body_armour_influenced": {
        "dense_fossil": {
            "base_cost": 150.0,
            "hit_probability": 0.15,
            "brick_risk": 0.10,
            "value_delta": 200.0,
        },
        "harvest_reforge": {
            "base_cost": 100.0,
            "hit_probability": 0.20,
            "brick_risk": 0.08,
            "value_delta": 170.0,
        },
        "essence": {
            "base_cost": 60.0,
            "hit_probability": 0.30,
            "brick_risk": 0.05,
            "value_delta": 140.0,
        },
    },
    "suppress_evasion_chest": {
        "dense_fossil": {
            "base_cost": 100.0,
            "hit_probability": 0.20,
            "brick_risk": 0.12,
            "value_delta": 150.0,
        },
        "harvest_reforge": {
            "base_cost": 70.0,
            "hit_probability": 0.26,
            "brick_risk": 0.08,
            "value_delta": 130.0,
        },
        "essence": {
            "base_cost": 40.0,
            "hit_probability": 0.38,
            "brick_risk": 0.05,
            "value_delta": 110.0,
        },
    },
    "wand_plus_gems": {
        "dense_fossil": {
            "base_cost": 80.0,
            "hit_probability": 0.12,
            "brick_risk": 0.15,
            "value_delta": 140.0,
        },
        "harvest_reforge": {
            "base_cost": 60.0,
            "hit_probability": 0.18,
            "brick_risk": 0.10,
            "value_delta": 120.0,
        },
        "essence": {
            "base_cost": 35.0,
            "hit_probability": 0.25,
            "brick_risk": 0.08,
            "value_delta": 100.0,
        },
    },
}


# ============================================================================
# RESULTADOS
# ============================================================================


@dataclass(frozen=True)
class CraftMethodResult:
    """
    Resultado do cálculo de EV para um método de craft.

    Campos obrigatórios de segurança:
    - data_source: indica origem dos dados (repoe_verified, repoe_live, repoe_fallback)
    - used_fallback: bool indicando se fallback foi usado
    - fallback_reason: texto explicando por que fallback foi usado (ou "")
    """

    method_name: str
    hit_probability: float  # 0.0 - 1.0, 2 casas decimais
    expected_cost: float  # chaos, 1 casa decimal
    brick_risk: float  # 0.0 - 1.0, 2 casas decimais
    ev_net_value: float  # chaos, 1 casa decimal
    recommended: bool
    notes: str
    # Segurança e rastreabilidade
    data_source: str
    used_fallback: bool
    fallback_reason: str


# ============================================================================
# ENGINE
# ============================================================================


class ProbabilityEngine:
    """
    Motor de cálculo de EV para comparação de métodos de craft.

    Suporta múltiplos nichos configurados em NICHE_CONFIGS.
    Sempre indica fonte de dados e se fallback foi usado.
    """

    # Nichos disponíveis
    SUPPORTED_NICHES = list(NICHE_CONFIGS.keys())

    def __init__(self, niche: str = "es_influence_shield"):
        if niche not in NICHE_CONFIGS:
            raise ValueError(
                f"Nicho '{niche}' não suportado. Nichos disponíveis: {self.SUPPORTED_NICHES}"
            )

        self.niche = niche
        self.config = NICHE_CONFIGS[niche]
        self.item_tag = self.config["item_tag"]
        self._base_cost = self.config["base_cost"]
        self._target_sale_value = self.config["target_sale_value"]

        self._repoe_parser: Optional[RePoeParser] = None
        self._repoe_loaded = False
        self._used_fallback = False
        self._fallback_reason = ""

        self._init_repoe()

    def _init_repoe(self) -> None:
        """Tenta inicializar RePoE parser."""
        try:
            self._repoe_parser = RePoeParser(data_dir="data")
            self._repoe_loaded = bool(self._repoe_parser.db)
        except Exception as e:
            logger.warning(f"RePoE não disponível: {e}")
            self._repoe_loaded = False

        if not self._repoe_loaded:
            self._used_fallback = True
            self._fallback_reason = "RePoE local não disponível; usando fallback"

    def _validate_mod_weights(
        self, mod_ids: list[str], item_tag: str
    ) -> tuple[float, str]:
        """
        Valida se os mod_ids existem no RePoE e retorna soma dos spawn_weights
        E loga se peso = 0.

        Returns:
            tuple: (total_weight, source)
            - total_weight: soma dos spawn_weights para o item_tag
            - source: 'repoe_verified' se todos os mods existem, 'repoe_fallback' caso contrário
        """
        if not self._repoe_loaded or not self._repoe_parser:
            return 0.0, "repoe_fallback"

        total_weight = 0
        mods_found = []
        mods_missing = []

        for mod_id in mod_ids:
            weight = self._repoe_parser.get_spawn_weight_for_tag(mod_id, item_tag)
            if weight > 0:
                total_weight += weight
                mods_found.append(mod_id)
            else:
                mods_missing.append(mod_id)
                logger.warning(
                    f"[RePoE] Mod '{mod_id}' tem weight=0 para item_tag='{item_tag}'. "
                    f"Verifique se o mod existe no RePoE ou se a tag do item está correta."
                )

        if mods_missing:
            logger.warning(
                f"[RePoE] Mods não encontrados: {mods_missing}. "
                f"Usando fallback para cálculo."
            )
            return total_weight, "repoe_fallback"

        source = "repoe_verified" if mods_found else "repoe_fallback"
        return total_weight, source

    def _calculate_hit_probability(
        self,
        mod_ids: list[str],
        item_tag: str,
        generation_type: str,
        pool_groups: Optional[list[str]] = None,
    ) -> tuple[float, str, str]:
        """
        Calcula hit_probability usando spawn_weights do RePoE.

        Args:
            mod_ids: Lista de mod_ids alvo
            item_tag: Tag do item (ex: "dex_int_armour" para ES Shield)
            generation_type: "prefix" ou "suffix"
            pool_groups: Grupos para filtrar pool (opcional)

        Returns:
            tuple: (hit_probability, source, fallback_reason)
        """
        if not self._repoe_loaded or not self._repoe_parser:
            return 0.0, "repoe_fallback", "RePoE não carregado"

        # Validar mods e somar pesos
        mod_weight_sum, source = self._validate_mod_weights(mod_ids, item_tag)

        if mod_weight_sum == 0:
            return 0.0, "repoe_fallback", f"mods {mod_ids} têm weight=0 para {item_tag}"

        # Calcular pool total
        if pool_groups:
            total_pool = self._repoe_parser.get_total_spawn_weight_by_groups(
                item_tag, pool_groups, generation_type
            )
        else:
            total_pool = self._repoe_parser.get_total_spawn_weight_by_tag(
                item_tag, generation_type=generation_type
            )

        if total_pool == 0:
            return (
                0.0,
                "repoe_fallback",
                f"pool vazio para {item_tag}/{generation_type}",
            )

        # Probabilidade = peso dos mods / pool total
        hit_prob = mod_weight_sum / total_pool

        # Consistente com o nicho: caps em range razoável
        hit_prob = max(0.01, min(hit_prob, 0.99))

        return hit_prob, source, ""

    def _get_method_params(self, method_key: str) -> dict:
        """Retorna parâmetros de um método usando RePoE quando disponível."""
        # Pegar fallback do nicho específico ou usar padrão
        niche_fallbacks = _NICHE_FALLBACK_PARAMS.get(self.niche, {})
        method_fallback = niche_fallbacks.get(method_key, _ESSENCE_PARAMS_FALLBACK)

        params = {
            "base_cost": method_fallback["base_cost"],
            "hit_probability": method_fallback["hit_probability"],
            "brick_risk": method_fallback["brick_risk"],
            "value_delta": method_fallback["value_delta"],
        }

        # Obter configuração do mod para este método no nicho atual
        target_mods = self.config.get("target_mods", {})
        method_config = target_mods.get(method_key)

        if not method_config:
            # Método não configurado para este nicho - usar fallback total
            params["source"] = "repoe_fallback"
            params["fallback_reason"] = (
                f"Método '{method_key}' não disponível neste nicho"
            )
            return params

        mod_ids = method_config.get("mod_ids", [])
        generation_type = method_config.get("generation_type", "suffix")
        pool_groups = self.config.get("pool_groups")

        # Tentar calcular hit_probability via RePoE
        if self._repoe_loaded and self._repoe_parser and mod_ids:
            hit_prob, source, fallback_reason = self._calculate_hit_probability(
                mod_ids=mod_ids,
                item_tag=self.item_tag,
                generation_type=generation_type,
                pool_groups=pool_groups,
            )
            params["hit_probability"] = hit_prob
            params["source"] = source
            if fallback_reason:
                params["fallback_reason"] = fallback_reason
            else:
                params["fallback_reason"] = ""
        else:
            params["source"] = "repoe_fallback"
            params["fallback_reason"] = "RePoE não disponível ou mod_ids vazios"

        return params

    def calculate_ev(self, method_key: str, method_name: str) -> CraftMethodResult:
        """
        Calcula o EV líquido de um método.

        Fórmula:
        EV = (P_hit × Valor_delta) - Custo_base - (P_brick × Custo_base × 0.4)
        """
        params = self._get_method_params(method_key)

        p_hit = params["hit_probability"]
        p_brick = params["brick_risk"]
        base_cost = params["base_cost"]
        value_delta = params["value_delta"]
        source = params.get("source", "repoe_fallback")
        fallback_reason = params.get("fallback_reason", self._fallback_reason)
        used_fallback = source == "repoe_fallback"

        # Custo esperado
        expected_cost = base_cost

        # Ganho esperado
        expected_gain = p_hit * value_delta

        # Perda por brick (fração do valor do item base)
        expected_brick_loss = p_brick * (self._base_cost * 0.4)

        # EV líquido
        ev_net = expected_gain - expected_cost - expected_brick_loss

        return CraftMethodResult(
            method_name=method_name,
            hit_probability=round(p_hit, 4),
            expected_cost=round(expected_cost, 1),
            brick_risk=round(p_brick, 2),
            ev_net_value=round(ev_net, 1),
            recommended=False,
            notes=self._build_notes(method_key, source, fallback_reason),
            data_source=source,
            used_fallback=used_fallback,
            fallback_reason=fallback_reason if used_fallback else "",
        )

    def _build_notes(self, method_key: str, source: str, fallback_reason: str) -> str:
        """Constrói notas legíveis para o método."""
        notes_map = {
            "dense_fossil": "Dense Fossil: defences tag, competitivo para ES Shields.",
            "harvest_reforge": "Harvest Reforge Defence: mais direto, risco menor.",
            "essence": "Essence: alta chance, baixo risco, pool limitado.",
        }
        base_note = notes_map.get(method_key, "Método de craft.")

        if source == "repoe_verified":
            base_note += " [RePoE: dados verificados]"
        elif source == "repoe_live":
            base_note += " [RePoE: dados reais]"
        elif source == "repoe_fallback":
            base_note += f" [FALLBACK: {fallback_reason}]"

        return base_note

    def compare_methods(self) -> list[CraftMethodResult]:
        """
        Compara todos os métodos disponíveis no nicho e retorna ranking por EV líquido.
        """
        # Métodos disponíveis (podem variar por nicho no futuro)
        methods = [
            ("dense_fossil", "Dense Fossil"),
            ("harvest_reforge", "Harvest Reforge Defence"),
            ("essence", "Essence"),
        ]

        results = [self.calculate_ev(key, name) for key, name in methods]

        # Ordena por EV líquido descending
        results.sort(key=lambda r: r.ev_net_value, reverse=True)

        # Marca o melhor como recommended (se EV for positivo)
        if results and results[0].ev_net_value > 0:
            best = results[0]
            results[0] = CraftMethodResult(
                method_name=best.method_name,
                hit_probability=best.hit_probability,
                expected_cost=best.expected_cost,
                brick_risk=best.brick_risk,
                ev_net_value=best.ev_net_value,
                recommended=True,
                notes=best.notes,
                data_source=best.data_source,
                used_fallback=best.used_fallback,
                fallback_reason=best.fallback_reason,
            )

        return results

    def get_metadata(self) -> dict:
        """Retorna metadados do engine para output."""
        return {
            "niche": self.niche,
            "description": self.config.get("description", ""),
            "item_tag": self.item_tag,
            "base_cost": self._base_cost,
            "target_sale_value": self._target_sale_value,
            "pool_groups": self.config.get("pool_groups"),
            "data_source": "repoe_verified"
            if self._repoe_loaded and not self._used_fallback
            else "repoe_fallback",
            "used_fallback": self._used_fallback,
            "fallback_reason": self._fallback_reason if self._used_fallback else "",
            "supported_niches": self.SUPPORTED_NICHES,
        }


def create_engine(niche: str = "es_influence_shield") -> ProbabilityEngine:
    """Factory para criar o engine do nicho específico."""
    return ProbabilityEngine(niche=niche)
