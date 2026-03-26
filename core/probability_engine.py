"""
Probability Engine para craft-plan MVP.

Motor de cálculo de EV (Expected Value) para comparação de métodos de crafting.
NICHO MVP: es_influence_shield (Influenced Energy Shield)

Métodos comparados:
1. Dense Fossil - fossil-based synthesis
2. Harvest Reforge Defence - harvest augment defence
3. Essence - essence crafting

Segurança:
- Todos os resultados incluem data_source e used_fallback
- RePoEParser tentado primeiro; fallback explícito quando indisponível
- Arredondamento conservativo para evitar falsa precisão

Bloco 6: Endurecer o nicho com RePoE real
- Mod IDs mapeados corretamente (ChanceToSuppressSpells, LocalIncreasedEnergyShieldPercent)
- spawn_weights no lugar de generation_weights
- Filtragem por item_tag (dex_int_armour para ES Shield)
"""

import logging
from dataclasses import dataclass
from typing import Optional

from core.data_parser import RePoeParser, REPOE_MOD_IDS, DENSE_FOSSIL_POSITIVE_TAG

logger = logging.getLogger(__name__)


# ============================================================================
# PROFILE: es_influence_shield (Influenced Energy Shield)
# ============================================================================

# Tipo de item para ES Shield (Energy Shield + Dex/Int)
ES_SHIELD_ITEM_TAG = "dex_int_armour"  # ES Shield típico

# Custo de base influenciada (shaper/elder) - valor de mercado típico
_BASE_COST_INFLUENCED_ES_SHIELD = 50.0  # chaos

# Valor de venda do item final com mods desejados (estimativa conservadora)
_TARGET_SALE_VALUE = 350.0  # chaos


# ============================================================================
# MAPEAMENTO: Mod IDs REPOE VERIFICADOS
# ============================================================================
# Fonte: RePoE mods.json - spawn_weights verificados
# ============================================================================

# Spell Suppression Suffix (alvo principal para Dense Fossil/Harvest)
_SPELL_SUPPRESSION_MOD_IDS = REPOE_MOD_IDS["spell_suppression"]["mod_ids"]

# Energy Shield % Prefix (alvo principal para Essence)
_ES_PERCENT_MOD_IDS = REPOE_MOD_IDS["energy_shield_percent"]["mod_ids"]

# Grupos de mods para cálculo de pool por grupo
_DEFENCE_MOD_GROUPS = ["DefencesPercent", "DefencesFlat", "DefencesAnd"]
_SPELL_SUPPRESS_GROUP = ["ChanceToSuppressSpells"]


# ============================================================================
# PARÂMETROS PROBABILÍSTICOS - FALLBACKS
# ============================================================================
# Usados quando RePoE não está disponível ou dados incompletos
# ============================================================================

from typing import TypedDict


class MethodParams(TypedDict, total=False):
    base_cost: float
    hit_probability: float
    brick_risk: float
    value_delta: float
    source: str
    fallback_reason: str


_DENSE_FOSSIL_PARAMS_FALLBACK: MethodParams = {
    "base_cost": 120.0,
    "hit_probability": 0.18,
    "brick_risk": 0.12,
    "value_delta": 180.0,
}

_HARVEST_REFORGE_PARAMS_FALLBACK: MethodParams = {
    "base_cost": 80.0,
    "hit_probability": 0.24,
    "brick_risk": 0.08,
    "value_delta": 150.0,
}

_ESSENCE_DREAD_PARAMS_FALLBACK: MethodParams = {
    "base_cost": 45.0,
    "hit_probability": 0.35,
    "brick_risk": 0.05,
    "value_delta": 120.0,
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

    MVP: compara 3 métodos predefined, retorna ranking por EV líquido.
    Sempre indica fonte de dados e se fallback foi usado.

    Bloco 6: Integração com RePoE real usando spawn_weights corretos.
    """

    def __init__(self, niche: str = "es_influence_shield"):
        self.niche = niche
        self.target_mod = "Spell Suppression"  # default para MVP
        self.item_tag = ES_SHIELD_ITEM_TAG
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
        # Parâmetros base (custos, brick_risk, value_delta)
        base_params = {
            "dense_fossil": _DENSE_FOSSIL_PARAMS_FALLBACK.copy(),
            "harvest_reforge": _HARVEST_REFORGE_PARAMS_FALLBACK.copy(),
            "essence": _ESSENCE_DREAD_PARAMS_FALLBACK.copy(),
        }

        params = base_params.get(method_key, base_params["essence"]).copy()

        # Tentar calcular hit_probability via RePoE
        if self._repoe_loaded and self._repoe_parser:
            if method_key in ("dense_fossil", "harvest_reforge"):
                # Dense Fossil e Harvest usam Spell Suppression como alvo
                hit_prob, source, fallback_reason = self._calculate_hit_probability(
                    mod_ids=_SPELL_SUPPRESSION_MOD_IDS,
                    item_tag=self.item_tag,
                    generation_type="suffix",
                    pool_groups=_SPELL_SUPPRESS_GROUP,
                )
                params["hit_probability"] = hit_prob
                params["source"] = source
                if fallback_reason:
                    params["fallback_reason"] = fallback_reason
                else:
                    params["fallback_reason"] = ""

            elif method_key == "essence":
                # Essence: ES% prefix
                # Nota: Essence tem pool próprio e não é afetado por spawn_weights normais
                # Usamos fallback para Essence no MVP
                params["source"] = "repoe_fallback"
                params["fallback_reason"] = (
                    "Essence pool não mapeado no RePoE - usando dados comunidade"
                )
        else:
            params["source"] = "repoe_fallback"
            params["fallback_reason"] = "RePoE não disponível"

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
        expected_brick_loss = p_brick * (_BASE_COST_INFLUENCED_ES_SHIELD * 0.4)

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
            "essence": "Essence of Dread: alta chance, baixo risco, pool limitado.",
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
        Compara todos os métodos e retorna ranking por EV líquido.
        """
        methods = [
            ("dense_fossil", "Dense Fossil"),
            ("harvest_reforge", "Harvest Reforge Defence"),
            ("essence", "Essence of Dread"),
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
            "target_mod": self.target_mod,
            "item_tag": self.item_tag,
            "data_source": "repoe_verified"
            if self._repoe_loaded and not self._used_fallback
            else "repoe_fallback",
            "used_fallback": self._used_fallback,
            "fallback_reason": self._fallback_reason if self._used_fallback else "",
        }


def create_engine(niche: str = "es_influence_shield") -> ProbabilityEngine:
    """Factory para criar o engine do nicho específico."""
    return ProbabilityEngine(niche=niche)
