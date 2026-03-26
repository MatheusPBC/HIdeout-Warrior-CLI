"""
Probability Engine para craft-plan MVP.

Motor mínimo de cálculo de EV (Expected Value) para comparação de métodos de crafting.
NICHO MVP: es_influence_shield (Influenced Energy Shield)

Métodos comparados:
1. Dense Fossil - fossil-based synthesis
2. Harvest Reforge Defence - harvest augment defence
3. Essence - essence crafting

Segurança:
- Todos os resultados incluem data_source e used_fallback
- RePoEParser tentado primeiro; fallback explícito quando indisponível
- Arredondamento conservativo para evitar falsa precisão
"""

from dataclasses import dataclass
from typing import Optional

# Tentativa de usar RePoEParser existente; se falhar, usa fallback
try:
    from core.data_parser import RePoeParser

    _REPOE_AVAILABLE = True
except ImportError:
    _REPOE_AVAILABLE = False


# ============================================================================
# PROFILE: es_influence_shield (Influenced Energy Shield)
# ============================================================================

# Targets típicos para influenced shields com Energy Shield
_TARGET_MODS_ES_SHIELD = [
    "Spell Suppression",  # suffix defensivo premium
    "Maximum Energy Shield",  # prefix defensivo premium
    "Elemental Resistances",  # suffix utility
]

# Custo de base influenciada (shaper/elder) - valor de mercado típico
_BASE_COST_INFLUENCED_ES_SHIELD = 50.0  # chaos

# Valor de venda do item final com mods desejados (estimativa conservadora)
_TARGET_SALE_VALUE = 350.0  # chaos


# ============================================================================
# MAPEAMENTO RePoE: mod_id -> nome legível para o nicho
# ============================================================================
# Fonte: RePoE mods.json
# Tag relevante para Dense Fossil e Harvest Defence: "defence"
# ============================================================================
_REPOE_MOD_IDS = {
    # Spell Suppression - suffix defensivo premium
    "SpellSuppression1": "Spell Suppression",
    "SpellSuppression2": "Spell Suppression",
    "SpellSuppression3": "Spell Suppression",
    # Maximum Energy Shield - prefix defensivo
    "EnergyShield1": "Maximum Energy Shield",
    "EnergyShield2": "Maximum Energy Shield",
    "EnergyShield3": "Maximum Energy Shield",
    # Elemental Resistances - suffix utility
    "FireResist1": "Elemental Resistances",
    "FireResist2": "Elemental Resistances",
    "FireResist3": "Elemental Resistances",
    "ColdResist1": "Elemental Resistances",
    "ColdResist2": "Elemental Resistances",
    "LightningResist1": "Elemental Resistances",
    "LightningResist2": "Elemental Resistances",
}


# ============================================================================
# PARÂMETROS PROBABILÍSTICOS
# ============================================================================
# Fonte primária: RePoE (quando disponível)
# Fallback: dados de comunidade validados (hardcoded conservativo)
#
# Todos os valores são aproximados e indicativos.
# Não usar para decisões financeiras críticas sem validação adicional.
# ============================================================================

# Dense Fossil: usa tag "defence" no pool de generación
_DENSE_FOSSIL_PARAMS = {
    "base_cost": 120.0,
    "hit_probability": 0.18,  # ~18% - defesa tag em ES shield (FALLBACK)
    "brick_risk": 0.12,
    "value_delta": 180.0,
    "source": "repoe_fallback",  # indica fallback inicial; atualizado se RePoE disponível
    "tag": "defence",  # tag do pool de modificação
    "target_mods": ["SpellSuppression1", "SpellSuppression2", "SpellSuppression3"],
}

# Harvest Reforge Defence: usa tag "defence" (mesmo pool que Dense Fossil)
_HARVEST_REFORGE_DEFENCE_PARAMS = {
    "base_cost": 80.0,
    "hit_probability": 0.24,  # ~24% - reforge defence específico (FALLBACK)
    "brick_risk": 0.08,
    "value_delta": 150.0,
    "source": "repoe_fallback",
    "tag": "defence",
    "target_mods": ["SpellSuppression1", "SpellSuppression2", "SpellSuppression3"],
}

# Essence of Dread: modelagem imperfeita - usa pool próprio de essences
# Não há mapemaneto direto RePoE para este caso no MVP
_ESSENCE_DREAD_PARAMS = {
    "base_cost": 45.0,
    "hit_probability": 0.35,  # ~35% - essence de ES prefix (FALLBACK)
    "brick_risk": 0.05,
    "value_delta": 120.0,
    "source": "repoe_fallback",
    "tag": "",  # sem tag RePoE - usa pool próprio de essence
    "target_mods": [],  # não coberto pelo RePoE no MVP
}


# ============================================================================
# RESULTADOS
# ============================================================================


@dataclass(frozen=True)
class CraftMethodResult:
    """
    Resultado do cálculo de EV para um método de craft.

    Campos obrigatórios de segurança:
    - data_source: indica origem dos dados (repoe_live, repoe_cached, hardcoded_fallback)
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
    """

    def __init__(self, niche: str = "es_influence_shield"):
        self.niche = niche
        self.target_mod = "Spell Suppression"  # default para MVP
        self._repoe_parser: Optional[RePoeParser] = None
        self._repoe_loaded = False
        self._used_fallback = False
        self._fallback_reason = ""

        if _REPOE_AVAILABLE:
            self._init_repoe()

    def _init_repoe(self) -> None:
        """Tenta inicializar RePoE parser."""
        try:
            self._repoe_parser = RePoeParser(data_dir="data")
            # Tenta usar dados locais se existirem
            if not self._repoe_parser.db:
                self._repoe_parser._load_local_db()
            self._repoe_loaded = bool(self._repoe_parser.db)
        except Exception:
            self._repoe_loaded = False

        if not self._repoe_loaded:
            self._used_fallback = True
            self._fallback_reason = (
                "RePoE local não disponível ou vazio; usando fallback conservativo"
            )

    def _calculate_repoe_hit_probability(
        self, tag: str, target_mods: list[str]
    ) -> tuple[float, str, str]:
        """
        Calcula hit_probability usando RePoE real.

        Retorna:
            tuple: (hit_probability, source, fallback_reason)
            - hit_probability: 0.0 a 1.0
            - source: 'repoe_live' ou 'repoe_fallback'
            - fallback_reason: razão do fallback ou ""
        """
        if not self._repoe_loaded or not self._repoe_parser:
            return 0.18, "repoe_fallback", "RePoE não carregado"

        if not tag or not target_mods:
            return 0.18, "repoe_fallback", "tag ou mods não definidos para este método"

        # Soma pesos dos mods-alvo para a tag específica
        mod_weight_sum = 0
        found_mods = []
        for mod_id in target_mods:
            weight = self._repoe_parser.get_weight_for_tag(mod_id, tag)
            if weight > 0:
                mod_weight_sum += weight
                found_mods.append(mod_id)

        if not found_mods:
            return (
                0.18,
                "repoe_fallback",
                f"mods-alvo não encontrados no pool '{tag}' do RePoE",
            )

        # Soma total de todos os mods com essa tag (diluição do pool)
        total_tag_weight = self._repoe_parser.get_total_weight_by_tag(tag)

        if total_tag_weight == 0:
            return 0.18, "repoe_fallback", f"pool '{tag}' vazio no RePoE"

        # Probabilidade = peso dos mods-alvo / peso total do pool
        hit_prob = mod_weight_sum / total_tag_weight

        # Consistente com o nicho: caps em range razoável
        hit_prob = max(0.01, min(hit_prob, 0.99))

        return hit_prob, "repoe_live", ""

    def _get_method_params(self, method_key: str) -> dict:
        """
        Retorna parâmetros de um método.
        Tenta usar RePoE primeiro para hit_probability; fallback caso não disponível.
        """
        params_map = {
            "dense_fossil": _DENSE_FOSSIL_PARAMS,
            "harvest_reforge": _HARVEST_REFORGE_DEFENCE_PARAMS,
            "essence": _ESSENCE_DREAD_PARAMS,
        }

        base_params = params_map.get(method_key, params_map["essence"]).copy()

        # Tenta calcular hit_probability via RePoE se método tiver tag definida
        tag = base_params.get("tag", "")
        target_mods = base_params.get("target_mods", [])

        if tag and target_mods and self._repoe_loaded and self._repoe_parser:
            hit_prob, source, fallback_reason = self._calculate_repoe_hit_probability(
                tag, target_mods
            )
            base_params["hit_probability"] = hit_prob
            base_params["source"] = source
            if fallback_reason:
                base_params["fallback_reason"] = fallback_reason
        else:
            # Fallback por método (Harvest e Essence ainda não têm cobertura RePoE)
            base_params["source"] = "repoe_fallback"
            if method_key == "essence":
                base_params["fallback_reason"] = (
                    "Essence pool não mapeado no RePoE - usando dados comunidade"
                )
            elif not tag:
                base_params["fallback_reason"] = (
                    f"Método '{method_key}' sem tag RePoE definida"
                )

        return base_params

    def calculate_ev(self, method_key: str, method_name: str) -> CraftMethodResult:
        """
        Calcula o EV líquido de um método.

        Fórmula:
        EV = (P_hit × Valor_delta) - Custo_base - (P_brick × Custo_base × 0.4)

        O brick risk reduz o valor esperado porque nem sempre é perda total.
        """
        params = self._get_method_params(method_key)

        p_hit = params["hit_probability"]
        p_brick = params["brick_risk"]
        base_cost = params["base_cost"]
        value_delta = params["value_delta"]
        source = params["source"]

        # Custo esperado: pago independente de hit ou miss
        expected_cost = base_cost

        # Ganho esperado: só se hitar
        expected_gain = p_hit * value_delta

        # Perda por brick: fração do valor do item base
        # Porque brick raramente é perda total do item
        expected_brick_loss = p_brick * (_BASE_COST_INFLUENCED_ES_SHIELD * 0.4)

        # EV líquido
        ev_net = expected_gain - expected_cost - expected_brick_loss

        # FallbackReason: específico do método ou geral do engine
        method_fallback_reason = params.get("fallback_reason", "")
        fallback_reason = method_fallback_reason or (
            self._fallback_reason if self._used_fallback else ""
        )

        return CraftMethodResult(
            method_name=method_name,
            # Arredondamento conservativo para evitar falsa precisão
            hit_probability=round(p_hit, 2),
            expected_cost=round(expected_cost, 1),
            brick_risk=round(p_brick, 2),
            ev_net_value=round(ev_net, 1),
            recommended=False,  # marcado após comparação
            notes=self._build_notes(method_key, params, source),
            # Rastreabilidade de fonte
            data_source=source,
            used_fallback=(source == "repoe_fallback"),
            fallback_reason=fallback_reason,
        )

    def _build_notes(self, method_key: str, params: dict, source: str) -> str:
        """Constrói notas legíveis para o método."""
        notes_map = {
            "dense_fossil": "Dense Fossil: defence tag, competitivo para ES. Rola suffixes+prefixes.",
            "harvest_reforge": "Harvest Reforge Defence: mais direto, risco menor, custo menor.",
            "essence": "Essence of Dread: alta chance, baixo risco, mas pool de mods limitado.",
        }
        base_note = notes_map.get(method_key, "Método de craft.")

        # Indica fonte dos dados
        if source == "repoe_live":
            base_note += " [RePoE: dados reais]"
        elif source == "repoe_fallback":
            base_note += " [FALLBACK: dados aproximados]"
        return base_note

    def compare_methods(self) -> list[CraftMethodResult]:
        """
        Compara todos os métodos e retorna ranking por EV líquido.

        Marca o método com maior EV líquido como 'recommended'.
        Retorna todos os campos de rastreabilidade.
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
            "data_source": "hardcoded_fallback" if self._used_fallback else "repoe",
            "used_fallback": self._used_fallback,
            "fallback_reason": self._fallback_reason if self._used_fallback else "",
        }


def create_engine(niche: str = "es_influence_shield") -> ProbabilityEngine:
    """Factory para criar o engine do nicho específico."""
    return ProbabilityEngine(niche=niche)
