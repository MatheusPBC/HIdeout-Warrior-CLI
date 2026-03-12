from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List

from pydantic import BaseModel, Field


class ItemMeta(BaseModel):
    base_type: str = Field(..., description="Nome exato da base do item (ex: 'Omen Wand')")
    item_class: str = Field(..., description="Classe do item (ex: 'Wand', 'Belt')")
    min_ilvl: int = Field(default=1, description="Item level mínimo exigido")
    influence: List[str] = Field(default_factory=list, description="Tipos de influência (ex: 'Shaper', 'Elder')")


class AffixTarget(BaseModel):
    trade_api_id: str = Field(..., description="ID exato usado pela API oficial de Trade (pseudo ou explicit)")
    description: str = Field(default="", description="Descrição textual do mod para logs/CLI")
    min_tier: int = Field(default=1, description="O Tier mínimo aceitável (1 é o melhor, geralmente)")
    is_fractured_acceptable: bool = Field(default=False, description="Se for True, a base pode ser comprada com o afixo fraturado")
    weight: int = Field(default=0, description="Peso heurístico dinâmico atribuído ao afixo (1-100)")


class TargetStats(BaseModel):
    prefixes: List[AffixTarget] = Field(default_factory=list)
    suffixes: List[AffixTarget] = Field(default_factory=list)


class Constraints(BaseModel):
    open_prefixes_required: int = Field(default=0, description="Requisito de afixos de Prefixo livres ao final do craft (ex: pra craftar vida na bancada)")
    open_suffixes_required: int = Field(default=0, description="Requisito de afixos de Sufixo livres")
    max_crafting_budget_divines: float = Field(default=float('inf'), description="Orçamento máximo (EV) em Divines antes do Motor Grafo abandonar o craft")


class CraftingTargetSchema(BaseModel):
    """
    Data Contract mestre que define o "Estado Final" (Nó de Destino)
    para o Módulo B (Graph Engine) e os pesos para o Módulo A (Hospital Snipe).
    """

    item_meta: ItemMeta
    target_stats: TargetStats
    constraints: Constraints


@dataclass
class FlipTargetRecommendation:
    label: str
    goal_mods: List[str]
    expected_value: float
    confidence: float
    rationale: str
    requires_suppression: bool = False
    required_link_count: int = 0

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class CraftActionEvaluation:
    action_type: str
    action_name: str
    target_mod: str
    eligibility: bool
    failure_reason: str
    expected_cost: float
    expected_value_delta: float
    brick_risk: float
    confidence_delta: float
    probability: float
    expected_value_after_step: float
    notes: str
    stop_here: bool = False

    def to_dict(self) -> dict:
        payload = asdict(self)
        payload["cost_chaos"] = round(self.expected_cost, 1)
        return payload


@dataclass
class ExitMarketEstimate:
    expected_sale_value: float
    market_floor: float
    market_median: float
    comparables_count: int
    pricing_position: str
    evidence_strength: str
    oracle_confidence: float = 0.0
    oracle_model_source: str = ""
    required_structure: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class PlanConfidenceBreakdown:
    base_confidence: float
    craft_confidence: float
    exit_confidence: float
    overall_confidence: float

    @classmethod
    def compose(
        cls,
        *,
        base_confidence: float,
        craft_confidence: float,
        exit_confidence: float,
        base_weight: float = 0.3,
        craft_weight: float = 0.3,
        exit_weight: float = 0.4,
        minimum: float = 0.0,
        maximum: float = 0.92,
    ) -> "PlanConfidenceBreakdown":
        weighted = (
            (base_confidence * base_weight)
            + (craft_confidence * craft_weight)
            + (exit_confidence * exit_weight)
        )
        overall_confidence = max(minimum, min(maximum, weighted))
        return cls(
            base_confidence=round(base_confidence, 2),
            craft_confidence=round(craft_confidence, 2),
            exit_confidence=round(exit_confidence, 2),
            overall_confidence=round(overall_confidence, 2),
        )

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class CraftPlan:
    opportunity: Any
    target: FlipTargetRecommendation
    steps: List[CraftActionEvaluation]
    buy_cost: float
    expected_craft_cost: float
    expected_sale_value: float
    expected_profit: float
    trusted_profit: float
    plan_confidence: float
    confidence_breakdown: PlanConfidenceBreakdown
    exit_estimate: ExitMarketEstimate
    stop_condition: str
    plan_explanation: str
    risk_notes: List[str] = field(default_factory=list)
    alternatives: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "opportunity": self.opportunity.to_dict(),
            "target": self.target.to_dict(),
            "steps": [step.to_dict() for step in self.steps],
            "buy_cost": round(self.buy_cost, 1),
            "expected_craft_cost": round(self.expected_craft_cost, 1),
            "expected_sale_value": round(self.expected_sale_value, 1),
            "expected_profit": round(self.expected_profit, 1),
            "trusted_profit": round(self.trusted_profit, 1),
            "plan_confidence": round(self.plan_confidence, 2),
            "confidence_breakdown": self.confidence_breakdown.to_dict(),
            "exit_estimate": self.exit_estimate.to_dict(),
            "stop_condition": self.stop_condition,
            "plan_explanation": self.plan_explanation,
            "risk_notes": self.risk_notes,
            "alternatives": self.alternatives,
        }
