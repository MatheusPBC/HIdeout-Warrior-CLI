from dataclasses import asdict, dataclass, field
from typing import Dict, List, Tuple

from core.market_scanner import OnDemandScanner, ScanOpportunity, ScanStats


@dataclass
class FlipTargetRecommendation:
    label: str
    goal_mods: List[str]
    expected_value: float
    confidence: float
    rationale: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class CraftStep:
    action_name: str
    target_mod: str
    cost_chaos: float
    probability: float
    expected_cost: float
    expected_value_after_step: float
    notes: str
    stop_here: bool = False

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class CraftPlan:
    opportunity: ScanOpportunity
    target: FlipTargetRecommendation
    steps: List[CraftStep]
    buy_cost: float
    expected_craft_cost: float
    expected_sale_value: float
    expected_profit: float
    plan_confidence: float
    stop_condition: str
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
            "plan_confidence": round(self.plan_confidence, 2),
            "stop_condition": self.stop_condition,
            "risk_notes": self.risk_notes,
            "alternatives": self.alternatives,
        }


class FlipAdvisor:
    """Planner econômico de flips guiado por heurística."""

    _FAMILY_PROFILES: Dict[str, Dict[str, object]] = {
        "wand_caster": {
            "label": "Caster Wand Flip",
            "goal_mods": ["SpellDamage1", "CastSpeed1", "CritChanceSpells1"],
            "premium": 190.0,
            "rationale": "Wands caster monetizam bem spell damage, cast speed e crit quando a base entra abaixo do mercado.",
        },
        "body_armour_defense": {
            "label": "Defensive Armour Flip",
            "goal_mods": ["Life1", "SpellSuppress1", "Resist1"],
            "premium": 210.0,
            "rationale": "Body armours com vida, suppress e resist tendem a vender perto da mediana alta do mercado.",
        },
        "jewel_cluster": {
            "label": "Jewel Utility Flip",
            "goal_mods": ["Life1", "CritChanceSpells1"],
            "premium": 120.0,
            "rationale": "Jewels e clusters com pares de mods vendáveis convertem bem upgrades pequenos em margem.",
        },
        "accessory_generic": {
            "label": "Accessory Fix-Up Flip",
            "goal_mods": ["Life1", "Resist1", "Attributes1"],
            "premium": 130.0,
            "rationale": "Accessories melhoram muito quando fecham vida, resist e um terceiro atributo útil.",
        },
        "generic": {
            "label": "Generic Utility Flip",
            "goal_mods": ["Life1", "Resist1"],
            "premium": 90.0,
            "rationale": "Quando a base não encaixa num archetype claro, priorizamos upgrades genéricos e vendáveis.",
        },
    }

    _ACTION_CATALOG = {
        "SpellDamage1": ("Essence spam", 35.0, 0.42, "Força spell damage de forma relativamente controlada."),
        "CastSpeed1": ("Harvest Reforge Speed", 20.0, 0.32, "Busca cast speed com um custo estável para flip de wand."),
        "CritChanceSpells1": ("Bench craft crit", 8.0, 1.0, "Fechamento barato quando sobra espaço útil."),
        "Life1": ("Essence of Greed", 18.0, 0.48, "Vida é o upgrade mais vendável para flips generalistas."),
        "SpellSuppress1": ("Harvest Reforge Defence", 24.0, 0.26, "Tenta consolidar suppress/defence em bases elegíveis."),
        "Resist1": ("Bench craft resistance", 4.0, 1.0, "Ajuste barato para fechar venda e viabilizar lucro."),
        "Attributes1": ("Bench craft attribute", 4.0, 1.0, "Ajuste barato para melhorar a liquidez do accessory."),
    }

    def __init__(self, league: str = "auto"):
        self.scanner = OnDemandScanner(league=league)

    def recommend_plans(
        self,
        item_class: str = "",
        ilvl_min: int = 1,
        rarity: str = "rare",
        max_items: int = 30,
        min_profit: float = 0.0,
        min_listed_price: float = 0.0,
        anti_fix: bool = True,
        safe_buy: bool = False,
        stale_hours: float = 48.0,
        budget: float = 150.0,
        top_plans: int = 3,
    ) -> Tuple[List[CraftPlan], ScanStats]:
        opportunities, stats = self.scanner.scan_opportunities(
            item_class=item_class,
            ilvl_min=ilvl_min,
            rarity=rarity,
            max_items=max_items,
            min_profit=min_profit,
            min_listed_price=min_listed_price,
            anti_fix=anti_fix,
            safe_buy=safe_buy,
            stale_hours=stale_hours,
        )
        plans = self.build_plans_from_opportunities(opportunities, budget=budget)
        return plans[:top_plans], stats

    def build_plans_from_opportunities(
        self,
        opportunities: List[ScanOpportunity],
        budget: float,
    ) -> List[CraftPlan]:
        plans: List[CraftPlan] = []
        ranked = sorted(
            opportunities,
            key=lambda opp: (opp.score, opp.trusted_profit, opp.profit),
            reverse=True,
        )

        for opportunity in ranked[:10]:
            plan = self._build_plan(opportunity, budget)
            if plan is not None:
                plans.append(plan)

        plans.sort(key=lambda plan: (plan.expected_profit, plan.plan_confidence), reverse=True)

        for index, plan in enumerate(plans):
            alternatives = []
            for alt in plans[index + 1 : index + 3]:
                alternatives.append(
                    f"{alt.opportunity.base_type}: lucro esperado {alt.expected_profit:.1f}c"
                )
            plan.alternatives = alternatives

        return plans

    def _build_plan(self, opportunity: ScanOpportunity, budget: float) -> CraftPlan | None:
        target = self._recommend_target(opportunity)
        current_mods = set(opportunity.mod_tokens)
        missing_mods = [mod for mod in target.goal_mods if mod not in current_mods]
        steps = self._build_steps(opportunity, target, missing_mods)

        expected_craft_cost = round(sum(step.expected_cost for step in steps), 1)
        if expected_craft_cost > budget:
            return None

        expected_sale_value = target.expected_value
        expected_profit = round(expected_sale_value - opportunity.listed_price - expected_craft_cost, 1)
        if expected_profit <= 0:
            return None

        market_confidence = 0.1 if opportunity.pricing_position == "below_floor" else 0.0
        plan_confidence = max(
            0.25,
            min(
                0.95,
                (opportunity.ml_confidence * 0.65)
                + (target.confidence * 0.2)
                + (self._step_confidence(steps) * 0.1)
                + market_confidence,
            ),
        )

        stop_condition = self._build_stop_condition(opportunity, steps, expected_profit)

        risk_notes = list(opportunity.risk_flags)
        if expected_craft_cost > budget * 0.8:
            risk_notes.append("near_budget_limit")
        if len(steps) >= 4:
            risk_notes.append("multi_step_execution")
        if opportunity.pricing_position == "outlier":
            risk_notes.append("weak_market_position")

        return CraftPlan(
            opportunity=opportunity,
            target=target,
            steps=steps,
            buy_cost=opportunity.listed_price,
            expected_craft_cost=expected_craft_cost,
            expected_sale_value=expected_sale_value,
            expected_profit=expected_profit,
            plan_confidence=round(plan_confidence, 2),
            stop_condition=stop_condition,
            risk_notes=risk_notes,
        )

    def _recommend_target(self, opportunity: ScanOpportunity) -> FlipTargetRecommendation:
        profile = self._FAMILY_PROFILES.get(opportunity.item_family, self._FAMILY_PROFILES["generic"])
        missing_count = sum(1 for mod in profile["goal_mods"] if mod not in opportunity.mod_tokens)

        valuation_value = opportunity.valuation_result.get("predicted_value", opportunity.ml_value)
        market_anchor = max(opportunity.market_median, opportunity.market_floor)
        expected_value = round(
            max(
                valuation_value + float(profile["premium"]),
                market_anchor + (missing_count * 28.0),
                opportunity.listed_price + 35.0 + (missing_count * 25.0),
            ),
            1,
        )
        confidence = max(
            0.4,
            min(
                0.92,
                opportunity.ml_confidence
                + (0.08 if opportunity.pricing_position == "below_floor" else 0.0)
                + (0.04 if opportunity.comparables_count >= 3 else -0.03)
                - (missing_count * 0.05),
            ),
        )

        rationale = str(profile["rationale"])
        if opportunity.market_median > 0:
            rationale += f" Mediana local observada: {opportunity.market_median:.1f}c."

        return FlipTargetRecommendation(
            label=str(profile["label"]),
            goal_mods=list(profile["goal_mods"]),
            expected_value=expected_value,
            confidence=round(confidence, 2),
            rationale=rationale,
        )

    def _build_steps(
        self,
        opportunity: ScanOpportunity,
        target: FlipTargetRecommendation,
        missing_mods: List[str],
    ) -> List[CraftStep]:
        steps: List[CraftStep] = []
        premium_total = max(target.expected_value - opportunity.ml_value, 0.0)
        cumulative_share = 0.0
        selected_mods = missing_mods[:3] if missing_mods else target.goal_mods[:2]

        for mod in selected_mods:
            action_name, cost, probability, notes = self._ACTION_CATALOG.get(
                mod,
                ("Bench stabilization", 6.0, 1.0, "Fechamento conservador para preservar margem."),
            )
            incremental_value = premium_total / max(len(selected_mods), 1)
            cumulative_share += incremental_value
            expected_value_after_step = round(opportunity.ml_value + cumulative_share, 1)
            expected_cost = round(cost / max(probability, 0.05), 1)
            steps.append(
                CraftStep(
                    action_name=action_name,
                    target_mod=mod,
                    cost_chaos=cost,
                    probability=round(probability, 2),
                    expected_cost=expected_cost,
                    expected_value_after_step=expected_value_after_step,
                    notes=notes,
                )
            )

        repair_step = self._repair_step(opportunity)
        if repair_step is not None:
            steps.insert(0, repair_step)

        self._mark_stop_point(opportunity, steps)
        return steps

    def _repair_step(self, opportunity: ScanOpportunity) -> CraftStep | None:
        if not opportunity.risk_flags:
            return None

        if "fractured" in opportunity.risk_flags or "influenced" in opportunity.risk_flags:
            return CraftStep(
                action_name="Annul / repair simple",
                target_mod="cleanup",
                cost_chaos=10.0,
                probability=0.55,
                expected_cost=18.2,
                expected_value_after_step=round(opportunity.ml_value + 20.0, 1),
                notes="Passo defensivo para limpar ou estabilizar a base antes de investir pesado.",
            )
        return None

    def _mark_stop_point(self, opportunity: ScanOpportunity, steps: List[CraftStep]) -> None:
        if not steps:
            return

        cumulative_cost = 0.0
        for step in steps:
            cumulative_cost += step.expected_cost
            interim_profit = step.expected_value_after_step - opportunity.listed_price - cumulative_cost
            if interim_profit > 0:
                step.stop_here = True
                break

    def _build_stop_condition(
        self,
        opportunity: ScanOpportunity,
        steps: List[CraftStep],
        expected_profit: float,
    ) -> str:
        cumulative_cost = 0.0
        for step in steps:
            cumulative_cost += step.expected_cost
            interim_profit = step.expected_value_after_step - opportunity.listed_price - cumulative_cost
            if step.stop_here or interim_profit >= (expected_profit * 0.65):
                return (
                    f"Pare e venda se após '{step.action_name}' o valor implícito atingir "
                    f"{step.expected_value_after_step:.1f}c com lucro >= {max(interim_profit, 0):.1f}c."
                )
        return "Siga até o alvo recomendado; não há stop-and-sell antecipado claramente superior."

    def _step_confidence(self, steps: List[CraftStep]) -> float:
        if not steps:
            return 0.5
        return sum(step.probability for step in steps) / len(steps)


