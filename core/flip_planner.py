from typing import Dict, List, Tuple

from core.item_normalizer import NormalizedMarketItem
from core.market_scanner import OnDemandScanner, ScanOpportunity, ScanStats
from core.ml_oracle import PricePredictor
from core.models import (
    CraftActionEvaluation,
    CraftPlan,
    ExitMarketEstimate,
    FlipTargetRecommendation,
    PlanConfidenceBreakdown,
)


class FlipAdvisor:
    _DEFAULT_SIX_LINK_FUSINGS = 1200.0
    _FAMILY_PROFILES: Dict[str, Dict[str, object]] = {
        "wand_caster": {
            "label": "Caster Wand Flip",
            "goal_mods": ["SpellDamage1", "CastSpeed1", "CritChanceSpells1"],
            "rationale": "Wands caster convertem upgrades controlados em margem quando entram abaixo do mercado.",
        },
        "body_armour_defense": {
            "label": "Defensive Armour Flip",
            "goal_mods": ["Life1", "Resist1"],
            "rationale": "Body armours precisam de mods plausíveis para a base e saída suportada por mercado comparável.",
        },
        "jewel_cluster": {
            "label": "Jewel Utility Flip",
            "goal_mods": ["Life1", "CritChanceSpells1"],
            "rationale": "Jewels e clusters exigem saída consistente, então o planner reduz upside sem evidência.",
        },
        "accessory_generic": {
            "label": "Accessory Fix-Up Flip",
            "goal_mods": ["Life1", "Resist1", "Attributes1"],
            "rationale": "Accessories vendem bem quando fecham vida, resist e utilidade sem extrapolar o mercado final.",
        },
        "generic": {
            "label": "Generic Utility Flip",
            "goal_mods": ["Life1", "Resist1"],
            "rationale": "Quando não há archetype forte, o planner usa upgrades genéricos e conservadores.",
        },
    }
    _ACTION_CATALOG: Dict[str, Dict[str, object]] = {
        "SpellDamage1": {
            "action_type": "essence",
            "action_name": "Essence spell",
            "base_cost": 30.0,
            "probability": 0.42,
            "value_delta": 34.0,
            "brick_risk": 0.22,
            "notes": "Força spell damage de forma relativamente controlada.",
        },
        "CastSpeed1": {
            "action_type": "harvest_reforge",
            "action_name": "Harvest Reforge Speed",
            "base_cost": 20.0,
            "probability": 0.33,
            "value_delta": 28.0,
            "brick_risk": 0.28,
            "notes": "Busca cast speed preservando o upside do item.",
        },
        "CritChanceSpells1": {
            "action_type": "bench_craft",
            "action_name": "Bench craft crit",
            "base_cost": 8.0,
            "probability": 1.0,
            "value_delta": 18.0,
            "brick_risk": 0.02,
            "notes": "Fechamento barato quando sobra espaço útil.",
        },
        "Life1": {
            "action_type": "essence",
            "action_name": "Essence of Greed",
            "base_cost": 18.0,
            "probability": 0.48,
            "value_delta": 30.0,
            "brick_risk": 0.18,
            "notes": "Vida é o upgrade mais líquido para body armours e accessories.",
        },
        "SpellSuppress1": {
            "action_type": "harvest_reforge",
            "action_name": "Harvest Reforge Defence",
            "base_cost": 24.0,
            "probability": 0.24,
            "value_delta": 42.0,
            "brick_risk": 0.30,
            "notes": "Suppression só entra em bases com componente de Evasion/Dex.",
        },
        "Resist1": {
            "action_type": "bench_craft",
            "action_name": "Bench craft resistance",
            "base_cost": 4.0,
            "probability": 1.0,
            "value_delta": 16.0,
            "brick_risk": 0.01,
            "notes": "Fechamento barato para estabilizar a venda.",
        },
        "Attributes1": {
            "action_type": "bench_craft",
            "action_name": "Bench craft attribute",
            "base_cost": 4.0,
            "probability": 1.0,
            "value_delta": 12.0,
            "brick_risk": 0.01,
            "notes": "Ajuste barato para melhorar a liquidez do item.",
        },
    }
    _MOD_TEXT_BY_TOKEN: Dict[str, str] = {
        "SpellDamage1": "Adds high spell damage",
        "CastSpeed1": "Adds cast speed",
        "CritChanceSpells1": "Adds spell critical strike chance",
        "Life1": "+# to maximum Life",
        "SpellSuppress1": "+# chance to Suppress Spell Damage",
        "Resist1": "+#% to Elemental Resistances",
        "Attributes1": "+# to all Attributes",
        "Mana1": "+# to maximum Mana",
    }
    _PREFIX_MODS = {"SpellDamage1", "Life1", "Mana1", "SpellDamage", "Life", "Mana"}
    _SUFFIX_MODS = {
        "CastSpeed1",
        "CritChanceSpells1",
        "SpellSuppress1",
        "Resist1",
        "Attributes1",
        "CastSpeed",
        "CritChanceSpells",
        "SpellSuppress",
        "Resist",
        "Attributes",
    }

    def __init__(self, league: str = "auto"):
        self.predictor = PricePredictor()
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
        self, opportunities: List[ScanOpportunity], budget: float
    ) -> List[CraftPlan]:
        plans: List[CraftPlan] = []
        market_index = self._build_market_index(opportunities)
        ranked = sorted(
            opportunities,
            key=lambda opp: (opp.score, opp.trusted_profit, opp.profit),
            reverse=True,
        )
        for opportunity in ranked[:12]:
            plan = self._build_plan(opportunity, budget, market_index)
            if plan is not None:
                plans.append(plan)
        plans.sort(
            key=lambda plan: (
                plan.trusted_profit,
                plan.confidence_breakdown.overall_confidence,
                plan.expected_profit,
            ),
            reverse=True,
        )
        for index, plan in enumerate(plans):
            plan.alternatives = [
                f"{alt.opportunity.base_type}: trusted {alt.trusted_profit:.1f}c"
                for alt in plans[index + 1 : index + 3]
            ]
        return plans

    def _build_plan(
        self,
        opportunity: ScanOpportunity,
        budget: float,
        market_index: Dict[tuple[str, str, int, str], List[float]],
    ) -> CraftPlan | None:
        target = self._recommend_target(opportunity, budget)
        stage_a_ok, stage_a_reasons = self._passes_stage_a(opportunity, target, budget)
        if not stage_a_ok:
            return None
        current_mods = set(opportunity.mod_tokens)
        missing_mods = [mod for mod in target.goal_mods if mod not in current_mods]
        steps = self._build_steps(opportunity, target, missing_mods)
        if any(not step.eligibility for step in steps):
            return None
        expected_craft_cost = round(sum(step.expected_cost for step in steps), 1)
        exit_estimate = self._build_exit_estimate(
            opportunity, target, steps, market_index, budget, expected_craft_cost
        )
        if exit_estimate is None:
            return None
        if exit_estimate.required_structure.get(
            "link_count", self._link_count(opportunity)
        ) > self._link_count(opportunity):
            remaining_delta = max(
                exit_estimate.expected_sale_value
                - self._current_market_anchor(opportunity)
                - sum(step.expected_value_delta for step in steps),
                0.0,
            )
            linking_step = self._build_linking_step(
                opportunity,
                int(exit_estimate.required_structure["link_count"]),
                remaining_delta,
            )
            if linking_step is None or not linking_step.eligibility:
                return None
            steps.append(linking_step)
            expected_craft_cost = round(sum(step.expected_cost for step in steps), 1)
        if expected_craft_cost > budget:
            return None
        self._rebase_step_values(opportunity, steps, exit_estimate.expected_sale_value)
        expected_profit = round(
            exit_estimate.expected_sale_value
            - opportunity.listed_price
            - expected_craft_cost,
            1,
        )
        if expected_profit <= 0:
            return None
        confidence_breakdown = self._build_confidence_breakdown(
            opportunity, steps, exit_estimate, target, budget
        )
        stage_b_ok, stage_b_reasons = self._passes_stage_b(
            opportunity,
            target,
            steps,
            exit_estimate,
            confidence_breakdown,
            expected_profit,
            budget,
        )
        if not stage_b_ok:
            return None
        trusted_profit = round(
            expected_profit * confidence_breakdown.overall_confidence, 1
        )
        self._mark_stop_point(opportunity, steps, expected_profit)
        stop_condition = self._build_stop_condition(opportunity, steps, expected_profit)
        risk_notes = list(
            dict.fromkeys(opportunity.risk_flags + stage_a_reasons + stage_b_reasons)
        )
        if exit_estimate.required_structure.get("link_count", 0) > self._link_count(
            opportunity
        ):
            risk_notes.append("requires_linking")
        if exit_estimate.evidence_strength == "weak":
            risk_notes.append("weak_exit_evidence")
        plan_explanation = self._build_plan_explanation(
            opportunity, target, steps, exit_estimate, confidence_breakdown
        )
        return CraftPlan(
            opportunity=opportunity,
            target=target,
            steps=steps,
            buy_cost=opportunity.listed_price,
            expected_craft_cost=expected_craft_cost,
            expected_sale_value=round(exit_estimate.expected_sale_value, 1),
            expected_profit=expected_profit,
            trusted_profit=trusted_profit,
            plan_confidence=round(confidence_breakdown.overall_confidence, 2),
            confidence_breakdown=confidence_breakdown,
            exit_estimate=exit_estimate,
            stop_condition=stop_condition,
            plan_explanation=plan_explanation,
            risk_notes=risk_notes,
        )

    def _recommend_target(
        self, opportunity: ScanOpportunity, budget: float | None = None
    ) -> FlipTargetRecommendation:
        profile = self._FAMILY_PROFILES.get(
            opportunity.item_family, self._FAMILY_PROFILES["generic"]
        )
        if opportunity.item_family == "body_armour_defense":
            suppression_eligible = self._suppression_eligible(opportunity)
            goal_mods = ["Life1", "Resist1"]
            label = "Defensive Armour Flip"
            rationale_bits = [
                f"Base {opportunity.base_type} ({self._defence_profile(opportunity)}/{self._attribute_profile(opportunity)})",
                f"{self._link_count(opportunity)}L atual",
            ]
            suppression_ev_cost = float(
                self._ACTION_CATALOG["SpellSuppress1"]["base_cost"]
            ) / max(float(self._ACTION_CATALOG["SpellSuppress1"]["probability"]), 0.05)
            allow_suppression = (
                suppression_eligible
                and opportunity.ilvl >= 84
                and (budget is None or suppression_ev_cost <= (budget * 0.6))
            )
            if allow_suppression:
                goal_mods = ["Life1", "SpellSuppress1", "Resist1"]
                label = "Suppress Armour Flip"
                rationale_bits.append(
                    "base pode rolar Spell Suppression e o orçamento comporta esse passo"
                )
            else:
                rationale_bits.append(
                    "planner não exige suppression nesta base ou neste orçamento"
                )
            oracle_result = self.predictor.predict(
                self._simulate_target_item(opportunity, goal_mods=goal_mods)
            )
            confidence = self._clamp(
                (oracle_result.confidence * 0.6)
                + (opportunity.ml_confidence * 0.25)
                + (0.08 if opportunity.comparables_count >= 3 else -0.02)
                - (0.05 if opportunity.low_ilvl_context else 0.0),
                0.35,
                0.9,
            )
            rationale_bits.append(
                f"oráculo {oracle_result.model_source} projetou {oracle_result.predicted_value:.1f}c para a base-alvo"
            )
            return FlipTargetRecommendation(
                label=label,
                goal_mods=goal_mods,
                expected_value=round(oracle_result.predicted_value, 1),
                confidence=round(confidence, 2),
                rationale=". ".join(rationale_bits) + ".",
                requires_suppression=("SpellSuppress1" in goal_mods),
                required_link_count=self._link_count(opportunity),
            )
        goal_mods = list(profile["goal_mods"])
        oracle_result = self.predictor.predict(
            self._simulate_target_item(opportunity, goal_mods=goal_mods)
        )
        confidence = self._clamp(
            (oracle_result.confidence * 0.55)
            + (opportunity.ml_confidence * 0.25)
            + (0.04 if opportunity.comparables_count >= 3 else -0.03),
            0.35,
            0.9,
        )
        rationale = str(profile["rationale"])
        if opportunity.market_median > 0:
            rationale += f" Mediana local observada: {opportunity.market_median:.1f}c."
        rationale += f" Oráculo {oracle_result.model_source} projeta {oracle_result.predicted_value:.1f}c para o alvo sintético."
        return FlipTargetRecommendation(
            label=str(profile["label"]),
            goal_mods=goal_mods,
            expected_value=round(oracle_result.predicted_value, 1),
            confidence=round(confidence, 2),
            rationale=rationale,
        )

    def _passes_stage_a(
        self,
        opportunity: ScanOpportunity,
        target: FlipTargetRecommendation,
        budget: float,
    ) -> Tuple[bool, List[str]]:
        reasons: List[str] = []
        if opportunity.profit <= 0:
            reasons.append("base_not_discounted")
        if opportunity.item_family == "body_armour_defense":
            if opportunity.low_ilvl_context and not opportunity.twink_override:
                reasons.append("body_armour_low_ilvl")
            if target.requires_suppression and not self._suppression_eligible(
                opportunity
            ):
                reasons.append("suppression_base_ineligible")
            if opportunity.comparables_count <= 0 and opportunity.market_floor <= 0:
                reasons.append("body_armour_low_liquidity")
            if self._link_count(opportunity) <= 0 and opportunity.comparables_count < 2:
                reasons.append("linkless_low_evidence")
            if (
                self._link_count(opportunity) < 6
                and self._six_link_cost_chaos() > budget
                and opportunity.market_median <= opportunity.listed_price
            ):
                reasons.append("insufficient_budget_for_linking")
        return (not reasons, reasons)

    def _build_steps(
        self,
        opportunity: ScanOpportunity,
        target: FlipTargetRecommendation,
        missing_mods: List[str],
    ) -> List[CraftActionEvaluation]:
        steps: List[CraftActionEvaluation] = []
        selected_mods = missing_mods[:3] if missing_mods else target.goal_mods[:2]
        for mod in selected_mods:
            steps.append(self._evaluate_action(opportunity, mod))
        repair_step = self._repair_step(opportunity)
        if repair_step is not None:
            steps.insert(0, repair_step)
        return steps

    def _evaluate_action(
        self, opportunity: ScanOpportunity, mod: str
    ) -> CraftActionEvaluation:
        config = self._ACTION_CATALOG.get(
            mod,
            {
                "action_type": "bench_craft",
                "action_name": "Bench stabilization",
                "base_cost": 6.0,
                "probability": 1.0,
                "value_delta": 10.0,
                "brick_risk": 0.02,
                "notes": "Fechamento conservador para preservar margem.",
            },
        )
        eligible = True
        failure_reason = ""
        notes = str(config["notes"])
        if mod == "SpellSuppress1" and not self._suppression_eligible(opportunity):
            eligible = False
            failure_reason = "suppression_base_ineligible"
            notes = "Spell Suppression só pode rolar em bases com Evasion/Dex."
        elif (
            mod in {"Resist1", "Attributes1", "CritChanceSpells1"}
            and opportunity.open_suffixes <= 0
        ):
            notes += " Base sem suffix aberto; pode exigir recraft parcial."
        elif mod == "Life1" and opportunity.open_prefixes <= 0:
            notes += " Base sem prefix aberto; pode exigir recraft parcial."
        probability = float(config["probability"]) if eligible else 0.0
        expected_cost = (
            round(float(config["base_cost"]) / max(probability, 0.05), 1)
            if eligible
            else 0.0
        )
        expected_value_delta = (
            round(float(config["value_delta"]), 1) if eligible else 0.0
        )
        return CraftActionEvaluation(
            action_type=str(config["action_type"]),
            action_name=str(config["action_name"]),
            target_mod=mod,
            eligibility=eligible,
            failure_reason=failure_reason,
            expected_cost=expected_cost,
            expected_value_delta=expected_value_delta,
            brick_risk=float(config["brick_risk"]),
            confidence_delta=round(
                (probability * 0.12) - (float(config["brick_risk"]) * 0.08), 2
            ),
            probability=round(probability, 2),
            expected_value_after_step=0.0,
            notes=notes,
        )

    def _repair_step(
        self, opportunity: ScanOpportunity
    ) -> CraftActionEvaluation | None:
        if not opportunity.risk_flags:
            return None
        if (
            "fractured" in opportunity.risk_flags
            or "influenced" in opportunity.risk_flags
        ):
            return CraftActionEvaluation(
                action_type="annul_repair",
                action_name="Annul / repair simple",
                target_mod="cleanup",
                eligibility=True,
                failure_reason="",
                expected_cost=18.2,
                expected_value_delta=12.0,
                brick_risk=0.2,
                confidence_delta=-0.04,
                probability=0.55,
                expected_value_after_step=0.0,
                notes="Passo defensivo para limpar ou estabilizar a base antes de investir pesado.",
            )
        return None

    def _build_linking_step(
        self, opportunity: ScanOpportunity, target_link_count: int, value_delta: float
    ) -> CraftActionEvaluation | None:
        if target_link_count <= self._link_count(opportunity):
            return None
        if target_link_count != 6:
            return None
        expected_cost = round(self._six_link_cost_chaos(), 1)
        return CraftActionEvaluation(
            action_type="socket_linking",
            action_name="Socket linking to 6-link",
            target_mod=f"{target_link_count}L",
            eligibility=True,
            failure_reason="",
            expected_cost=expected_cost,
            expected_value_delta=round(max(value_delta, 0.0), 1),
            brick_risk=0.05,
            confidence_delta=-0.1,
            probability=1.0,
            expected_value_after_step=0.0,
            notes=f"Inclui custo esperado de {self._DEFAULT_SIX_LINK_FUSINGS:.0f} Orbs of Fusing convertido para chaos.",
        )

    def _build_exit_estimate(
        self,
        opportunity: ScanOpportunity,
        target: FlipTargetRecommendation,
        steps: List[CraftActionEvaluation],
        market_index: Dict[tuple[str, str, int, str], List[float]],
        budget: float,
        expected_craft_cost: float,
    ) -> ExitMarketEstimate | None:
        link_options = [self._link_count(opportunity)]
        if (
            opportunity.item_family == "body_armour_defense"
            and self._link_count(opportunity) < 6
        ):
            link_options.append(6)
        best_candidate: tuple[float, ExitMarketEstimate] | None = None
        for link_count in link_options:
            prices = list(
                market_index.get(
                    self._market_key(
                        opportunity.item_family,
                        opportunity.base_type,
                        link_count,
                        opportunity.ilvl,
                    ),
                    [],
                )
            )
            if link_count == self._link_count(opportunity) and not prices:
                prices = [
                    price
                    for price in (opportunity.market_floor, opportunity.market_median)
                    if price > 0
                ]
            if link_count != self._link_count(opportunity) and not prices:
                continue
            market_floor, market_median, market_spread, comparables_count = (
                self._market_stats_from_prices(prices)
            )
            if link_count == self._link_count(opportunity):
                market_floor = max(market_floor, opportunity.market_floor)
                market_median = max(market_median, opportunity.market_median)
                market_spread = max(market_spread, opportunity.market_spread)
                comparables_count = max(
                    comparables_count, opportunity.comparables_count
                )
            if comparables_count <= 0:
                comparables_count = opportunity.comparables_count
                market_floor = opportunity.market_floor
                market_median = opportunity.market_median
                market_spread = opportunity.market_spread
            if (
                link_count > self._link_count(opportunity)
                and self._six_link_cost_chaos() > budget
            ):
                continue
            synthetic_item = self._simulate_target_item(
                opportunity, goal_mods=target.goal_mods, link_count=link_count
            )
            oracle_result = self.predictor.predict(synthetic_item)
            evidence_strength = self._evidence_strength(comparables_count)
            if comparables_count >= 3 and market_median > 0:
                expected_sale_value = (market_median * 0.6) + (
                    oracle_result.predicted_value * 0.4
                )
            elif comparables_count >= 1 and market_median > 0:
                expected_sale_value = (
                    (market_floor * 0.25)
                    + (market_median * 0.4)
                    + (min(oracle_result.predicted_value, market_median * 1.1) * 0.35)
                )
            elif market_floor > 0:
                expected_sale_value = (market_floor * 0.55) + (
                    min(
                        oracle_result.predicted_value,
                        max(market_floor * 1.15, market_floor + 10.0),
                    )
                    * 0.45
                )
            else:
                if oracle_result.model_source == "family_fallback":
                    continue
                expected_sale_value = oracle_result.predicted_value
            if market_median > 0:
                expected_sale_value = min(
                    expected_sale_value,
                    max(
                        market_median * 1.1,
                        market_floor + max(market_spread * 0.35, 10.0),
                    ),
                )
                expected_sale_value = max(expected_sale_value, market_floor)
            pricing_position = self._pricing_position(
                expected_sale_value, market_floor, market_median, market_spread
            )
            exit_estimate = ExitMarketEstimate(
                expected_sale_value=round(expected_sale_value, 1),
                market_floor=round(market_floor, 1),
                market_median=round(market_median, 1),
                comparables_count=comparables_count,
                pricing_position=pricing_position,
                evidence_strength=evidence_strength,
                oracle_confidence=round(oracle_result.confidence, 2),
                oracle_model_source=oracle_result.model_source,
                required_structure={
                    "link_count": link_count,
                    "defence_profile": self._defence_profile(opportunity),
                    "base_type": opportunity.base_type,
                },
            )
            linking_cost = (
                self._six_link_cost_chaos()
                if link_count > self._link_count(opportunity)
                else 0.0
            )
            support_weight = {"strong": 0.85, "moderate": 0.68, "weak": 0.45}[
                evidence_strength
            ]
            supported_profit = (
                (
                    exit_estimate.expected_sale_value
                    - opportunity.listed_price
                    - expected_craft_cost
                    - linking_cost
                )
                * support_weight
                * max(exit_estimate.oracle_confidence, 0.25)
            )
            if best_candidate is None or supported_profit > best_candidate[0]:
                best_candidate = (supported_profit, exit_estimate)
        return best_candidate[1] if best_candidate is not None else None

    def _build_confidence_breakdown(
        self,
        opportunity: ScanOpportunity,
        steps: List[CraftActionEvaluation],
        exit_estimate: ExitMarketEstimate,
        target: FlipTargetRecommendation,
        budget: float,
    ) -> PlanConfidenceBreakdown:
        base_confidence = self._clamp(
            (opportunity.ml_confidence * 0.55)
            + (target.confidence * 0.2)
            + (0.15 if opportunity.pricing_position == "below_floor" else 0.0)
            + (0.08 if opportunity.comparables_count >= 3 else -0.05)
            - (
                0.1
                if opportunity.low_ilvl_context and not opportunity.twink_override
                else 0.0
            ),
            0.2,
            0.95,
        )
        if target.requires_suppression and not self._suppression_eligible(opportunity):
            craft_confidence = 0.0
        elif any(not step.eligibility for step in steps):
            craft_confidence = 0.0
        else:
            craft_confidence = self._clamp(
                (sum(step.probability for step in steps) / max(len(steps), 1))
                - (sum(step.brick_risk for step in steps) * 0.08)
                - (
                    0.12
                    if any(
                        step.action_type == "socket_linking"
                        and step.expected_cost > budget * 0.75
                        for step in steps
                    )
                    else 0.0
                ),
                0.0,
                0.9,
            )
        evidence_base = {"strong": 0.82, "moderate": 0.66, "weak": 0.44}[
            exit_estimate.evidence_strength
        ]
        exit_confidence = evidence_base + (exit_estimate.oracle_confidence * 0.18)
        if exit_estimate.comparables_count < 2:
            exit_confidence -= 0.12
        if exit_estimate.oracle_model_source == "family_fallback":
            exit_confidence -= 0.08
        if exit_estimate.pricing_position == "outlier":
            exit_confidence -= 0.12
        if exit_estimate.required_structure.get(
            "link_count", self._link_count(opportunity)
        ) > self._link_count(opportunity):
            exit_confidence -= 0.08
        exit_confidence = self._clamp(exit_confidence, 0.0, 0.9)
        return PlanConfidenceBreakdown.compose(
            base_confidence=base_confidence,
            craft_confidence=craft_confidence,
            exit_confidence=exit_confidence,
        )

    def _passes_stage_b(
        self,
        opportunity: ScanOpportunity,
        target: FlipTargetRecommendation,
        steps: List[CraftActionEvaluation],
        exit_estimate: ExitMarketEstimate,
        confidence_breakdown: PlanConfidenceBreakdown,
        expected_profit: float,
        budget: float,
    ) -> Tuple[bool, List[str]]:
        reasons: List[str] = []
        if expected_profit <= 0:
            reasons.append("non_profitable_plan")
        if confidence_breakdown.craft_confidence <= 0:
            reasons.append("craft_ineligible")
        if confidence_breakdown.overall_confidence < 0.45:
            reasons.append("confidence_too_low")
        required_link_count = int(
            exit_estimate.required_structure.get(
                "link_count", self._link_count(opportunity)
            )
        )
        has_structural_market_anchor = (
            required_link_count > self._link_count(opportunity)
            and exit_estimate.market_floor > 0
        )
        if (
            exit_estimate.oracle_model_source == "family_fallback"
            and exit_estimate.comparables_count < 2
            and not has_structural_market_anchor
        ):
            reasons.append("fallback_exit_low_evidence")
        if (
            exit_estimate.pricing_position == "outlier"
            and exit_estimate.comparables_count < 3
        ):
            reasons.append("exit_outlier_low_evidence")
        if required_link_count > self._link_count(opportunity):
            linking_steps = [
                step
                for step in steps
                if step.action_type == "socket_linking" and step.eligibility
            ]
            if not linking_steps:
                reasons.append("missing_socket_linking_step")
            elif sum(step.expected_cost for step in linking_steps) > budget:
                reasons.append("linking_over_budget")
        if target.requires_suppression and not self._suppression_eligible(opportunity):
            reasons.append("suppression_base_ineligible")
        return (not reasons, reasons)

    def _mark_stop_point(
        self,
        opportunity: ScanOpportunity,
        steps: List[CraftActionEvaluation],
        expected_profit: float,
    ) -> None:
        cumulative_cost = 0.0
        for step in steps:
            cumulative_cost += step.expected_cost
            interim_profit = (
                step.expected_value_after_step
                - opportunity.listed_price
                - cumulative_cost
            )
            if interim_profit > 0 and interim_profit >= (expected_profit * 0.6):
                step.stop_here = True
                return

    def _build_stop_condition(
        self,
        opportunity: ScanOpportunity,
        steps: List[CraftActionEvaluation],
        expected_profit: float,
    ) -> str:
        cumulative_cost = 0.0
        for step in steps:
            cumulative_cost += step.expected_cost
            interim_profit = (
                step.expected_value_after_step
                - opportunity.listed_price
                - cumulative_cost
            )
            if step.stop_here or interim_profit >= (expected_profit * 0.65):
                return f"Pare e venda se após '{step.action_name}' o valor implícito atingir {step.expected_value_after_step:.1f}c com lucro >= {max(interim_profit, 0):.1f}c."
        return "Siga até o alvo recomendado; não há stop-and-sell antecipado claramente superior."

    def _build_plan_explanation(
        self,
        opportunity: ScanOpportunity,
        target: FlipTargetRecommendation,
        steps: List[CraftActionEvaluation],
        exit_estimate: ExitMarketEstimate,
        confidence_breakdown: PlanConfidenceBreakdown,
    ) -> str:
        bits = [
            f"Base escolhida: {opportunity.base_type} por {opportunity.listed_price:.1f}c, posição {opportunity.pricing_position} e {opportunity.comparables_count} comparáveis de entrada.",
            f"Alvo: {target.label} com mods {', '.join(target.goal_mods)}.",
        ]
        if opportunity.item_family == "body_armour_defense":
            bits.append(
                f"Estrutura: {self._defence_profile(opportunity)}/{self._attribute_profile(opportunity)}, {self._link_count(opportunity)}L atual."
            )
            if target.requires_suppression:
                bits.append(
                    "Suppression exigido e validado para base com componente de Evasion/Dex."
                )
            else:
                bits.append(
                    "Planner não força Spell Suppression porque a base não sustenta esse alvo ou o mercado não paga por isso."
                )
        required_links = exit_estimate.required_structure.get(
            "link_count", self._link_count(opportunity)
        )
        if required_links > self._link_count(opportunity):
            bits.append(
                f"Saída exige {required_links}L; custo de linking foi embutido no plano."
            )
        bits.append(
            f"Saída validada por mercado: piso {exit_estimate.market_floor:.1f}c, mediana {exit_estimate.market_median:.1f}c, comparáveis {exit_estimate.comparables_count}, evidência {exit_estimate.evidence_strength}."
        )
        bits.append(
            f"Validação de saída via oráculo: {exit_estimate.oracle_model_source} com confiança {exit_estimate.oracle_confidence:.2f}."
        )
        bits.append(
            f"Confiança composta: base {confidence_breakdown.base_confidence:.2f}, craft {confidence_breakdown.craft_confidence:.2f}, exit {confidence_breakdown.exit_confidence:.2f}."
        )
        return " ".join(bits)

    def _rebase_step_values(
        self,
        opportunity: ScanOpportunity,
        steps: List[CraftActionEvaluation],
        final_sale_value: float,
    ) -> None:
        anchor = self._current_market_anchor(opportunity)
        total_delta = sum(max(step.expected_value_delta, 0.0) for step in steps)
        if total_delta <= 0:
            for step in steps:
                step.expected_value_after_step = round(anchor, 1)
            return
        running_delta = 0.0
        available_delta = max(final_sale_value - anchor, 0.0)
        for step in steps:
            share = max(step.expected_value_delta, 0.0) / total_delta
            running_delta += available_delta * share
            step.expected_value_after_step = round(anchor + running_delta, 1)

    def _simulate_target_item(
        self,
        opportunity: ScanOpportunity,
        goal_mods: List[str],
        link_count: int | None = None,
    ) -> NormalizedMarketItem:
        current_tokens = list(dict.fromkeys(list(opportunity.mod_tokens)))
        missing_tokens = [mod for mod in goal_mods if mod not in current_tokens]
        simulated_tokens = list(dict.fromkeys(current_tokens + missing_tokens))
        explicit_mods = list(opportunity.explicit_mods)
        for token in missing_tokens:
            explicit_mods.append(self._MOD_TEXT_BY_TOKEN.get(token, token))
        prefix_count = min(
            3,
            opportunity.prefix_count
            + sum(1 for token in missing_tokens if token in self._PREFIX_MODS),
        )
        suffix_count = min(
            3,
            opportunity.suffix_count
            + sum(1 for token in missing_tokens if token in self._SUFFIX_MODS),
        )
        open_prefixes = max(0, 3 - prefix_count)
        open_suffixes = max(0, 3 - suffix_count)
        return NormalizedMarketItem(
            item_id=opportunity.item_id,
            base_type=opportunity.base_type,
            item_family=opportunity.item_family,
            ilvl=opportunity.ilvl,
            listed_price=opportunity.listed_price,
            listing_currency=opportunity.listing_currency,
            listing_amount=opportunity.listing_amount,
            seller=opportunity.seller,
            listed_at=opportunity.indexed_at,
            whisper=opportunity.whisper,
            corrupted=opportunity.corrupted,
            fractured=opportunity.fractured,
            influences=list(opportunity.influences),
            explicit_mods=explicit_mods,
            implicit_mods=list(opportunity.implicit_mods),
            prefix_count=prefix_count,
            suffix_count=suffix_count,
            open_prefixes=open_prefixes,
            open_suffixes=open_suffixes,
            mod_tokens=simulated_tokens,
            tag_tokens=list(opportunity.tag_tokens),
            numeric_mod_features={},
            tier_source="none",
            native_tier_count=0,
            twink_override=bool(getattr(opportunity, "twink_override", False)),
            tier_ilvl_mismatch=bool(getattr(opportunity, "tier_ilvl_mismatch", False)),
            low_ilvl_context=bool(getattr(opportunity, "low_ilvl_context", False)),
            fractured_low_ilvl_brick=bool(
                getattr(opportunity, "fractured_low_ilvl_brick", False)
            ),
        )

    def _current_market_anchor(self, opportunity: ScanOpportunity) -> float:
        return max(
            opportunity.market_floor, opportunity.market_median, opportunity.ml_value
        )

    def _suppression_eligible(self, opportunity: ScanOpportunity) -> bool:
        defence_profile = self._defence_profile(opportunity).lower()
        attribute_profile = self._attribute_profile(opportunity).lower()
        return "evasion" in defence_profile or "dex" in attribute_profile

    def _market_key(
        self, item_family: str, base_type: str, link_count: int, ilvl: int
    ) -> tuple[str, str, int, str]:
        ilvl_band = "any"
        if item_family == "body_armour_defense":
            if ilvl >= 84:
                ilvl_band = "84plus"
            elif ilvl >= 78:
                ilvl_band = "78to83"
            else:
                ilvl_band = "low"
        return (item_family, base_type, int(link_count), ilvl_band)

    def _build_market_index(
        self, opportunities: List[ScanOpportunity]
    ) -> Dict[tuple[str, str, int, str], List[float]]:
        index: Dict[tuple[str, str, int, str], List[float]] = {}
        for opportunity in opportunities:
            key = self._market_key(
                opportunity.item_family,
                opportunity.base_type,
                self._link_count(opportunity),
                opportunity.ilvl,
            )
            index.setdefault(key, []).append(opportunity.listed_price)
        return index

    def _market_stats_from_prices(
        self, prices: List[float]
    ) -> tuple[float, float, float, int]:
        filtered = sorted(float(price) for price in prices if price > 0)
        if not filtered:
            return (0.0, 0.0, 0.0, 0)
        floor = filtered[0]
        middle = len(filtered) // 2
        median = (
            (filtered[middle - 1] + filtered[middle]) / 2
            if len(filtered) % 2 == 0
            else filtered[middle]
        )
        spread = max(filtered) - min(filtered)
        return (round(floor, 1), round(median, 1), round(spread, 1), len(filtered))

    def _pricing_position(
        self,
        expected_sale_value: float,
        market_floor: float,
        market_median: float,
        market_spread: float,
    ) -> str:
        if market_floor <= 0 and market_median <= 0:
            return "near_market"
        if expected_sale_value <= max(market_floor * 1.05, market_floor + 5.0):
            return "below_floor"
        if market_median > 0 and expected_sale_value > max(
            market_median * 1.18, market_floor + max(market_spread, 15.0)
        ):
            return "outlier"
        return "near_market"

    def _evidence_strength(self, comparables_count: int) -> str:
        if comparables_count >= 3:
            return "strong"
        if comparables_count >= 2:
            return "moderate"
        return "weak"

    def _six_link_cost_chaos(self) -> float:
        fusing_rate = float(
            self.scanner.currency_rates.get("Orb of Fusing", 1.0) or 1.0
        )
        return self._DEFAULT_SIX_LINK_FUSINGS * max(fusing_rate, 0.01)

    def _clamp(self, value: float, minimum: float, maximum: float) -> float:
        return max(minimum, min(maximum, value))

    def _defence_profile(self, opportunity: ScanOpportunity) -> str:
        value = getattr(opportunity, "defence_profile", "unknown") or "unknown"
        if value != "unknown":
            return str(value)
        base = opportunity.base_type.lower()
        if "plate" in base:
            return "armour"
        if any(token in base for token in ("garb", "carnal", "silks")):
            return "evasion_energy_shield"
        if any(token in base for token in ("regalia", "robe", "vestment")):
            return "energy_shield"
        if any(token in base for token in ("leather", "jerkin", "tunic", "doublet")):
            return "evasion"
        return "unknown"

    def _attribute_profile(self, opportunity: ScanOpportunity) -> str:
        value = getattr(opportunity, "attribute_profile", "unknown") or "unknown"
        if value != "unknown":
            return str(value)
        defence = self._defence_profile(opportunity)
        mapping = {
            "armour": "str",
            "evasion": "dex",
            "energy_shield": "int",
            "armour_evasion": "str_dex",
            "armour_energy_shield": "str_int",
            "evasion_energy_shield": "dex_int",
        }
        return mapping.get(defence, "unknown")

    def _link_count(self, opportunity: ScanOpportunity) -> int:
        return int(getattr(opportunity, "link_count", 0) or 0)
