from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from typing import Any, Protocol


@dataclass(frozen=True)
class FamilyAnalysis:
    family: str
    analyst: str
    archetype: str
    score: float
    confidence: float
    decision: str
    reasons: list[str] = field(default_factory=list)
    risks: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class FamilyAnalyst(Protocol):
    name: str

    def analyze(
        self,
        *,
        segment: dict[str, Any],
        metrics: dict[str, Any],
        opportunity: dict[str, Any],
        model: dict[str, Any] | None,
    ) -> FamilyAnalysis:
        ...


class GenericFamilyAnalyst:
    name = "GenericFamilyAnalyst"

    def analyze(
        self,
        *,
        segment: dict[str, Any],
        metrics: dict[str, Any],
        opportunity: dict[str, Any],
        model: dict[str, Any] | None,
    ) -> FamilyAnalysis:
        family = str(segment.get("item_family") or "generic")
        listed = float(opportunity.get("listed_price") or 0.0)
        reference = float(opportunity.get("reference_price") or 0.0)
        sample_count = int(metrics.get("sample_count") or 0)
        expected_profit = reference - listed
        reasons: list[str] = []
        risks = ["domain_rules_missing"]

        if expected_profit > 0:
            reasons.append("positive_market_edge")
        if sample_count < 10:
            risks.append("low_comparable_count")
        if not model:
            risks.append("model_missing")

        score = min(100.0, max(0.0, expected_profit))
        confidence = 0.25 + min(sample_count, 20) / 100
        if model:
            confidence += 0.15

        return FamilyAnalysis(
            family=family,
            analyst=self.name,
            archetype="unknown",
            score=round(score, 2),
            confidence=round(min(confidence, 0.7), 2),
            decision="market_context_only",
            reasons=reasons,
            risks=risks,
        )


class JewelClusterAnalyst:
    name = "JewelClusterAnalyst"
    _PREMIUM_ENCHANTS = {
        "minion damage",
        "aura effect",
        "spell damage",
        "armour",
        "physical damage",
        "attack damage",
    }
    _BAIT_ENCHANTS = {
        "dual wielding",
        "totem damage",
        "brand damage",
        "shield damage",
    }

    def analyze(
        self,
        *,
        segment: dict[str, Any],
        metrics: dict[str, Any],
        opportunity: dict[str, Any],
        model: dict[str, Any] | None,
    ) -> FamilyAnalysis:
        evidence = _cluster_evidence(segment, opportunity)
        if not evidence["cluster_size"] or evidence["passives"] is None:
            return FamilyAnalysis(
                family="jewel_cluster",
                analyst=self.name,
                archetype="cluster_jewel_unknown",
                score=0.0,
                confidence=0.2,
                decision="needs_more_evidence",
                reasons=[],
                risks=["missing_cluster_evidence"],
            )

        reasons: list[str] = []
        risks: list[str] = []
        cluster_size = evidence["cluster_size"]
        passives = evidence["passives"]
        ilvl = evidence["ilvl"] or 0
        enchant = evidence["enchant"]
        notables = evidence["notables"]

        if "primordial bond" in notables:
            risks.append("bad_meta_notable_primordial_bond")
        if cluster_size == "large" and passives >= 10 and ilvl < 84:
            risks.append("large_cluster_too_many_passives_below_ilvl_84")
        if cluster_size == "medium" and passives == 6:
            risks.append("medium_cluster_six_passives")
        if enchant in self._BAIT_ENCHANTS:
            risks.append("bait_cluster_enchant")

        if risks:
            return FamilyAnalysis(
                family="jewel_cluster",
                analyst=self.name,
                archetype=f"{cluster_size}_cluster",
                score=0.0,
                confidence=0.75,
                decision="exclude",
                reasons=[],
                risks=risks,
            )

        score = 0.0
        if cluster_size == "large" and passives == 8:
            score += 35.0
            reasons.append("premium_large_8_passives")
        if cluster_size == "medium" and passives in {4, 5}:
            score += 30.0
            reasons.append("efficient_medium_passives")
        if cluster_size == "small" and passives == 2:
            score += 25.0
            reasons.append("efficient_small_passives")
        if ilvl >= 84:
            score += 25.0
            reasons.append("ilvl_84_plus")
        if enchant in self._PREMIUM_ENCHANTS:
            score += 20.0
            reasons.append("premium_cluster_enchant")

        decision = "watch_only"
        if score >= 70.0:
            decision = "valid_for_manual_review"

        return FamilyAnalysis(
            family="jewel_cluster",
            analyst=self.name,
            archetype=f"{cluster_size}_cluster",
            score=round(score, 2),
            confidence=0.65,
            decision=decision,
            reasons=reasons,
            risks=risks,
        )


class AccessoryAnalyst:
    name = "AccessoryAnalyst"
    _PREMIUM_BASES = {"cord belt", "simplex amulet", "bone ring", "stygian vise"}

    def analyze(
        self,
        *,
        segment: dict[str, Any],
        metrics: dict[str, Any],
        opportunity: dict[str, Any],
        model: dict[str, Any] | None,
    ) -> FamilyAnalysis:
        payload = {**opportunity, **segment}
        base_type = _normalize_text(payload.get("base_type"))
        tokens = _token_set(payload)
        ilvl = _coerce_int(payload.get("ilvl") or payload.get("item_level")) or 0
        reasons: list[str] = []
        risks: list[str] = []

        has_core_stats = bool(tokens & {"life", "energyshield", "plusallgems"})
        if "amulet" in base_type and not has_core_stats:
            risks.append("amulet_missing_life_es_or_gem_levels")
        if risks:
            return _excluded_analysis("accessory_generic", self.name, "accessory", risks)

        score = 0.0
        if base_type in self._PREMIUM_BASES:
            score += 35.0
            reasons.append("premium_accessory_base")
        if ilvl >= 85:
            score += 25.0
            reasons.append("ilvl_85_plus")
        if tokens & {"plusallgems", "negative_lightning_resistance", "infamy"}:
            score += 30.0
            reasons.append("transformational_accessory_mod")
        if _coerce_int(payload.get("open_suffixes")) or _coerce_int(payload.get("open_prefixes")):
            score += 10.0
            reasons.append("open_affix_for_crafting")

        return _scored_analysis("accessory_generic", self.name, "accessory", score, reasons)


class WandCasterAnalyst:
    name = "WandCasterAnalyst"

    def analyze(
        self,
        *,
        segment: dict[str, Any],
        metrics: dict[str, Any],
        opportunity: dict[str, Any],
        model: dict[str, Any] | None,
    ) -> FamilyAnalysis:
        payload = {**opportunity, **segment}
        base_type = _normalize_text(payload.get("base_type"))
        tokens = _token_set(payload)
        reasons: list[str] = []
        risks: list[str] = []

        has_caster_core = bool(tokens & {"plusallspellgems", "plusallgems", "castspeed", "miniondamage", "infamy"})
        if "wand" in base_type and "addedattackdamage" in tokens and not has_caster_core:
            risks.append("attack_damage_wand_without_caster_core")
        if risks:
            return _excluded_analysis("wand_caster", self.name, "caster_weapon", risks)

        score = 0.0
        if tokens & {"plusallspellgems", "plusallgems"}:
            score += 40.0
            reasons.append("plus_spell_gem_level")
        if "castspeed" in tokens:
            score += 20.0
            reasons.append("cast_speed")
        if _coerce_int(payload.get("open_suffixes")):
            score += 20.0
            reasons.append("open_suffix_for_trigger")
        if tokens & {"infamy", "miniondamage"}:
            score += 25.0
            reasons.append("premium_wand_modifier")

        return _scored_analysis("wand_caster", self.name, "caster_weapon", score, reasons)


class BodyArmourDefenseAnalyst:
    name = "BodyArmourDefenseAnalyst"

    def analyze(
        self,
        *,
        segment: dict[str, Any],
        metrics: dict[str, Any],
        opportunity: dict[str, Any],
        model: dict[str, Any] | None,
    ) -> FamilyAnalysis:
        payload = {**opportunity, **segment}
        tokens = _token_set(payload)
        ilvl = _coerce_int(payload.get("ilvl") or payload.get("item_level")) or 0
        reasons: list[str] = []
        risks: list[str] = []

        has_defensive_core = bool(tokens & {"spellsuppress", "life", "energyshield", "armour", "evasion", "infamy"})
        if ilvl < 84 and not has_defensive_core:
            risks.append("low_ilvl_without_defensive_core")
        if {"stunrecovery", "reflectmelee", "liferegen"}.issubset(tokens):
            risks.append("junk_defensive_mod_bundle")
        if risks:
            return _excluded_analysis("body_armour_defense", self.name, "defensive_body_armour", risks)

        score = 0.0
        if ilvl >= 86:
            score += 35.0
            reasons.append("scarce_ilvl_86_base")
        if "spellsuppress" in tokens:
            score += 30.0
            reasons.append("spell_suppression")
        if tokens & {"life", "energyshield", "armour", "evasion"}:
            score += 20.0
            reasons.append("defensive_core_stats")
        if _coerce_int(payload.get("open_prefixes")):
            score += 10.0
            reasons.append("open_prefix_for_crafting")

        return _scored_analysis("body_armour_defense", self.name, "defensive_body_armour", score, reasons)


def _excluded_analysis(
    family: str, analyst: str, archetype: str, risks: list[str]
) -> FamilyAnalysis:
    return FamilyAnalysis(
        family=family,
        analyst=analyst,
        archetype=archetype,
        score=0.0,
        confidence=0.75,
        decision="exclude",
        reasons=[],
        risks=risks,
    )


def _scored_analysis(
    family: str, analyst: str, archetype: str, score: float, reasons: list[str]
) -> FamilyAnalysis:
    decision = "valid_for_manual_review" if score >= 70.0 else "watch_only"
    risks = [] if reasons else ["missing_family_evidence"]
    return FamilyAnalysis(
        family=family,
        analyst=analyst,
        archetype=archetype,
        score=round(score, 2),
        confidence=0.65 if reasons else 0.2,
        decision=decision if reasons else "needs_more_evidence",
        reasons=reasons,
        risks=risks,
    )


def _token_set(payload: dict[str, Any]) -> set[str]:
    raw_tokens = payload.get("mod_tokens") or payload.get("tag_tokens") or []
    return {_normalize_text(token).replace(" ", "") for token in raw_tokens}


def _cluster_evidence(
    segment: dict[str, Any], opportunity: dict[str, Any]
) -> dict[str, Any]:
    payload = {**opportunity, **segment}
    base_text = _normalize_text(payload.get("base_type"))
    enchant = _normalize_text(
        payload.get("cluster_enchant")
        or payload.get("enchant")
        or payload.get("base_enchant")
    )
    cluster_size = _coerce_cluster_size(payload.get("cluster_size"), base_text)
    passives = _coerce_int(
        payload.get("cluster_passives")
        or payload.get("passives")
        or payload.get("added_passives")
    )
    ilvl = _coerce_int(payload.get("ilvl") or payload.get("item_level"))
    notables = {
        _normalize_text(notable)
        for notable in payload.get("notables", [])
        if _normalize_text(notable)
    }
    return {
        "cluster_size": cluster_size,
        "passives": passives,
        "ilvl": ilvl,
        "enchant": enchant,
        "notables": notables,
    }


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value).strip().lower())


def _coerce_int(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        match = re.search(r"\d+", str(value))
        return int(match.group(0)) if match else None


def _coerce_cluster_size(value: Any, base_text: str) -> str:
    text = _normalize_text(value) or base_text
    for cluster_size in ("large", "medium", "small"):
        if cluster_size in text:
            return cluster_size
    return ""


_GENERIC_ANALYST = GenericFamilyAnalyst()
_ANALYSTS: dict[str, FamilyAnalyst] = {
    "accessory_generic": AccessoryAnalyst(),
    "body_armour_defense": BodyArmourDefenseAnalyst(),
    "jewel_cluster": JewelClusterAnalyst(),
    "wand_caster": WandCasterAnalyst(),
}


def get_family_analyst(family: str) -> FamilyAnalyst:
    return _ANALYSTS.get(family, _GENERIC_ANALYST)


def analyze_family_candidate(
    *,
    family: str,
    segment: dict[str, Any],
    metrics: dict[str, Any],
    opportunity: dict[str, Any],
    model: dict[str, Any] | None,
) -> FamilyAnalysis:
    segment_with_family = {**segment, "item_family": family}
    return get_family_analyst(family).analyze(
        segment=segment_with_family,
        metrics=metrics,
        opportunity=opportunity,
        model=model,
    )
