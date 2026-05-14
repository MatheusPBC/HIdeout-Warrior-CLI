from __future__ import annotations

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

    def analyze(
        self,
        *,
        segment: dict[str, Any],
        metrics: dict[str, Any],
        opportunity: dict[str, Any],
        model: dict[str, Any] | None,
    ) -> FamilyAnalysis:
        return FamilyAnalysis(
            family="jewel_cluster",
            analyst=self.name,
            archetype="pending_cluster_jewel_rubric",
            score=0.0,
            confidence=0.2,
            decision="needs_domain_rules",
            reasons=[],
            risks=["cluster_jewel_rules_pending"],
        )


_GENERIC_ANALYST = GenericFamilyAnalyst()
_ANALYSTS: dict[str, FamilyAnalyst] = {
    "jewel_cluster": JewelClusterAnalyst(),
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
