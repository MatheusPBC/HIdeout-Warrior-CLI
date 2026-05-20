import ast
from dataclasses import asdict, dataclass, field
from typing import Any

import pandas as pd


BALANCED_WEIGHTS = {
    "safety_score": 0.35,
    "liquidity_score": 0.30,
    "margin_score": 0.20,
    "trend_score": 0.15,
}
EXCLUDED_MARKET_FAMILIES = {"map", "gem", "flask"}

GOLD_MOD_FEATURES = (
    ("has_life", "Life"),
    ("has_resist", "Resist"),
    ("has_attributes", "Attributes"),
    ("has_mana", "Mana"),
    ("has_crit", "Crit"),
    ("has_spell_damage", "SpellDamage"),
    ("has_cast_speed", "CastSpeed"),
    ("has_spell_crit", "SpellCrit"),
    ("has_suppress", "SpellSuppress"),
    ("plus_all_spell_gems", "PlusAllSpellGems"),
)


@dataclass(frozen=True)
class MarketSegment:
    key: str
    league: str
    item_family: str
    base_type: str
    ilvl_band: str
    price_band: str
    mod_signature: str
    tag_signature: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class MarketSegmentMetrics:
    sample_count: int
    fresh_ratio: float
    stale_ratio: float
    price_floor: float
    price_median: float
    price_spread: float
    volume_score: float
    liquidity_score: float
    safety_score: float
    margin_score: float
    trend_score: float

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class MarketSegmentScore:
    market_score: float
    status: str
    explanation: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class MarketOpportunityCandidate:
    item_id: str
    base_type: str
    listed_price: float
    reference_price: float
    estimated_upside: float
    freshness_band: str
    ilvl: int = 0
    open_prefixes: int = 0
    open_suffixes: int = 0
    mod_tokens: list[str] = field(default_factory=list)
    tag_tokens: list[str] = field(default_factory=list)
    cluster_size: str = ""
    cluster_passives: int | None = None
    cluster_enchant: str = ""
    notables: list[str] = field(default_factory=list)
    mode: str = "evaluation"

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class MarketSegmentAnalysis:
    segment: MarketSegment
    metrics: MarketSegmentMetrics
    score: MarketSegmentScore
    opportunities: list[MarketOpportunityCandidate]

    def to_dict(self) -> dict:
        return {
            "segment": self.segment.to_dict(),
            "metrics": self.metrics.to_dict(),
            "score": self.score.to_dict(),
            "opportunities": [item.to_dict() for item in self.opportunities],
        }


def build_segment_key(row: dict[str, Any]) -> str:
    segment = _build_segment(row)
    return segment.key


def build_market_segments(frame: pd.DataFrame) -> list[MarketSegmentAnalysis]:
    if frame.empty:
        return []

    working = frame.copy()
    if "item_family" in working.columns:
        working = working[
            ~working["item_family"].astype(str).isin(EXCLUDED_MARKET_FAMILIES)
        ]
    if working.empty:
        return []

    working["segment_key"] = working.apply(lambda row: build_segment_key(row.to_dict()), axis=1)

    analyses = []
    for _, group in working.groupby("segment_key", sort=True):
        first_row = group.iloc[0].to_dict()
        segment = _build_segment(first_row)
        metrics = _calculate_metrics(group)
        opportunities = _build_opportunities(group)
        analyses.append(
            MarketSegmentAnalysis(
                segment,
                metrics,
                score_market_segment(metrics),
                opportunities,
            )
        )
    return sorted(analyses, key=lambda item: item.score.market_score, reverse=True)


def score_market_segment(
    metrics: MarketSegmentMetrics, risk_profile: str = "balanced"
) -> MarketSegmentScore:
    if risk_profile != "balanced":
        raise ValueError("Only balanced risk profile is supported for now")

    score = _weighted_score(metrics)
    status = _classify_status(metrics, score)
    explanation = _explain_score(metrics, status)
    return MarketSegmentScore(
        market_score=round(score, 4),
        status=status,
        explanation=explanation,
    )


def _build_segment(row: dict[str, Any]) -> MarketSegment:
    league = _clean_value(row.get("league"), "unknown")
    item_family = _clean_value(row.get("item_family"), "generic")
    base_type = _clean_value(row.get("base_type"), "unknown")
    ilvl_band = _clean_value(row.get("ilvl_band"), "unknown")
    price_band = _price_band(row.get("price_chaos"))
    mod_signature = _mod_signature(row)
    tag_signature = _token_signature(row.get("tag_tokens"))
    key = "|".join(
        [league, item_family, base_type, ilvl_band, price_band, mod_signature, tag_signature]
    )
    return MarketSegment(
        key=key,
        league=league,
        item_family=item_family,
        base_type=base_type,
        ilvl_band=ilvl_band,
        price_band=price_band,
        mod_signature=mod_signature,
        tag_signature=tag_signature,
    )


def _calculate_metrics(group: pd.DataFrame) -> MarketSegmentMetrics:
    prices = pd.to_numeric(group.get("price_chaos"), errors="coerce").dropna()
    freshness = group.get("freshness_band", pd.Series(dtype=str)).astype(str)
    sample_count = int(len(group))
    fresh_ratio = _ratio(freshness.isin(["fresh", "active"]).sum(), sample_count)
    stale_ratio = _ratio((freshness == "stale").sum(), sample_count)
    price_floor = float(prices.min()) if not prices.empty else 0.0
    price_median = float(prices.median()) if not prices.empty else 0.0
    price_spread = _price_spread(price_floor, price_median)
    volume_score = min(sample_count / 50.0, 1.0)
    liquidity_score = _clamp((fresh_ratio * 0.65) + (volume_score * 0.35))
    safety_score = _clamp(1.0 - stale_ratio - (0.25 if sample_count < 10 else 0.0))
    margin_score = _clamp(price_spread)
    trend_score = _clamp(fresh_ratio * volume_score)
    return MarketSegmentMetrics(
        sample_count=sample_count,
        fresh_ratio=round(fresh_ratio, 4),
        stale_ratio=round(stale_ratio, 4),
        price_floor=round(price_floor, 2),
        price_median=round(price_median, 2),
        price_spread=round(price_spread, 4),
        volume_score=round(volume_score, 4),
        liquidity_score=round(liquidity_score, 4),
        safety_score=round(safety_score, 4),
        margin_score=round(margin_score, 4),
        trend_score=round(trend_score, 4),
    )


def _build_opportunities(group: pd.DataFrame) -> list[MarketOpportunityCandidate]:
    prices = pd.to_numeric(group.get("price_chaos"), errors="coerce")
    freshness = group.get("freshness_band", pd.Series(dtype=str)).astype(str)
    fresh_prices = prices[freshness.isin(["fresh", "active"])].dropna()
    if fresh_prices.empty:
        return []

    reference_price = float(fresh_prices.median())
    if reference_price <= 0:
        return []

    candidates = []
    for _, row in group.assign(_price_chaos=prices).iterrows():
        freshness_band = str(row.get("freshness_band", "unknown"))
        listed_price = row.get("_price_chaos")
        if freshness_band not in {"fresh", "active"} or pd.isna(listed_price):
            continue
        listed_price = float(listed_price)
        upside = _estimated_upside(listed_price, reference_price)
        if upside <= 0:
            continue
        candidates.append(
            MarketOpportunityCandidate(
                item_id=_item_id(row),
                base_type=_clean_value(row.get("base_type"), "unknown"),
                listed_price=round(listed_price, 2),
                reference_price=round(reference_price, 2),
                estimated_upside=round(upside, 4),
                freshness_band=freshness_band,
                ilvl=_coerce_int(row.get("ilvl")),
                open_prefixes=_coerce_int(row.get("open_prefixes")),
                open_suffixes=_coerce_int(row.get("open_suffixes")),
                mod_tokens=_coerce_tokens(row.get("mod_tokens")),
                tag_tokens=_coerce_tokens(row.get("tag_tokens")),
                cluster_size=_clean_value(row.get("cluster_size"), ""),
                cluster_passives=_coerce_optional_int(row.get("cluster_passives")),
                cluster_enchant=_clean_value(row.get("cluster_enchant"), ""),
                notables=_coerce_tokens(row.get("notables")),
            )
        )
    return sorted(candidates, key=lambda item: item.estimated_upside, reverse=True)[:5]


def _clean_value(value: Any, default: str) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return default
    text = str(value).strip()
    return text or default


def _coerce_int(value: Any) -> int:
    parsed = _coerce_optional_int(value)
    return parsed if parsed is not None else 0


def _coerce_optional_int(value: Any) -> int | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _price_band(value: Any) -> str:
    try:
        price = float(value)
    except (TypeError, ValueError):
        return "unknown"
    if price <= 15:
        return "1-15"
    if price <= 50:
        return "16-50"
    if price <= 150:
        return "51-150"
    if price <= 500:
        return "151-500"
    return "501+"


def _token_signature(value: Any) -> str:
    tokens = _coerce_tokens(value)
    if not tokens:
        return "none"
    return "+".join(sorted(dict.fromkeys(tokens))[:4])


def _mod_signature(row: dict[str, Any]) -> str:
    signature = _token_signature(row.get("mod_tokens"))
    if signature != "none":
        return signature
    feature_tokens = [
        label for column, label in GOLD_MOD_FEATURES if _is_truthy(row.get(column))
    ]
    return _token_signature(feature_tokens)


def _item_id(row: dict[str, Any]) -> str:
    item_id = _clean_value(row.get("item_id"), "")
    if item_id:
        return item_id
    return _clean_value(row.get("event_key"), "unknown")


def _is_truthy(value: Any) -> bool:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return False
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes"}
    return bool(value)


def _coerce_tokens(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    if isinstance(value, tuple):
        return [str(item) for item in value if str(item).strip()]
    if isinstance(value, str):
        try:
            parsed = ast.literal_eval(value)
        except (SyntaxError, ValueError):
            parsed = [part.strip() for part in value.split(",")]
        return _coerce_tokens(parsed)
    return []


def _ratio(part: int, total: int) -> float:
    return float(part) / float(total) if total > 0 else 0.0


def _price_spread(price_floor: float, price_median: float) -> float:
    if price_median <= 0:
        return 0.0
    return _clamp((price_median - price_floor) / price_median)


def _estimated_upside(listed_price: float, reference_price: float) -> float:
    if listed_price <= 0:
        return 0.0
    return max(0.0, (reference_price / listed_price) - 1.0)


def _clamp(value: float) -> float:
    return max(0.0, min(float(value), 1.0))


def _weighted_score(metrics: MarketSegmentMetrics) -> float:
    return (
        metrics.safety_score * BALANCED_WEIGHTS["safety_score"]
        + metrics.liquidity_score * BALANCED_WEIGHTS["liquidity_score"]
        + metrics.margin_score * BALANCED_WEIGHTS["margin_score"]
        + metrics.trend_score * BALANCED_WEIGHTS["trend_score"]
    )


def _classify_status(metrics: MarketSegmentMetrics, score: float) -> str:
    if metrics.sample_count < 10:
        if _is_evaluation_candidate(metrics):
            return "evaluation_candidate"
        return "avoid"
    if metrics.safety_score < 0.45:
        return "avoid"
    if metrics.safety_score >= 0.70 and metrics.liquidity_score >= 0.65 and score >= 0.70:
        return "strong_candidate"
    if metrics.sample_count >= 20 and metrics.trend_score >= 0.75 and metrics.margin_score >= 0.60:
        return "emerging"
    return "watch"


def _is_evaluation_candidate(metrics: MarketSegmentMetrics) -> bool:
    return (
        metrics.safety_score >= 0.70
        and metrics.liquidity_score >= 0.65
        and metrics.margin_score >= 0.60
    )


def _explain_score(metrics: MarketSegmentMetrics, status: str) -> str:
    reasons = []
    if status == "evaluation_candidate":
        reasons.append("manual evaluation")
    if metrics.sample_count < 10:
        reasons.append("low evidence")
    if metrics.safety_score < 0.45:
        reasons.append("unsafe market")
    if metrics.safety_score >= 0.70:
        reasons.append("safe market")
    if metrics.liquidity_score >= 0.65:
        reasons.append("liquid market")
    if metrics.margin_score >= 0.60:
        reasons.append("healthy margin")
    if metrics.trend_score >= 0.75:
        reasons.append("strong trend")
    if not reasons:
        reasons.append("balanced watchlist candidate")
    return f"{status}: " + ", ".join(reasons)
