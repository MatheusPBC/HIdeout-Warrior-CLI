import logging
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote

from core.api_integrator import MarketAPIClient
from core.item_normalizer import (
    ComparableMarketStats,
    NormalizedMarketItem,
    build_comparable_market_stats,
    normalize_trade_item,
)
from core.ml_oracle import PricePredictor, ValuationResult
from core.ops_metrics import append_metric_event

logger = logging.getLogger(__name__)


@dataclass
class ListingSnapshot:
    item_id: str
    base_type: str
    ilvl: int
    listed_price: float
    listing_currency: str
    listing_amount: float
    seller: str
    indexed_at: Optional[str]
    whisper: str
    trade_link: str
    trade_search_link: str
    corrupted: bool
    fractured: bool
    influences: List[str] = field(default_factory=list)
    explicit_mods: List[str] = field(default_factory=list)
    implicit_mods: List[str] = field(default_factory=list)
    item_family: str = "generic"
    prefix_count: int = 0
    suffix_count: int = 0
    open_prefixes: int = 3
    open_suffixes: int = 3
    mod_tokens: List[str] = field(default_factory=list)
    tag_tokens: List[str] = field(default_factory=list)
    numeric_mod_features: Dict[str, float] = field(default_factory=dict)
    tier_source: str = "none"
    native_tier_count: int = 0
    twink_override: bool = False
    tier_ilvl_mismatch: bool = False
    low_ilvl_context: bool = False
    fractured_low_ilvl_brick: bool = False

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class ScanStats:
    total_found: int = 0
    total_evaluated: int = 0
    filtered_anti_fix: int = 0
    filtered_min_profit: int = 0
    filtered_min_listed_price: int = 0
    skipped_invalid_currency: int = 0
    filtered_safe_buy_confidence: int = 0
    filtered_safe_buy_age: int = 0
    filtered_safe_buy_price: int = 0
    filtered_open_confidence: int = 0
    filtered_open_cheap_low_confidence: int = 0
    filtered_open_cheap_low_profit: int = 0
    filtered_open_cheap_stale: int = 0
    filtered_stage_a_low_ilvl_no_twink: int = 0
    filtered_stage_a_fractured_low_ilvl_brick: int = 0
    avg_profit: float = 0.0
    max_profit: float = 0.0
    avg_score: float = 0.0
    scan_profile: str = "open_market"
    resolved_league: str = ""
    macro_queries: int = 0
    micro_queries: int = 0
    deduped_ttl: int = 0
    stage_a_candidates: int = 0
    stage_b_passed: int = 0
    budget_exhausted: int = 0


@dataclass
class ScanOpportunity:
    item_id: str
    base_type: str
    item_family: str
    ilvl: int
    listed_price: float
    ml_value: float
    ml_confidence: float
    profit: float
    score: float
    valuation_gap: float
    relative_discount: float
    whisper: str
    trade_link: str
    trade_search_link: str
    listing_currency: str
    listing_amount: float
    seller: str
    indexed_at: Optional[str]
    resolved_league: str
    corrupted: bool
    fractured: bool
    influences: List[str] = field(default_factory=list)
    explicit_mods: List[str] = field(default_factory=list)
    implicit_mods: List[str] = field(default_factory=list)
    prefix_count: int = 0
    suffix_count: int = 0
    open_prefixes: int = 3
    open_suffixes: int = 3
    mod_tokens: List[str] = field(default_factory=list)
    tag_tokens: List[str] = field(default_factory=list)
    trusted_profit: float = 0.0
    valuation_result: Dict = field(default_factory=dict)
    market_floor: float = 0.0
    market_median: float = 0.0
    comparables_count: int = 0
    market_spread: float = 0.0
    pricing_position: str = "near_market"
    risk_flags: List[str] = field(default_factory=list)
    valuation_explanation: str = ""
    numeric_mod_features: Dict[str, float] = field(default_factory=dict)
    tier_source: str = "none"
    native_tier_count: int = 0
    twink_override: bool = False
    low_ilvl_context: bool = False
    tier_ilvl_mismatch: bool = False
    fractured_low_ilvl_brick: bool = False

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "ScanOpportunity":
        return cls(
            item_id=data.get("item_id", ""),
            base_type=data.get("base_type", ""),
            item_family=data.get("item_family", "generic"),
            ilvl=data.get("ilvl", 0),
            listed_price=data.get("listed_price", 0.0),
            ml_value=data.get("ml_value", 0.0),
            ml_confidence=data.get("ml_confidence", 0.3),
            profit=data.get("profit", 0.0),
            score=data.get("score", 0.0),
            valuation_gap=data.get("valuation_gap", 0.0),
            relative_discount=data.get("relative_discount", 0.0),
            whisper=data.get("whisper", ""),
            trade_link=data.get("trade_link", ""),
            trade_search_link=data.get("trade_search_link", ""),
            listing_currency=data.get("listing_currency", "chaos"),
            listing_amount=data.get("listing_amount", 0.0),
            seller=data.get("seller", ""),
            indexed_at=data.get("indexed_at"),
            resolved_league=data.get("resolved_league", ""),
            corrupted=data.get("corrupted", False),
            fractured=data.get("fractured", False),
            influences=data.get("influences", []),
            explicit_mods=data.get("explicit_mods", []),
            implicit_mods=data.get("implicit_mods", []),
            prefix_count=data.get("prefix_count", 0),
            suffix_count=data.get("suffix_count", 0),
            open_prefixes=data.get("open_prefixes", 3),
            open_suffixes=data.get("open_suffixes", 3),
            mod_tokens=data.get("mod_tokens", []),
            tag_tokens=data.get("tag_tokens", []),
            trusted_profit=data.get("trusted_profit", 0.0),
            valuation_result=data.get("valuation_result", {}),
            market_floor=data.get("market_floor", 0.0),
            market_median=data.get("market_median", 0.0),
            comparables_count=data.get("comparables_count", 0),
            market_spread=data.get("market_spread", 0.0),
            pricing_position=data.get("pricing_position", "near_market"),
            risk_flags=data.get("risk_flags", []),
            valuation_explanation=data.get("valuation_explanation", ""),
            numeric_mod_features=data.get("numeric_mod_features", {}),
            tier_source=data.get("tier_source", "none"),
            native_tier_count=data.get("native_tier_count", 0),
            twink_override=data.get("twink_override", False),
            low_ilvl_context=data.get("low_ilvl_context", False),
            tier_ilvl_mismatch=data.get("tier_ilvl_mismatch", False),
            fractured_low_ilvl_brick=data.get("fractured_low_ilvl_brick", False),
        )


class OnDemandScanner:
    """Scanner de mercado sob demanda com valuation por família."""

    def __init__(self, league: str = "auto"):
        self.api_client = MarketAPIClient(league=league)
        self.oracle = PricePredictor()
        self.currency_rates = self.api_client.sync_ninja_economy()
        self._segment_cursor = 0
        self._dedupe_ttl_seconds = 120.0
        self._dedupe_ttl_cache: Dict[str, float] = {}
        self._query_budget_per_cycle = 8
        self._fetch_budget_per_cycle = 12
        self._stage_a_candidate_cap = 120
        self._scan_error_count = 0
        logger.info(
            "[%s] Scanner inicializado com %s taxas de moeda",
            self.api_client.league,
            len(self.currency_rates),
        )

    def build_trade_query(
        self,
        item_class: str = "",
        ilvl_min: int = 1,
        rarity: str = "rare",
        is_influenced: bool = False,
        min_listed_price: float = 1.0,
    ) -> dict:
        query_min_price = max(min_listed_price, 1.0)
        query: dict = {
            "query": {
                "status": {"option": "online"},
                "filters": {
                    "trade_filters": {"filters": {"price": {"min": query_min_price}}},
                    "type_filters": {"filters": {"rarity": {"option": rarity}}},
                    "misc_filters": {"filters": {"ilvl": {"min": ilvl_min}}},
                },
            },
            "sort": {"price": "asc"},
        }

        if item_class:
            query["query"]["type"] = item_class

        if is_influenced:
            # Trade API atual rejeita o filtro legacy `misc_filters.filters.influence`.
            # Mantemos a assinatura por compatibilidade e ignoramos a variação aqui.
            pass

        return query

    def _cleanup_ttl_cache(self, now_ts: float) -> None:
        if not self._dedupe_ttl_cache:
            return
        expired = [
            item_id
            for item_id, seen_at in self._dedupe_ttl_cache.items()
            if (now_ts - seen_at) >= self._dedupe_ttl_seconds
        ]
        for item_id in expired:
            self._dedupe_ttl_cache.pop(item_id, None)

    def _is_deduped_by_ttl(self, item_id: str, now_ts: float) -> bool:
        if not item_id:
            return False
        seen_at = self._dedupe_ttl_cache.get(item_id)
        if seen_at is not None and (now_ts - seen_at) < self._dedupe_ttl_seconds:
            return True
        self._dedupe_ttl_cache[item_id] = now_ts
        return False

    def _build_segmented_macro_queries(
        self,
        item_class: str,
        ilvl_min: int,
        rarity: str,
        min_listed_price: float,
    ) -> List[dict]:
        price_buckets: List[Tuple[float, Optional[float]]] = [
            (1.0, 10.0),
            (10.0, 40.0),
            (40.0, 120.0),
            (120.0, 300.0),
            (300.0, None),
        ]

        ilvl_buckets: List[Tuple[int, Optional[int]]] = []
        if ilvl_min <= 74:
            ilvl_buckets.append((ilvl_min, 74))
        ilvl_buckets.append((max(ilvl_min, 75), 83))
        ilvl_buckets.append((max(ilvl_min, 84), None))

        segments: List[dict] = []
        for ilvl_low, ilvl_high in ilvl_buckets:
            if ilvl_high is not None and ilvl_low > ilvl_high:
                continue
            for min_price, max_price in price_buckets:
                effective_min_price = max(min_price, min_listed_price, 1.0)
                if max_price is not None and effective_min_price >= max_price:
                    continue
                for influenced in (False,):
                    query = self.build_trade_query(
                        item_class=item_class,
                        ilvl_min=ilvl_low,
                        rarity=rarity,
                        is_influenced=influenced,
                        min_listed_price=effective_min_price,
                    )
                    price_filters = query["query"]["filters"]["trade_filters"][
                        "filters"
                    ].setdefault("price", {})
                    price_filters["min"] = effective_min_price
                    if max_price is not None:
                        price_filters["max"] = max_price
                    ilvl_filters = query["query"]["filters"]["misc_filters"][
                        "filters"
                    ].setdefault("ilvl", {})
                    ilvl_filters["min"] = ilvl_low
                    if ilvl_high is not None:
                        ilvl_filters["max"] = ilvl_high
                    segments.append(query)
        return segments

    def _build_micro_queries(
        self,
        ilvl_min: int,
        rarity: str,
        min_listed_price: float,
    ) -> List[dict]:
        micro_price_cap = max(40.0, min(220.0, max(min_listed_price * 3.0, 80.0)))
        micro_bases = [
            "Imbued Wand",
            "Opal Ring",
            "Sadist Garb",
            "Hubris Circlet",
            "Two-Toned Boots",
            "Stygian Vise",
        ]
        queries: List[dict] = []
        for base in micro_bases:
            query = self.build_trade_query(
                item_class=base,
                ilvl_min=max(ilvl_min, 84),
                rarity=rarity,
                is_influenced=False,
                min_listed_price=max(min_listed_price, 1.0),
            )
            query["query"]["filters"]["trade_filters"]["filters"]["price"]["max"] = (
                micro_price_cap
            )
            queries.append(query)
        return queries

    def _rotate_macro_segments(self, segments: List[dict], budget: int) -> List[dict]:
        if not segments or budget <= 0:
            return []
        rotated = [
            segments[(self._segment_cursor + idx) % len(segments)]
            for idx in range(min(budget, len(segments)))
        ]
        self._segment_cursor = (self._segment_cursor + budget) % len(segments)
        return rotated

    def _safe_search_items(self, query: dict) -> Tuple[str, List[str]]:
        try:
            return self.api_client.search_items(query)
        except Exception as exc:
            self._scan_error_count += 1
            logger.warning("Falha em search_items; continuando ciclo: %s", exc)
            return "", []

    def _safe_fetch_item_details(
        self, item_ids: List[str], query_id: str
    ) -> List[dict]:
        try:
            return self.api_client.fetch_item_details(item_ids, query_id)
        except Exception as exc:
            self._scan_error_count += 1
            logger.warning("Falha em fetch_item_details; continuando ciclo: %s", exc)
            return []

    def _emit_scan_metric(
        self,
        *,
        run_id: str,
        started_at: float,
        status: str,
        stats: ScanStats,
        max_items: int,
        item_class: str,
        safe_buy: bool,
    ) -> None:
        duration_ms = max((time.time() - started_at) * 1000.0, 0.0)
        try:
            append_metric_event(
                component="market_scanner.scan_opportunities",
                run_id=run_id,
                duration_ms=duration_ms,
                status=status,
                error_count=self._scan_error_count,
                payload={
                    "resolved_league": stats.resolved_league,
                    "scan_profile": stats.scan_profile,
                    "max_items": max_items,
                    "item_class": item_class,
                    "safe_buy": safe_buy,
                    "total_found": stats.total_found,
                    "total_evaluated": stats.total_evaluated,
                    "stage_a_candidates": stats.stage_a_candidates,
                    "filtered_stage_a_low_ilvl_no_twink": stats.filtered_stage_a_low_ilvl_no_twink,
                    "filtered_stage_a_fractured_low_ilvl_brick": stats.filtered_stage_a_fractured_low_ilvl_brick,
                    "stage_b_passed": stats.stage_b_passed,
                    "macro_queries": stats.macro_queries,
                    "micro_queries": stats.micro_queries,
                    "deduped_ttl": stats.deduped_ttl,
                    "avg_profit": stats.avg_profit,
                    "max_profit": stats.max_profit,
                    "avg_score": stats.avg_score,
                },
            )
        except Exception:
            logger.debug(
                "Falha ao emitir métrica operacional do scanner", exc_info=True
            )

    def _passes_stage_b_consensus(self, opportunity: ScanOpportunity) -> bool:
        consensus_ok, _ = self._stage_b_consensus_decision(opportunity)
        return consensus_ok

    def _high_ticket_ilvl_min(self, item_family: str) -> int:
        minimums = {
            "wand_caster": 75,
            "body_armour_defense": 78,
            "jewel_cluster": 80,
            "accessory_generic": 75,
            "generic": 75,
        }
        return minimums.get(item_family, minimums["generic"])

    def _is_high_ticket(self, opportunity: ScanOpportunity) -> bool:
        return opportunity.ml_value >= 150.0 or opportunity.profit >= 80.0

    def _stage_b_consensus_decision(
        self, opportunity: ScanOpportunity
    ) -> Tuple[bool, str]:
        if opportunity.comparables_count < 3 and opportunity.market_median > 0:
            ml_market_ratio = opportunity.ml_value / max(opportunity.market_median, 1.0)
            if ml_market_ratio > 3.0:
                return (False, "low_evidence_ml_market_divergence_3x")
            if ml_market_ratio > 2.0:
                return (False, "low_evidence_ml_market_divergence_2x")

        if (
            opportunity.low_ilvl_context
            and not opportunity.twink_override
            and opportunity.pricing_position == "outlier"
        ):
            return (False, "low_ilvl_outlier")

        if (
            self._is_high_ticket(opportunity)
            and not opportunity.twink_override
            and (
                opportunity.low_ilvl_context
                or opportunity.ilvl
                < self._high_ticket_ilvl_min(opportunity.item_family)
            )
        ):
            return (False, "high_ticket_low_ilvl_without_override")

        if (
            opportunity.valuation_result.get("model_source") == "family_fallback"
            and opportunity.comparables_count < 3
        ):
            if opportunity.pricing_position == "outlier":
                return (False, "fallback_low_evidence_outlier")
            if opportunity.ml_confidence < 0.65:
                return (False, "fallback_low_evidence_low_confidence")
            if opportunity.profit < 15.0 and opportunity.relative_discount < 0.18:
                return (False, "fallback_low_evidence_weak_value")
            if opportunity.low_ilvl_context and not opportunity.twink_override:
                return (False, "fallback_low_evidence_low_ilvl")

        ml_signal = (
            opportunity.profit > 0 and opportunity.ml_value > opportunity.listed_price
        )

        comparable_signal = False
        if opportunity.comparables_count <= 1:
            comparable_signal = (
                opportunity.relative_discount >= 0.25 or opportunity.profit >= 20.0
            )
        elif opportunity.pricing_position == "below_floor":
            comparable_signal = True
        elif opportunity.market_median > 0:
            comparable_signal = opportunity.listed_price <= (
                opportunity.market_median * 0.94
            )
        elif opportunity.market_floor > 0:
            comparable_signal = opportunity.listed_price <= opportunity.market_floor

        if not ml_signal:
            return (False, "ml_signal_negative")
        if not comparable_signal:
            return (False, "market_signal_negative")
        return (True, "ml_market_consensus_ok")

    def _build_valuation_explanation(self, opportunity: ScanOpportunity) -> str:
        summary = (
            f"IA estima {opportunity.ml_value:.1f}c para {opportunity.base_type} "
            f"(confiança {opportunity.ml_confidence:.2f}) contra {opportunity.listed_price:.1f}c "
            f"listado, gap de {opportunity.profit:.1f}c."
        )

        if opportunity.comparables_count > 0:
            market_context = (
                f" Comparáveis: piso {opportunity.market_floor:.1f}c, "
                f"mediana {opportunity.market_median:.1f}c, "
                f"spread {opportunity.market_spread:.1f}c, "
                f"posição {opportunity.pricing_position}."
            )
        else:
            market_context = (
                " Sem comparáveis suficientes; priorizando sinal do modelo."
            )

        if opportunity.risk_flags:
            risk_context = f" Riscos: {', '.join(opportunity.risk_flags)}."
        else:
            risk_context = " Sem flags de risco relevantes."

        plausibility_bits: List[str] = []
        if opportunity.tier_source != "none":
            plausibility_bits.append(f"tier_source={opportunity.tier_source}")
        if opportunity.tier_ilvl_mismatch:
            plausibility_bits.append("tier_ilvl_mismatch")
        if opportunity.native_tier_count > 0:
            plausibility_bits.append(f"native_tiers={opportunity.native_tier_count}")
        if opportunity.twink_override:
            plausibility_bits.append("twink_override")
        if opportunity.low_ilvl_context and not opportunity.twink_override:
            plausibility_bits.append("low_ilvl_context")
        if opportunity.valuation_result.get("ml_value_cap_applied"):
            cap_before = float(
                opportunity.valuation_result.get(
                    "ml_value_before_cap", opportunity.ml_value
                )
            )
            cap_after = float(
                opportunity.valuation_result.get(
                    "ml_value_after_cap", opportunity.ml_value
                )
            )
            plausibility_bits.append(f"cap={cap_before:.1f}->{cap_after:.1f}")
        if plausibility_bits:
            plausibility_context = f" Plausibilidade: {', '.join(plausibility_bits)}."
        else:
            plausibility_context = ""

        consensus, reason = self._stage_b_consensus_decision(opportunity)
        consensus_context = (
            f" Consenso ML+mercado: aprovado ({reason})."
            if consensus
            else f" Consenso ML+mercado: bloqueado ({reason})."
        )

        return (
            f"{summary}{market_context}{risk_context}{plausibility_context}"
            f"{consensus_context}"
        )

    def _is_low_ilvl_by_family(self, item_family: str, ilvl: int) -> bool:
        thresholds = {
            "wand_caster": 82,
            "body_armour_defense": 84,
            "jewel_cluster": 84,
            "accessory_generic": 82,
            "generic": 80,
        }
        threshold = thresholds.get(item_family, thresholds["generic"])
        return ilvl < threshold

    def extract_price_chaos(self, listing_json: dict) -> Optional[float]:
        price_info = listing_json.get("price", {})
        currency = str(price_info.get("currency", ""))

        try:
            amount = float(price_info.get("amount", 0.0))
        except (ValueError, TypeError):
            return None

        if amount <= 0:
            return None

        if currency == "chaos":
            return amount

        ninja_key_map = {
            "divine": "Divine Orb",
            "exalted": "Exalted Orb",
            "mirror": "Mirror of Kalandra",
            "alch": "Orb of Alchemy",
        }

        ninja_key = ninja_key_map.get(currency) or (currency.title() + " Orb")
        rate = self.currency_rates.get(ninja_key)
        if rate is None:
            logger.warning(
                "Moeda não encontrada nas taxas: %s (%s)", currency, ninja_key
            )
            return None

        return amount * rate

    def _listing_age_hours(self, indexed_at: Optional[str]) -> Optional[float]:
        if not indexed_at:
            return None
        try:
            indexed_dt = datetime.fromisoformat(indexed_at.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            return None
        return (datetime.now(timezone.utc) - indexed_dt).total_seconds() / 3600

    def _scan_profile(self, item_class: str) -> str:
        return "targeted" if item_class.strip() else "open_market"

    def _is_probable_price_fix(
        self,
        listed_price_chaos: float,
        ml_value: float,
        indexed_at: Optional[str],
        stale_hours: float,
    ) -> bool:
        age_hours = self._listing_age_hours(indexed_at)
        if age_hours is None:
            return False
        return (
            age_hours > stale_hours
            and listed_price_chaos <= 2.0
            and listed_price_chaos > 0
            and (ml_value / listed_price_chaos) >= 8.0
        )

    def _risk_flags(
        self,
        item: NormalizedMarketItem,
        valuation: ValuationResult,
        stale_hours: float,
    ) -> List[str]:
        flags: List[str] = []

        if self._is_probable_price_fix(
            item.listed_price,
            valuation.predicted_value,
            item.listed_at,
            stale_hours,
        ):
            flags.append("price_fix_suspected")

        age_hours = self._listing_age_hours(item.listed_at)
        if age_hours is not None and age_hours > stale_hours:
            flags.append("stale_listing")

        if valuation.confidence < 0.5:
            flags.append("low_confidence")

        if item.listed_price >= 80 and valuation.confidence < 0.75:
            flags.append("high_ticket_low_confidence")

        if item.listed_price < 5:
            flags.append("cheap_listing")

        if item.corrupted:
            flags.append("corrupted")

        if item.fractured:
            flags.append("fractured")

        if item.influences:
            flags.append("influenced")

        if valuation.model_source == "family_fallback":
            flags.append("family_fallback")

        if (
            self._is_low_ilvl_by_family(item.item_family, item.ilvl)
            and not item.twink_override
        ):
            flags.append("low_ilvl_context")

        if item.tier_ilvl_mismatch:
            flags.append("tier_ilvl_mismatch")

        return flags

    def _open_market_filter_reason(self, opportunity: ScanOpportunity) -> Optional[str]:
        if opportunity.ml_confidence < 0.45:
            return "filtered_open_confidence"

        if opportunity.listed_price < 5.0:
            if opportunity.ml_confidence < 0.60:
                return "filtered_open_cheap_low_confidence"
            if opportunity.profit < 20.0:
                return "filtered_open_cheap_low_profit"

            age_hours = self._listing_age_hours(opportunity.indexed_at)
            if age_hours is not None and age_hours > 12.0:
                return "filtered_open_cheap_stale"

        return None

    def _compute_base_score(
        self,
        item: NormalizedMarketItem,
        valuation: ValuationResult,
        risk_flags: List[str],
    ) -> Tuple[float, float, float, float]:
        if item.listed_price <= 0:
            return (0.0, 0.0, 0.0, 0.0)

        profit = valuation.predicted_value - item.listed_price
        trusted_profit = max(profit, 0.0) * valuation.confidence
        roi = max(profit / max(item.listed_price, 1.0), 0.0)
        relative_discount = max(profit / max(valuation.predicted_value, 1.0), 0.0)

        score = 0.0
        score += trusted_profit * 0.9
        score += min(roi, 3.0) * 16.0
        score += min(relative_discount, 0.8) * 20.0
        score += valuation.confidence * 25.0

        penalties = {
            "price_fix_suspected": 45.0,
            "stale_listing": 14.0,
            "low_confidence": 18.0,
            "high_ticket_low_confidence": 18.0,
            "cheap_listing": 18.0,
            "corrupted": 10.0,
            "family_fallback": 6.0,
            "low_ilvl_context": 12.0,
            "tier_ilvl_mismatch": 20.0,
            "fallback_low_evidence": 15.0,
        }
        score -= sum(penalties.get(flag, 0.0) for flag in risk_flags)
        if "cheap_listing" in risk_flags and 0.45 <= valuation.confidence < 0.60:
            score -= 16.0

        return (
            round(max(score, 0.0), 1),
            round(trusted_profit, 1),
            round(profit, 1),
            round(relative_discount, 2),
        )

    def _apply_market_context(
        self,
        base_score: float,
        item: NormalizedMarketItem,
        valuation: ValuationResult,
        comparables: ComparableMarketStats,
    ) -> float:
        score = base_score
        if comparables.pricing_position == "below_floor":
            score += 14.0
        elif comparables.pricing_position == "near_market":
            score += 4.0
        else:
            score -= 8.0

        market_gap = comparables.market_median - item.listed_price
        if market_gap > 0:
            score += min(market_gap, 80.0) * 0.08
        if comparables.comparables_count <= 1:
            score -= 6.0
        if comparables.market_spread > max(comparables.market_median * 1.1, 30.0):
            score -= 4.0
        if valuation.feature_completeness < 0.4:
            score -= 5.0
        return round(max(score, 0.0), 1)

    def _build_listing_snapshot(
        self,
        normalized_item: NormalizedMarketItem,
        query_id: str,
    ) -> ListingSnapshot:
        league_encoded = quote(self.api_client.league, safe="")
        search_link = (
            f"https://www.pathofexile.com/trade/search/{league_encoded}/{query_id}"
        )
        trade_link = (
            f"{search_link}#{normalized_item.item_id}"
            if normalized_item.item_id
            else search_link
        )
        return ListingSnapshot(
            item_id=normalized_item.item_id,
            base_type=normalized_item.base_type,
            ilvl=normalized_item.ilvl,
            listed_price=normalized_item.listed_price,
            listing_currency=normalized_item.listing_currency,
            listing_amount=normalized_item.listing_amount,
            seller=normalized_item.seller,
            indexed_at=normalized_item.listed_at,
            whisper=normalized_item.whisper,
            trade_link=trade_link,
            trade_search_link=search_link,
            corrupted=normalized_item.corrupted,
            fractured=normalized_item.fractured,
            influences=normalized_item.influences,
            explicit_mods=normalized_item.explicit_mods,
            implicit_mods=normalized_item.implicit_mods,
            item_family=normalized_item.item_family,
            prefix_count=normalized_item.prefix_count,
            suffix_count=normalized_item.suffix_count,
            open_prefixes=normalized_item.open_prefixes,
            open_suffixes=normalized_item.open_suffixes,
            mod_tokens=normalized_item.mod_tokens,
            tag_tokens=normalized_item.tag_tokens,
            numeric_mod_features=normalized_item.numeric_mod_features,
            tier_source=normalized_item.tier_source,
            native_tier_count=normalized_item.native_tier_count,
            twink_override=normalized_item.twink_override,
            tier_ilvl_mismatch=normalized_item.tier_ilvl_mismatch,
            low_ilvl_context=normalized_item.low_ilvl_context,
            fractured_low_ilvl_brick=normalized_item.fractured_low_ilvl_brick,
        )

    def _apply_low_evidence_cap(self, opportunity: ScanOpportunity) -> None:
        if not (
            opportunity.valuation_result.get("model_source") == "family_fallback"
            and opportunity.comparables_count < 3
        ):
            return

        if "fallback_low_evidence" not in opportunity.risk_flags:
            opportunity.risk_flags.append("fallback_low_evidence")

        anchor = opportunity.market_median
        if anchor <= 0:
            anchor = opportunity.market_floor
        if anchor <= 0:
            anchor = opportunity.listed_price
        cap_multiplier = 1.6
        cap_value = max(anchor * cap_multiplier, opportunity.listed_price * 1.1, 15.0)

        if opportunity.ml_value <= cap_value:
            return

        before = float(opportunity.ml_value)
        opportunity.ml_value = round(float(cap_value), 1)
        opportunity.profit = round(opportunity.ml_value - opportunity.listed_price, 1)
        opportunity.valuation_gap = opportunity.profit
        opportunity.relative_discount = round(
            max(opportunity.profit / max(opportunity.ml_value, 1.0), 0.0),
            2,
        )
        opportunity.trusted_profit = round(
            max(opportunity.profit, 0.0) * opportunity.ml_confidence,
            1,
        )
        opportunity.valuation_result["ml_value_before_cap"] = before
        opportunity.valuation_result["ml_value_after_cap"] = opportunity.ml_value
        opportunity.valuation_result["ml_value_cap_applied"] = True

    def _apply_low_evidence_ml_market_penalty(
        self, opportunity: ScanOpportunity
    ) -> None:
        if opportunity.comparables_count >= 3 or opportunity.market_median <= 0:
            return

        ml_market_ratio = opportunity.ml_value / max(opportunity.market_median, 1.0)
        if ml_market_ratio > 3.0:
            if "low_evidence_ml_market_divergence_3x" not in opportunity.risk_flags:
                opportunity.risk_flags.append("low_evidence_ml_market_divergence_3x")
            opportunity.score = round(max(opportunity.score - 80.0, 0.0), 1)
            return

        if ml_market_ratio > 2.0:
            if "low_evidence_ml_market_divergence_2x" not in opportunity.risk_flags:
                opportunity.risk_flags.append("low_evidence_ml_market_divergence_2x")
            opportunity.score = round(max(opportunity.score - 55.0, 0.0), 1)

    def _build_opportunity(
        self,
        item_json: dict,
        query_id: str,
        stale_hours: float,
        normalized_item: Optional[NormalizedMarketItem] = None,
    ) -> Optional[tuple[ScanOpportunity, NormalizedMarketItem]]:
        listing = item_json.get("listing", {})
        item_data = item_json.get("item", {})
        listed_price = self.extract_price_chaos(listing)
        if listed_price is None or not item_data:
            return None

        if normalized_item is None:
            price_info = listing.get("price", {})
            normalized_item = normalize_trade_item(
                item_json,
                listed_price=listed_price,
                listing_currency=price_info.get("currency", "chaos"),
                listing_amount=float(price_info.get("amount", 0.0) or 0.0),
            )
        if normalized_item is None:
            return None

        valuation = self.oracle.predict(normalized_item)
        risk_flags = self._risk_flags(normalized_item, valuation, stale_hours)
        base_score, trusted_profit, profit, relative_discount = (
            self._compute_base_score(
                normalized_item,
                valuation,
                risk_flags,
            )
        )
        snapshot = self._build_listing_snapshot(normalized_item, query_id)

        opportunity = ScanOpportunity(
            item_id=snapshot.item_id,
            base_type=snapshot.base_type,
            item_family=snapshot.item_family,
            ilvl=snapshot.ilvl,
            listed_price=snapshot.listed_price,
            ml_value=valuation.predicted_value,
            ml_confidence=valuation.confidence,
            profit=profit,
            score=base_score,
            valuation_gap=profit,
            relative_discount=relative_discount,
            whisper=snapshot.whisper,
            trade_link=snapshot.trade_link,
            trade_search_link=snapshot.trade_search_link,
            listing_currency=snapshot.listing_currency,
            listing_amount=snapshot.listing_amount,
            seller=snapshot.seller,
            indexed_at=snapshot.indexed_at,
            resolved_league=self.api_client.league,
            corrupted=snapshot.corrupted,
            fractured=snapshot.fractured,
            influences=snapshot.influences,
            explicit_mods=snapshot.explicit_mods,
            implicit_mods=snapshot.implicit_mods,
            prefix_count=snapshot.prefix_count,
            suffix_count=snapshot.suffix_count,
            open_prefixes=snapshot.open_prefixes,
            open_suffixes=snapshot.open_suffixes,
            mod_tokens=snapshot.mod_tokens,
            tag_tokens=snapshot.tag_tokens,
            trusted_profit=trusted_profit,
            valuation_result=valuation.to_dict(),
            risk_flags=risk_flags,
            numeric_mod_features=snapshot.numeric_mod_features,
            tier_source=snapshot.tier_source,
            native_tier_count=snapshot.native_tier_count,
            twink_override=snapshot.twink_override,
            low_ilvl_context=snapshot.low_ilvl_context,
            tier_ilvl_mismatch=snapshot.tier_ilvl_mismatch,
            fractured_low_ilvl_brick=snapshot.fractured_low_ilvl_brick,
        )
        opportunity.valuation_explanation = self._build_valuation_explanation(
            opportunity
        )
        return (opportunity, normalized_item)

    def _enrich_market_context(
        self,
        raw_opportunities: List[tuple[ScanOpportunity, NormalizedMarketItem]],
    ) -> List[ScanOpportunity]:
        grouped_prices: Dict[tuple[str, str], List[float]] = {}
        for _, normalized_item in raw_opportunities:
            key = (normalized_item.item_family, normalized_item.base_type)
            grouped_prices.setdefault(key, []).append(normalized_item.listed_price)

        enriched: List[ScanOpportunity] = []
        for opportunity, normalized_item in raw_opportunities:
            key = (normalized_item.item_family, normalized_item.base_type)
            all_prices = list(grouped_prices.get(key, []))
            comparable_prices = [
                price
                for price in all_prices
                if price != normalized_item.listed_price or len(all_prices) == 1
            ]
            comparables = build_comparable_market_stats(
                normalized_item, comparable_prices
            )
            opportunity.market_floor = comparables.market_floor
            opportunity.market_median = comparables.market_median
            opportunity.market_spread = comparables.market_spread
            opportunity.comparables_count = comparables.comparables_count
            opportunity.pricing_position = comparables.pricing_position
            self._apply_low_evidence_cap(opportunity)
            opportunity.score = self._apply_market_context(
                opportunity.score,
                normalized_item,
                ValuationResult(
                    predicted_value=float(
                        opportunity.valuation_result.get(
                            "predicted_value", opportunity.ml_value
                        )
                    ),
                    confidence=float(
                        opportunity.valuation_result.get(
                            "confidence", opportunity.ml_confidence
                        )
                    ),
                    item_family=str(
                        opportunity.valuation_result.get(
                            "item_family", opportunity.item_family
                        )
                    ),
                    model_source=str(
                        opportunity.valuation_result.get(
                            "model_source", "family_fallback"
                        )
                    ),
                    feature_completeness=float(
                        opportunity.valuation_result.get("feature_completeness", 0.0)
                    ),
                ),
                comparables,
            )
            self._apply_low_evidence_ml_market_penalty(opportunity)
            opportunity.valuation_explanation = self._build_valuation_explanation(
                opportunity
            )
            enriched.append(opportunity)
        return enriched

    def scan_opportunities(
        self,
        item_class: str = "",
        ilvl_min: int = 1,
        rarity: str = "rare",
        max_items: int = 30,
        min_profit: float = float("-inf"),
        min_listed_price: float = 0.0,
        anti_fix: bool = True,
        stale_hours: float = 48.0,
        safe_buy: bool = False,
    ) -> Tuple[List[ScanOpportunity], ScanStats]:
        run_id = str(int(time.time() * 1000))
        started_at = time.time()
        self._scan_error_count = 0

        if max_items <= 0:
            stats = ScanStats(
                resolved_league=self.api_client.league,
                scan_profile=self._scan_profile(item_class),
            )
            self._emit_scan_metric(
                run_id=run_id,
                started_at=started_at,
                status="ok",
                stats=stats,
                max_items=max_items,
                item_class=item_class,
                safe_buy=safe_buy,
            )
            return [], stats

        scan_profile = self._scan_profile(item_class)

        now_ts = time.time()
        self._cleanup_ttl_cache(now_ts)

        query_budget = max(2, min(self._query_budget_per_cycle, (max_items // 4) + 4))
        fetch_budget = max(2, min(self._fetch_budget_per_cycle, (max_items // 3) + 4))

        macro_segments = self._build_segmented_macro_queries(
            item_class=item_class,
            ilvl_min=ilvl_min,
            rarity=rarity,
            min_listed_price=min_listed_price,
        )
        macro_budget = max(1, int(query_budget * 0.7))
        rotated_macro = self._rotate_macro_segments(macro_segments, macro_budget)

        micro_queries = self._build_micro_queries(
            ilvl_min=ilvl_min,
            rarity=rarity,
            min_listed_price=min_listed_price,
        )
        if item_class.strip():
            targeted_query = self.build_trade_query(
                item_class=item_class,
                ilvl_min=ilvl_min,
                rarity=rarity,
                is_influenced=False,
                min_listed_price=max(min_listed_price, 1.0),
            )
            micro_queries.insert(0, targeted_query)

        micro_budget = max(1, query_budget - len(rotated_macro))
        selected_micro = micro_queries[:micro_budget]

        candidate_query_map: Dict[str, str] = {}
        query_to_candidates: Dict[str, List[str]] = {}
        total_found = 0
        macro_queries_executed = 0
        micro_queries_executed = 0

        stage_a_candidate_cap = min(self._stage_a_candidate_cap, max(max_items * 6, 40))

        for query in rotated_macro:
            query_id, result_ids = self._safe_search_items(query)
            macro_queries_executed += 1
            if not query_id or not result_ids:
                continue
            total_found += len(result_ids)
            for result_id in result_ids:
                if result_id in candidate_query_map:
                    continue
                candidate_query_map[result_id] = query_id
                query_to_candidates.setdefault(query_id, []).append(result_id)
                if len(candidate_query_map) >= stage_a_candidate_cap:
                    break
            if len(candidate_query_map) >= stage_a_candidate_cap:
                break

        if len(candidate_query_map) < stage_a_candidate_cap:
            for query in selected_micro:
                query_id, result_ids = self._safe_search_items(query)
                micro_queries_executed += 1
                if not query_id or not result_ids:
                    continue
                total_found += len(result_ids)
                for result_id in result_ids:
                    if result_id in candidate_query_map:
                        continue
                    candidate_query_map[result_id] = query_id
                    query_to_candidates.setdefault(query_id, []).append(result_id)
                    if len(candidate_query_map) >= stage_a_candidate_cap:
                        break
                if len(candidate_query_map) >= stage_a_candidate_cap:
                    break

        budget_exhausted = int(
            len(rotated_macro) < len(macro_segments)
            or len(selected_micro) < len(micro_queries)
            or len(candidate_query_map) >= stage_a_candidate_cap
        )

        if not candidate_query_map:
            stats = ScanStats(
                resolved_league=self.api_client.league,
                scan_profile=scan_profile,
                macro_queries=macro_queries_executed,
                micro_queries=micro_queries_executed,
                stage_a_candidates=0,
                budget_exhausted=budget_exhausted,
            )
            self._emit_scan_metric(
                run_id=run_id,
                started_at=started_at,
                status="error" if self._scan_error_count > 0 else "ok",
                stats=stats,
                max_items=max_items,
                item_class=item_class,
                safe_buy=safe_buy,
            )
            return [], stats

        raw_opportunities: List[tuple[ScanOpportunity, NormalizedMarketItem]] = []
        total_evaluated = 0
        filtered_anti_fix = 0
        filtered_min_profit = 0
        filtered_min_listed_price = 0
        skipped_invalid_currency = 0
        filtered_safe_buy_confidence = 0
        filtered_safe_buy_age = 0
        filtered_safe_buy_price = 0
        filtered_open_confidence = 0
        filtered_open_cheap_low_confidence = 0
        filtered_open_cheap_low_profit = 0
        filtered_open_cheap_stale = 0
        filtered_stage_a_low_ilvl_no_twink = 0
        filtered_stage_a_fractured_low_ilvl_brick = 0

        deduped_ttl = 0
        fetch_calls = 0
        for query_id, item_ids in query_to_candidates.items():
            for i in range(0, len(item_ids), 10):
                if fetch_calls >= fetch_budget:
                    budget_exhausted = 1
                    break
                details = self._safe_fetch_item_details(item_ids[i : i + 10], query_id)
                fetch_calls += 1
                for item_json in details:
                    listing = item_json.get("listing", {})
                    if not listing.get("whisper"):
                        continue

                    item_data = item_json.get("item", {})
                    item_id = str(item_data.get("id", ""))
                    if self._is_deduped_by_ttl(item_id, now_ts):
                        deduped_ttl += 1
                        continue

                    listed_price = self.extract_price_chaos(listing)
                    if listed_price is None:
                        skipped_invalid_currency += 1
                        continue

                    price_info = listing.get("price", {})
                    normalized_item = normalize_trade_item(
                        item_json,
                        listed_price=listed_price,
                        listing_currency=price_info.get("currency", "chaos"),
                        listing_amount=float(price_info.get("amount", 0.0) or 0.0),
                    )
                    if normalized_item is None:
                        continue

                    if normalized_item.ilvl < 75 and not normalized_item.twink_override:
                        filtered_stage_a_low_ilvl_no_twink += 1
                        continue

                    if normalized_item.fractured_low_ilvl_brick:
                        filtered_stage_a_fractured_low_ilvl_brick += 1
                        continue

                    built = self._build_opportunity(
                        item_json,
                        query_id,
                        stale_hours,
                        normalized_item=normalized_item,
                    )
                    if built is None:
                        continue

                    opportunity, normalized_item = built
                    total_evaluated += 1

                    if opportunity.listed_price < min_listed_price:
                        filtered_min_listed_price += 1
                        continue
                    if anti_fix and "price_fix_suspected" in opportunity.risk_flags:
                        filtered_anti_fix += 1
                        continue

                    raw_opportunities.append((opportunity, normalized_item))
            if fetch_calls >= fetch_budget:
                break

        opportunities = self._enrich_market_context(raw_opportunities)
        stage_b_opportunities = [
            opportunity
            for opportunity in opportunities
            if self._passes_stage_b_consensus(opportunity)
        ]
        filtered_opportunities: List[ScanOpportunity] = []
        for opportunity in stage_b_opportunities:
            if scan_profile == "open_market":
                open_market_reason = self._open_market_filter_reason(opportunity)
                if open_market_reason == "filtered_open_confidence":
                    filtered_open_confidence += 1
                    continue
                if open_market_reason == "filtered_open_cheap_low_confidence":
                    filtered_open_cheap_low_confidence += 1
                    continue
                if open_market_reason == "filtered_open_cheap_low_profit":
                    filtered_open_cheap_low_profit += 1
                    continue
                if open_market_reason == "filtered_open_cheap_stale":
                    filtered_open_cheap_stale += 1
                    continue

            if safe_buy:
                confidence_threshold = 0.70
                if opportunity.listed_price >= 120:
                    confidence_threshold = 0.82
                elif opportunity.listed_price >= 50:
                    confidence_threshold = 0.78
                if opportunity.ml_confidence < confidence_threshold:
                    filtered_safe_buy_confidence += 1
                    continue
                age_hours = self._listing_age_hours(opportunity.indexed_at)
                if age_hours is not None and age_hours > 24:
                    filtered_safe_buy_age += 1
                    continue
                if opportunity.listed_price < 5:
                    filtered_safe_buy_price += 1
                    continue

            if min_profit > float("-inf") and opportunity.profit < min_profit:
                filtered_min_profit += 1
                continue

            filtered_opportunities.append(opportunity)

        filtered_opportunities.sort(
            key=lambda opp: (opp.score, opp.trusted_profit, opp.profit),
            reverse=True,
        )
        filtered_opportunities = filtered_opportunities[:max_items]

        stats = ScanStats(
            total_found=total_found,
            total_evaluated=total_evaluated,
            filtered_anti_fix=filtered_anti_fix,
            filtered_min_profit=filtered_min_profit,
            filtered_min_listed_price=filtered_min_listed_price,
            skipped_invalid_currency=skipped_invalid_currency,
            filtered_safe_buy_confidence=filtered_safe_buy_confidence,
            filtered_safe_buy_age=filtered_safe_buy_age,
            filtered_safe_buy_price=filtered_safe_buy_price,
            filtered_open_confidence=filtered_open_confidence,
            filtered_open_cheap_low_confidence=filtered_open_cheap_low_confidence,
            filtered_open_cheap_low_profit=filtered_open_cheap_low_profit,
            filtered_open_cheap_stale=filtered_open_cheap_stale,
            filtered_stage_a_low_ilvl_no_twink=filtered_stage_a_low_ilvl_no_twink,
            filtered_stage_a_fractured_low_ilvl_brick=filtered_stage_a_fractured_low_ilvl_brick,
            avg_profit=round(
                sum(o.profit for o in filtered_opportunities)
                / len(filtered_opportunities),
                1,
            )
            if filtered_opportunities
            else 0.0,
            max_profit=max((o.profit for o in filtered_opportunities), default=0.0),
            avg_score=round(
                sum(o.score for o in filtered_opportunities)
                / len(filtered_opportunities),
                1,
            )
            if filtered_opportunities
            else 0.0,
            scan_profile=scan_profile,
            resolved_league=self.api_client.league,
            macro_queries=macro_queries_executed,
            micro_queries=micro_queries_executed,
            deduped_ttl=deduped_ttl,
            stage_a_candidates=len(candidate_query_map),
            stage_b_passed=len(stage_b_opportunities),
            budget_exhausted=budget_exhausted,
        )
        self._emit_scan_metric(
            run_id=run_id,
            started_at=started_at,
            status="error" if self._scan_error_count > 0 else "ok",
            stats=stats,
            max_items=max_items,
            item_class=item_class,
            safe_buy=safe_buy,
        )
        return filtered_opportunities, stats

    def run_scan(
        self,
        item_class: str = "",
        ilvl_min: int = 1,
        rarity: str = "rare",
        max_items: int = 30,
        min_profit: float = float("-inf"),
        min_listed_price: float = 0.0,
        anti_fix: bool = True,
        stale_hours: float = 48.0,
        safe_buy: bool = False,
    ) -> Tuple[List[Dict], ScanStats]:
        opportunities, stats = self.scan_opportunities(
            item_class=item_class,
            ilvl_min=ilvl_min,
            rarity=rarity,
            max_items=max_items,
            min_profit=min_profit,
            min_listed_price=min_listed_price,
            anti_fix=anti_fix,
            stale_hours=stale_hours,
            safe_buy=safe_buy,
        )
        return [opportunity.to_dict() for opportunity in opportunities], stats
