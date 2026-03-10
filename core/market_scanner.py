import logging
import asyncio
import time
from copy import deepcopy
from statistics import median
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import quote

from core.api_integrator import MarketAPIClient
from core.graph_engine import ItemState
from core.ml_oracle import PricePredictor

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
    avg_profit: float = 0.0
    max_profit: float = 0.0
    avg_score: float = 0.0
    resolved_league: str = ""
    coverage_by_bucket: Dict[str, int] = field(default_factory=dict)
    candidates_macro: int = 0
    candidates_micro: int = 0
    deduped: int = 0
    stage_a_passed: int = 0
    stage_b_passed: int = 0
    final_approval_rate: float = 0.0


@dataclass
class ScanOpportunity:
    item_id: str
    base_type: str
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
    risk_flags: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "ScanOpportunity":
        return cls(
            item_id=data.get("item_id", ""),
            base_type=data.get("base_type", ""),
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
            risk_flags=data.get("risk_flags", []),
        )


class _ScannerBase:
    """
    Scanner de mercado sob demanda.
    Busca itens na trade API, calcula valuation via ML e ranqueia oportunidades.
    """

    def __init__(self, league: str = "auto"):
        self.api_client = MarketAPIClient(league=league)
        self.oracle = PricePredictor()
        self.currency_rates = self.api_client.sync_ninja_economy()
        self._seen_item_ids: Dict[str, float] = {}
        self._dedupe_ttl_seconds = 15 * 60
        self._macro_price_buckets: List[Tuple[str, float, Optional[float]]] = [
            ("1-10", 1.0, 10.0),
            ("10-40", 10.0, 40.0),
            ("40-120", 40.0, 120.0),
            ("120-300", 120.0, 300.0),
            ("300+", 300.0, None),
        ]
        self._priority_base_types = [
            "Imbued Wand",
            "Opal Ring",
            "Amethyst Ring",
            "Hubris Circlet",
            "Sorcerer Boots",
            "Vaal Regalia",
        ]
        self._macro_segment_cursor = 0
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
        max_listed_price: Optional[float] = None,
        ilvl_max: Optional[int] = None,
        fractured_only: bool = False,
    ) -> dict:
        query_min_price = max(min_listed_price, 1.0)
        price_filter: Dict[str, float] = {"min": query_min_price}
        if max_listed_price is not None and max_listed_price > 0:
            price_filter["max"] = max_listed_price

        ilvl_filter: Dict[str, int] = {"min": ilvl_min}
        if ilvl_max is not None and ilvl_max >= ilvl_min:
            ilvl_filter["max"] = ilvl_max

        query: dict = {
            "query": {
                "status": {"option": "online"},
                "filters": {
                    "trade_filters": {"filters": {"price": price_filter}},
                    "type_filters": {"filters": {"rarity": {"option": rarity}}},
                    "misc_filters": {"filters": {"ilvl": ilvl_filter}},
                },
            },
            "sort": {"price": "asc"},
        }

        if item_class:
            query["query"]["type"] = item_class

        return query

    def _extract_item_id(self, item_json: dict, prefer_top_level: bool = True) -> str:
        if not isinstance(item_json, dict):
            return ""

        item_data = item_json.get("item", {}) or {}
        top_level_id = item_json.get("id")
        nested_id = item_data.get("id")

        candidate_ids = (top_level_id, nested_id)
        if not prefer_top_level:
            candidate_ids = (nested_id, top_level_id)

        for candidate_id in candidate_ids:
            if candidate_id:
                return str(candidate_id)

        return ""

    def _sanitize_query_flag_filters(self, query: dict) -> Optional[dict]:
        sanitized_query = deepcopy(query)
        misc_filters = (
            sanitized_query.get("query", {})
            .get("filters", {})
            .get("misc_filters", {})
            .get("filters", {})
        )

        removed = False
        for key in ("influence", "fractured"):
            if key in misc_filters:
                misc_filters.pop(key, None)
                removed = True

        return sanitized_query if removed else None

    def _macro_query_budget(self, max_items: int, total_segments: int) -> int:
        if total_segments <= 0:
            return 0

        base_budget = max(6, max_items // 2 if max_items > 0 else 6)
        capped_budget = min(24, base_budget)
        return max(1, min(total_segments, capped_budget))

    def parse_api_to_state(self, item_json: dict) -> Optional[ItemState]:
        item_data = item_json.get("item", {})
        if not item_data:
            return None

        base_type = item_data.get("baseType", "Unknown Base")
        ilvl = item_data.get("ilvl", 1)
        raw_mods = item_data.get("explicitMods", [])
        prefixes = set()
        suffixes = set()

        for i, mod in enumerate(raw_mods):
            if i % 2 == 0:
                prefixes.add(mod)
            else:
                suffixes.add(mod)

        is_fractured = bool(
            item_data.get("fractured", False) or item_data.get("influences", {})
        )

        return ItemState(
            base_type=base_type,
            ilvl=ilvl,
            prefixes=frozenset(prefixes),
            suffixes=frozenset(suffixes),
            is_fractured=is_fractured,
        )

    def extract_price_chaos(self, listing_json: dict) -> Optional[float]:
        price_info = listing_json.get("price", {})
        currency = str(price_info.get("currency") or "")

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

        ninja_key = ninja_key_map.get(currency, currency.title() + " Orb")
        if not ninja_key:
            return None
        rate = self.currency_rates.get(str(ninja_key))
        if rate is None:
            logger.warning(
                "Moeda não encontrada nas taxas: %s (%s)", currency, ninja_key
            )
            return None

        return amount * rate


class OnDemandScanner(_ScannerBase):
    """
    Compat layer para manter o contrato público existente.
    """

    def __init__(self, league: str = "auto"):
        super().__init__(league=league)

    def _listing_age_hours(self, indexed_at: Optional[str]) -> Optional[float]:
        if not indexed_at:
            return None
        try:
            indexed_dt = datetime.fromisoformat(indexed_at.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            return None
        return (datetime.now(timezone.utc) - indexed_dt).total_seconds() / 3600

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
        snapshot: ListingSnapshot,
        ml_value: float,
        ml_confidence: float,
        stale_hours: float,
    ) -> List[str]:
        flags: List[str] = []

        if self._is_probable_price_fix(
            snapshot.listed_price,
            ml_value,
            snapshot.indexed_at,
            stale_hours,
        ):
            flags.append("price_fix_suspected")

        age_hours = self._listing_age_hours(snapshot.indexed_at)
        if age_hours is not None and age_hours > stale_hours:
            flags.append("stale_listing")

        if ml_confidence < 0.5:
            flags.append("low_confidence")

        if snapshot.listed_price >= 80 and ml_confidence < 0.75:
            flags.append("high_ticket_low_confidence")

        if snapshot.listed_price < 5:
            flags.append("cheap_listing")

        if snapshot.corrupted:
            flags.append("corrupted")

        if snapshot.fractured:
            flags.append("fractured")

        if snapshot.influences:
            flags.append("influenced")

        return flags

    def _compute_opportunity_score(
        self,
        listed_price: float,
        ml_value: float,
        ml_confidence: float,
        risk_flags: List[str],
    ) -> float:
        if listed_price <= 0:
            return 0.0

        profit = ml_value - listed_price
        roi = max(profit / max(listed_price, 1.0), 0.0)
        relative_discount = max(profit / max(ml_value, 1.0), 0.0)

        score = 0.0
        score += min(max(profit, 0.0), 200.0) * 0.15
        score += min(roi, 3.0) * 28.0
        score += min(relative_discount, 0.8) * 35.0
        score += ml_confidence * 30.0

        penalties = {
            "price_fix_suspected": 45.0,
            "stale_listing": 8.0,
            "low_confidence": 10.0,
            "high_ticket_low_confidence": 18.0,
            "cheap_listing": 4.0,
            "corrupted": 6.0,
        }
        score -= sum(penalties.get(flag, 0.0) for flag in risk_flags)
        return round(max(score, 0.0), 1)

    def _build_listing_snapshot(
        self, item_json: dict, query_id: str
    ) -> Optional[ListingSnapshot]:
        listing = item_json.get("listing", {})
        item_data = item_json.get("item", {})
        whisper = listing.get("whisper", "")
        if not whisper:
            return None

        listed_price = self.extract_price_chaos(listing)
        if listed_price is None:
            return None

        price_info = listing.get("price", {})
        item_id = self._extract_item_id(item_json, prefer_top_level=False)
        league_encoded = quote(self.api_client.league, safe="")
        search_link = (
            f"https://www.pathofexile.com/trade/search/{league_encoded}/{query_id}"
        )
        trade_link = f"{search_link}#{item_id}" if item_id else search_link
        influences_dict = item_data.get("influences", {}) or {}

        return ListingSnapshot(
            item_id=item_id,
            base_type=item_data.get("baseType", "Unknown Base"),
            ilvl=item_data.get("ilvl", 1),
            listed_price=round(listed_price, 1),
            listing_currency=price_info.get("currency", "chaos"),
            listing_amount=float(price_info.get("amount", 0.0) or 0.0),
            seller=listing.get("account", {}).get("name", ""),
            indexed_at=listing.get("indexed") or None,
            whisper=whisper,
            trade_link=trade_link,
            trade_search_link=search_link,
            corrupted=bool(item_data.get("corrupted", False)),
            fractured=bool(item_data.get("fractured", False)),
            influences=list(influences_dict.keys()),
            explicit_mods=item_data.get("explicitMods", []),
            implicit_mods=item_data.get("implicitMods", []),
        )

    def _build_opportunity(
        self,
        item_json: dict,
        query_id: str,
        stale_hours: float,
    ) -> Optional[ScanOpportunity]:
        snapshot = self._build_listing_snapshot(item_json, query_id)
        if snapshot is None:
            return None

        state = self.parse_api_to_state(item_json)
        if not state:
            return None

        ml_value, ml_confidence = self.oracle.predict_value(state)
        valuation_gap = round(ml_value - snapshot.listed_price, 1)
        relative_discount = round(valuation_gap / max(snapshot.listed_price, 1.0), 2)
        risk_flags = self._risk_flags(snapshot, ml_value, ml_confidence, stale_hours)
        score = self._compute_opportunity_score(
            snapshot.listed_price,
            ml_value,
            ml_confidence,
            risk_flags,
        )

        return ScanOpportunity(
            item_id=snapshot.item_id,
            base_type=snapshot.base_type,
            ilvl=snapshot.ilvl,
            listed_price=snapshot.listed_price,
            ml_value=round(ml_value, 1),
            ml_confidence=round(ml_confidence, 2),
            profit=valuation_gap,
            score=score,
            valuation_gap=valuation_gap,
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
            risk_flags=risk_flags,
        )

    def _prune_seen_ids(self, now_ts: Optional[float] = None) -> None:
        now_ts = now_ts or time.time()
        expired_ids = [
            item_id
            for item_id, seen_at in self._seen_item_ids.items()
            if now_ts - seen_at > self._dedupe_ttl_seconds
        ]
        for item_id in expired_ids:
            self._seen_item_ids.pop(item_id, None)

    def _register_item_if_new(
        self, item_id: str, now_ts: Optional[float] = None
    ) -> bool:
        if not item_id:
            return True
        now_ts = now_ts or time.time()
        self._prune_seen_ids(now_ts)
        last_seen = self._seen_item_ids.get(item_id)
        if last_seen is not None and (now_ts - last_seen) <= self._dedupe_ttl_seconds:
            return False
        self._seen_item_ids[item_id] = now_ts
        return True

    def _safe_search_and_fetch(
        self, query: dict, max_items: int
    ) -> Tuple[str, List[dict], int]:
        query_attempts = [query]
        fallback_query = self._sanitize_query_flag_filters(query)
        if fallback_query is not None:
            query_attempts.append(fallback_query)

        query_id = ""
        result_ids: List[str] = []
        for index, attempt_query in enumerate(query_attempts):
            try:
                query_id, result_ids = self.api_client.search_items(attempt_query)
            except Exception as exc:
                logger.warning(
                    "[%s] search_items falhou: %s", self.api_client.league, exc
                )
                return "", [], 0

            if query_id and result_ids:
                break

            if index == 0 and len(query_attempts) > 1:
                logger.warning(
                    "[%s] Busca sem resultado com filtros de flag; retry sem influence/fractured",
                    self.api_client.league,
                )

        if not query_id or not result_ids:
            return "", [], 0

        target_ids = result_ids[: min(max_items, len(result_ids))]
        details: List[dict] = []
        for i in range(0, len(target_ids), 10):
            batch_ids = target_ids[i : i + 10]
            try:
                batch = self.api_client.fetch_item_details(batch_ids, query_id)
            except Exception as exc:
                logger.warning(
                    "[%s] fetch_item_details falhou (batch=%s): %s",
                    self.api_client.league,
                    len(batch_ids),
                    exc,
                )
                continue
            if batch:
                details.extend(batch)

        return query_id, details, len(result_ids)

    async def _run_macro_sweep(
        self,
        item_class: str,
        ilvl_min: int,
        rarity: str,
        max_items: int,
        min_listed_price: float,
    ) -> Dict[str, Any]:
        segments: List[Tuple[str, dict]] = []
        ilvl_segments = [(max(75, ilvl_min), 83), (84, None)]

        for bucket_name, price_min, price_max in self._macro_price_buckets:
            for base_type in self._priority_base_types:
                for ilvl_low, ilvl_high in ilvl_segments:
                    if ilvl_high is not None and ilvl_low > ilvl_high:
                        continue
                    for influenced, fractured in (
                        (False, False),
                        (True, False),
                        (False, True),
                    ):
                        query = self.build_trade_query(
                            item_class=item_class or base_type,
                            ilvl_min=ilvl_low,
                            rarity=rarity,
                            is_influenced=influenced,
                            min_listed_price=max(min_listed_price, price_min),
                            max_listed_price=price_max,
                            ilvl_max=ilvl_high,
                            fractured_only=fractured,
                        )
                        segment_key = (
                            f"{bucket_name}|{base_type}|{ilvl_low}+"
                            f"|inf={int(influenced)}|frac={int(fractured)}"
                        )
                        segments.append((segment_key, query))

        query_budget = self._macro_query_budget(
            max_items=max_items,
            total_segments=len(segments),
        )
        if query_budget <= 0:
            return {
                "candidates": [],
                "coverage": {},
                "query_ids": [],
                "total_found": 0,
            }

        start_index = self._macro_segment_cursor % len(segments)
        selected_segments = [
            segments[(start_index + offset) % len(segments)]
            for offset in range(query_budget)
        ]
        self._macro_segment_cursor = (start_index + query_budget) % len(segments)

        per_segment = max(4, min(12, max_items // 4 if max_items > 0 else 4))
        aggregated: List[dict] = []
        coverage: Dict[str, int] = {}
        query_ids: List[str] = []
        total_found = 0

        for segment_key, query in selected_segments:
            query_id, details, found_count = await asyncio.to_thread(
                self._safe_search_and_fetch, query, per_segment
            )
            total_found += found_count
            if query_id:
                query_ids.append(query_id)
            coverage_bucket = segment_key.split("|")[0]
            if details:
                aggregated.extend(details)
                coverage[coverage_bucket] = coverage.get(coverage_bucket, 0) + len(
                    details
                )

            if len(aggregated) >= max_items * 4:
                break

        return {
            "candidates": aggregated,
            "coverage": coverage,
            "query_ids": query_ids,
            "total_found": total_found,
        }

    async def _run_micro_snipe(
        self,
        rarity: str,
        max_items: int,
        min_listed_price: float,
    ) -> Dict[str, Any]:
        divine_rate = self.currency_rates.get("Divine Orb", 150.0)
        if divine_rate <= 0:
            divine_rate = 150.0
        cap_price = max(20.0, min(divine_rate, 220.0))

        micro_profiles = [
            {
                "base": "Imbued Wand",
                "ilvl_min": 84,
                "influenced": True,
                "fractured": False,
            },
            {
                "base": "Opal Ring",
                "ilvl_min": 84,
                "influenced": False,
                "fractured": True,
            },
            {
                "base": "Hubris Circlet",
                "ilvl_min": 84,
                "influenced": True,
                "fractured": False,
            },
        ]

        per_query = max(4, min(10, max_items // 2 if max_items > 0 else 4))
        aggregated: List[dict] = []
        query_ids: List[str] = []
        total_found = 0

        for profile in micro_profiles:
            query = self.build_trade_query(
                item_class=profile["base"],
                ilvl_min=profile["ilvl_min"],
                rarity=rarity,
                is_influenced=profile["influenced"],
                min_listed_price=max(min_listed_price, 1.0),
                max_listed_price=cap_price,
                fractured_only=profile["fractured"],
            )
            query_id, details, found_count = await asyncio.to_thread(
                self._safe_search_and_fetch, query, per_query
            )
            total_found += found_count
            if query_id:
                query_ids.append(query_id)
            if details:
                aggregated.extend(details)

        return {
            "candidates": aggregated,
            "query_ids": query_ids,
            "total_found": total_found,
        }

    def _infer_mod_signals(self, snapshot: ListingSnapshot) -> Set[str]:
        text = " ".join(snapshot.explicit_mods + snapshot.implicit_mods).lower()
        keywords = {
            "life": "life",
            "spell": "spell",
            "critical": "crit",
            "resistance": "res",
            "cast speed": "cast_speed",
            "attack speed": "attack_speed",
            "+1": "gem_levels",
        }
        signals: Set[str] = set()
        for token, signal in keywords.items():
            if token in text:
                signals.add(signal)
        return signals

    def _stage_a_heuristic_filter(
        self,
        snapshot: ListingSnapshot,
        min_listed_price: float,
    ) -> bool:
        if not snapshot.whisper:
            return False
        if snapshot.listed_price <= 0:
            return False
        if snapshot.listed_price < min_listed_price:
            return False
        if snapshot.listed_price > 100000:
            return False

        mod_text = " ".join(snapshot.explicit_mods + snapshot.implicit_mods).lower()
        relevant_tokens = ("life", "resist", "spell", "critical", "speed", "damage")
        return any(token in mod_text for token in relevant_tokens)

    def _comparable_fair_price(
        self,
        snapshot: ListingSnapshot,
        comparable_pool: List[ListingSnapshot],
    ) -> Optional[float]:
        if not comparable_pool:
            return None

        if snapshot.ilvl >= 84:
            ilvl_range = (84, 100)
        elif snapshot.ilvl >= 75:
            ilvl_range = (75, 83)
        else:
            ilvl_range = (1, 74)

        target_signals = self._infer_mod_signals(snapshot)
        prices: List[float] = []
        for candidate in comparable_pool:
            if candidate.item_id == snapshot.item_id:
                continue
            if candidate.base_type != snapshot.base_type:
                continue
            if not (ilvl_range[0] <= candidate.ilvl <= ilvl_range[1]):
                continue

            candidate_signals = self._infer_mod_signals(candidate)
            if (
                target_signals
                and candidate_signals
                and not (target_signals & candidate_signals)
            ):
                continue
            prices.append(candidate.listed_price)

        if len(prices) < 2:
            return None

        return round(float(median(prices)), 1)

    def _has_valuation_consensus(
        self,
        listed_price: float,
        ml_value: float,
        fair_price: Optional[float],
    ) -> bool:
        if listed_price <= 0:
            return False
        if fair_price is None:
            return ml_value >= listed_price * 1.2

        min_estimate = min(ml_value, fair_price)
        max_estimate = max(ml_value, fair_price)
        disagreement = (max_estimate - min_estimate) / max(max_estimate, 1.0)

        return min_estimate >= listed_price * 1.1 and disagreement <= 0.45

    def _passes_non_linear_ticket_rule(self, opportunity: ScanOpportunity) -> bool:
        listed = opportunity.listed_price
        profit = opportunity.profit

        if listed < 20:
            return profit >= 3
        if listed < 50:
            return profit >= 5
        if listed < 150:
            return profit >= 12
        return profit >= 30 and opportunity.ml_confidence >= 0.8

    def _stage_b_ml_evaluation(
        self,
        item_json: dict,
        query_id: str,
        stale_hours: float,
        comparable_pool: List[ListingSnapshot],
    ) -> Optional[ScanOpportunity]:
        opportunity = self._build_opportunity(item_json, query_id, stale_hours)
        if opportunity is None:
            return None

        snapshot = self._build_listing_snapshot(item_json, query_id)
        if snapshot is None:
            return None

        fair_price = self._comparable_fair_price(snapshot, comparable_pool)
        if not self._has_valuation_consensus(
            listed_price=opportunity.listed_price,
            ml_value=opportunity.ml_value,
            fair_price=fair_price,
        ):
            return None

        return opportunity

    def _execute_hybrid_ingestion(
        self,
        item_class: str,
        ilvl_min: int,
        rarity: str,
        max_items: int,
        min_listed_price: float,
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        async def _runner() -> Tuple[Dict[str, Any], Dict[str, Any]]:
            macro_task = self._run_macro_sweep(
                item_class=item_class,
                ilvl_min=ilvl_min,
                rarity=rarity,
                max_items=max_items,
                min_listed_price=min_listed_price,
            )
            micro_task = self._run_micro_snipe(
                rarity=rarity,
                max_items=max_items,
                min_listed_price=min_listed_price,
            )
            return await asyncio.gather(macro_task, micro_task)

        try:
            return asyncio.run(_runner())
        except RuntimeError:
            loop = asyncio.new_event_loop()
            try:
                return loop.run_until_complete(_runner())
            finally:
                loop.close()

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
        if max_items <= 0:
            return [], ScanStats(resolved_league=self.api_client.league)

        macro_result, micro_result = self._execute_hybrid_ingestion(
            item_class=item_class,
            ilvl_min=ilvl_min,
            rarity=rarity,
            max_items=max_items,
            min_listed_price=max(min_listed_price, 1.0),
        )

        macro_candidates = macro_result.get("candidates", [])
        micro_candidates = micro_result.get("candidates", [])
        aggregated_candidates = [*macro_candidates, *micro_candidates]
        if not aggregated_candidates:
            return [], ScanStats(
                resolved_league=self.api_client.league,
                coverage_by_bucket=macro_result.get("coverage", {}),
                candidates_macro=0,
                candidates_micro=0,
            )

        fallback_query_id = ""
        macro_query_ids = macro_result.get("query_ids", [])
        micro_query_ids = micro_result.get("query_ids", [])
        if macro_query_ids:
            fallback_query_id = macro_query_ids[0]
        elif micro_query_ids:
            fallback_query_id = micro_query_ids[0]

        now_ts = time.time()
        opportunities: List[ScanOpportunity] = []
        listing_pool: List[ListingSnapshot] = []
        stage_a_items: List[dict] = []
        deduped = 0
        total_evaluated = 0
        filtered_anti_fix = 0
        filtered_min_profit = 0
        filtered_min_listed_price = 0
        skipped_invalid_currency = 0
        filtered_safe_buy_confidence = 0
        filtered_safe_buy_age = 0
        filtered_safe_buy_price = 0
        stage_a_passed = 0
        stage_b_passed = 0

        unique_items: List[dict] = []
        for item_json in aggregated_candidates:
            item_id = self._extract_item_id(item_json)
            if not self._register_item_if_new(item_id=item_id, now_ts=now_ts):
                deduped += 1
                continue
            unique_items.append(item_json)

        target_items = unique_items[:max_items]
        query_id = fallback_query_id or "hybrid"

        for item_json in target_items:
            listing = item_json.get("listing", {})
            if not listing.get("whisper"):
                continue

            snapshot = self._build_listing_snapshot(item_json, query_id)
            if snapshot is None:
                skipped_invalid_currency += 1
                continue

            if not self._stage_a_heuristic_filter(snapshot, min_listed_price):
                if snapshot.listed_price < min_listed_price:
                    filtered_min_listed_price += 1
                continue

            stage_a_passed += 1
            listing_pool.append(snapshot)
            stage_a_items.append(item_json)

        for item_json in stage_a_items:
            opportunity = self._stage_b_ml_evaluation(
                item_json=item_json,
                query_id=query_id,
                stale_hours=stale_hours,
                comparable_pool=listing_pool,
            )
            if opportunity is None:
                continue

            stage_b_passed += 1
            total_evaluated += 1

            if anti_fix and "price_fix_suspected" in opportunity.risk_flags:
                filtered_anti_fix += 1
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

            if not self._passes_non_linear_ticket_rule(opportunity):
                filtered_min_profit += 1
                continue

            opportunities.append(opportunity)

        opportunities.sort(key=lambda opp: (opp.score, opp.profit), reverse=True)

        total_found = macro_result.get("total_found", 0) + micro_result.get(
            "total_found", 0
        )
        final_approval_rate = 0.0
        if stage_b_passed > 0:
            final_approval_rate = round(
                (len(opportunities) / stage_b_passed) * 100.0, 2
            )

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
            avg_profit=round(
                sum(o.profit for o in opportunities) / len(opportunities), 1
            )
            if opportunities
            else 0.0,
            max_profit=max((o.profit for o in opportunities), default=0.0),
            avg_score=round(sum(o.score for o in opportunities) / len(opportunities), 1)
            if opportunities
            else 0.0,
            resolved_league=self.api_client.league,
            coverage_by_bucket=macro_result.get("coverage", {}),
            candidates_macro=len(macro_candidates),
            candidates_micro=len(micro_candidates),
            deduped=deduped,
            stage_a_passed=stage_a_passed,
            stage_b_passed=stage_b_passed,
            final_approval_rate=final_approval_rate,
        )

        return opportunities, stats

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


class HybridMarketScanner(OnDemandScanner):
    """
    Alias explícito do scanner híbrido (macro + micro).
    Mantém compatibilidade total com o contrato legado de OnDemandScanner.
    """

    def __init__(self, league: str = "auto"):
        super().__init__(league=league)
