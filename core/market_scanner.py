import logging
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
    avg_profit: float = 0.0
    max_profit: float = 0.0
    avg_score: float = 0.0
    scan_profile: str = "open_market"
    resolved_league: str = ""


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
        )


class OnDemandScanner:
    """Scanner de mercado sob demanda com valuation por família."""

    def __init__(self, league: str = "auto"):
        self.api_client = MarketAPIClient(league=league)
        self.oracle = PricePredictor()
        self.currency_rates = self.api_client.sync_ninja_economy()
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
            query["query"]["filters"]["misc_filters"]["filters"]["influence"] = {
                "option": "true"
            }

        return query

    def extract_price_chaos(self, listing_json: dict) -> Optional[float]:
        price_info = listing_json.get("price", {})
        currency = price_info.get("currency", "")

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
        search_link = f"https://www.pathofexile.com/trade/search/{league_encoded}/{query_id}"
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
        )

    def _build_opportunity(
        self,
        item_json: dict,
        query_id: str,
        stale_hours: float,
    ) -> Optional[tuple[ScanOpportunity, NormalizedMarketItem]]:
        listing = item_json.get("listing", {})
        item_data = item_json.get("item", {})
        listed_price = self.extract_price_chaos(listing)
        if listed_price is None or not item_data:
            return None

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
        base_score, trusted_profit, profit, relative_discount = self._compute_base_score(
            normalized_item,
            valuation,
            risk_flags,
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
            comparables = build_comparable_market_stats(normalized_item, comparable_prices)
            opportunity.market_floor = comparables.market_floor
            opportunity.market_median = comparables.market_median
            opportunity.market_spread = comparables.market_spread
            opportunity.comparables_count = comparables.comparables_count
            opportunity.pricing_position = comparables.pricing_position
            opportunity.score = self._apply_market_context(
                opportunity.score,
                normalized_item,
                ValuationResult(**opportunity.valuation_result),
                comparables,
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
        scan_profile = self._scan_profile(item_class)
        if max_items <= 0:
            return [], ScanStats(resolved_league=self.api_client.league, scan_profile=scan_profile)

        query = self.build_trade_query(
            item_class,
            ilvl_min,
            rarity,
            False,
            min_listed_price=max(min_listed_price, 1.0),
        )
        query_id, result_ids = self.api_client.search_items(query)
        if not query_id or not result_ids:
            return [], ScanStats(resolved_league=self.api_client.league, scan_profile=scan_profile)

        target_ids = result_ids[: min(max_items, len(result_ids))]
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

        for i in range(0, len(target_ids), 10):
            details = self.api_client.fetch_item_details(target_ids[i : i + 10], query_id)
            for item_json in details:
                listing = item_json.get("listing", {})
                if not listing.get("whisper"):
                    continue
                if self.extract_price_chaos(listing) is None:
                    skipped_invalid_currency += 1
                    continue

                built = self._build_opportunity(item_json, query_id, stale_hours)
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

        opportunities = self._enrich_market_context(raw_opportunities)
        filtered_opportunities: List[ScanOpportunity] = []
        for opportunity in opportunities:
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

        stats = ScanStats(
            total_found=len(result_ids),
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
            avg_profit=round(sum(o.profit for o in filtered_opportunities) / len(filtered_opportunities), 1)
            if filtered_opportunities
            else 0.0,
            max_profit=max((o.profit for o in filtered_opportunities), default=0.0),
            avg_score=round(sum(o.score for o in filtered_opportunities) / len(filtered_opportunities), 1)
            if filtered_opportunities
            else 0.0,
            scan_profile=scan_profile,
            resolved_league=self.api_client.league,
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
