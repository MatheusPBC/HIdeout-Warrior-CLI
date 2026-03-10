import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
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
    trusted_profit: float = 0.0
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
            trusted_profit=data.get("trusted_profit", 0.0),
            risk_flags=data.get("risk_flags", []),
        )


class OnDemandScanner:
    """
    Scanner de mercado sob demanda.
    Busca itens na trade API, calcula valuation via ML e ranqueia oportunidades.
    """

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
    ) -> dict:
        query: dict = {
            "query": {
                "status": {"option": "online"},
                "filters": {
                    "trade_filters": {"filters": {"price": {"min": 1}}},
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
            logger.warning("Moeda nao encontrada nas taxas: %s (%s)", currency, ninja_key)
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
    ) -> Tuple[float, float]:
        if listed_price <= 0:
            return (0.0, 0.0)

        profit = ml_value - listed_price
        relative_discount = max(profit / max(listed_price, 1.0), 0.0)
        trusted_profit = max(profit, 0.0) * ml_confidence

        score = 0.0
        score += trusted_profit * 1.05
        score += min(max(profit, 0.0), 120.0) * 0.18
        score += min(relative_discount, 3.0) * 5.5
        score += ml_confidence * 28.0

        penalties = {
            "price_fix_suspected": 45.0,
            "stale_listing": 14.0,
            "low_confidence": 18.0,
            "cheap_listing": 12.0,
            "corrupted": 10.0,
        }
        score -= sum(penalties.get(flag, 0.0) for flag in risk_flags)
        if "cheap_listing" in risk_flags and 0.45 <= ml_confidence < 0.60:
            score -= 12.0
        return (round(max(score, 0.0), 1), round(trusted_profit, 1))

    def _scan_profile(self, item_class: str) -> str:
        return "targeted" if item_class.strip() else "open_market"

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

    def _build_listing_snapshot(self, item_json: dict, query_id: str) -> Optional[ListingSnapshot]:
        listing = item_json.get("listing", {})
        item_data = item_json.get("item", {})
        whisper = listing.get("whisper", "")
        if not whisper:
            return None

        listed_price = self.extract_price_chaos(listing)
        if listed_price is None:
            return None

        price_info = listing.get("price", {})
        item_id = item_data.get("id", "")
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
        score, trusted_profit = self._compute_opportunity_score(
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
            trusted_profit=trusted_profit,
            risk_flags=risk_flags,
        )

    def scan_opportunities(
        self,
        item_class: str = "",
        ilvl_min: int = 1,
        rarity: str = "rare",
        max_items: int = 30,
        min_profit: float = float("-inf"),
        anti_fix: bool = True,
        stale_hours: float = 48.0,
        safe_buy: bool = False,
    ) -> Tuple[List[ScanOpportunity], ScanStats]:
        scan_profile = self._scan_profile(item_class)
        if max_items <= 0:
            return [], ScanStats(
                resolved_league=self.api_client.league,
                scan_profile=scan_profile,
            )

        query = self.build_trade_query(item_class, ilvl_min, rarity, False)
        query_id, result_ids = self.api_client.search_items(query)
        if not query_id or not result_ids:
            return [], ScanStats(
                resolved_league=self.api_client.league,
                scan_profile=scan_profile,
            )

        target_ids = result_ids[: min(max_items, len(result_ids))]
        opportunities: List[ScanOpportunity] = []
        total_evaluated = 0
        filtered_anti_fix = 0
        filtered_min_profit = 0
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

                opportunity = self._build_opportunity(item_json, query_id, stale_hours)
                if opportunity is None:
                    continue

                total_evaluated += 1

                if anti_fix and "price_fix_suspected" in opportunity.risk_flags:
                    filtered_anti_fix += 1
                    continue

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
                    if opportunity.ml_confidence < 0.7:
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

                opportunities.append(opportunity)

        opportunities.sort(
            key=lambda opp: (opp.score, opp.trusted_profit, opp.profit),
            reverse=True,
        )

        stats = ScanStats(
            total_found=len(result_ids),
            total_evaluated=total_evaluated,
            filtered_anti_fix=filtered_anti_fix,
            filtered_min_profit=filtered_min_profit,
            skipped_invalid_currency=skipped_invalid_currency,
            filtered_safe_buy_confidence=filtered_safe_buy_confidence,
            filtered_safe_buy_age=filtered_safe_buy_age,
            filtered_safe_buy_price=filtered_safe_buy_price,
            filtered_open_confidence=filtered_open_confidence,
            filtered_open_cheap_low_confidence=filtered_open_cheap_low_confidence,
            filtered_open_cheap_low_profit=filtered_open_cheap_low_profit,
            filtered_open_cheap_stale=filtered_open_cheap_stale,
            avg_profit=round(sum(o.profit for o in opportunities) / len(opportunities), 1)
            if opportunities
            else 0.0,
            max_profit=max((o.profit for o in opportunities), default=0.0),
            avg_score=round(sum(o.score for o in opportunities) / len(opportunities), 1)
            if opportunities
            else 0.0,
            scan_profile=scan_profile,
            resolved_league=self.api_client.league,
        )

        return opportunities, stats

    def run_scan(
        self,
        item_class: str = "",
        ilvl_min: int = 1,
        rarity: str = "rare",
        max_items: int = 30,
        min_profit: float = float("-inf"),
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
            anti_fix=anti_fix,
            stale_hours=stale_hours,
            safe_buy=safe_buy,
        )
        return [opportunity.to_dict() for opportunity in opportunities], stats
