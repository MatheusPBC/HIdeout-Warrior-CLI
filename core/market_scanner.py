import logging
from dataclasses import dataclass, asdict, field
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timezone
from urllib.parse import quote

from core.api_integrator import MarketAPIClient
from core.graph_engine import ItemState
from core.ml_oracle import PricePredictor

logger = logging.getLogger(__name__)


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
    avg_profit: float = 0.0
    max_profit: float = 0.0


@dataclass
class ScanResult:
    base_type: str
    ilvl: int
    listed_price: float
    ml_value: float
    ml_confidence: float
    profit: float
    whisper: str
    trade_link: str
    trade_search_link: str
    item_id: str
    listing_currency: str
    listing_amount: float
    seller: str
    indexed_at: Optional[str]
    corrupted: bool
    fractured: bool
    influences: List[str]
    explicit_mods: List[str]
    implicit_mods: List[str]

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "ScanResult":
        return cls(
            base_type=data.get("base_type", ""),
            ilvl=data.get("ilvl", 0),
            listed_price=data.get("listed_price", 0.0),
            ml_value=data.get("ml_value", 0.0),
            ml_confidence=data.get("ml_confidence", 0.3),
            profit=data.get("profit", 0.0),
            whisper=data.get("whisper", ""),
            trade_link=data.get("trade_link", ""),
            trade_search_link=data.get("trade_search_link", ""),
            item_id=data.get("item_id", ""),
            listing_currency=data.get("listing_currency", "chaos"),
            listing_amount=data.get("listing_amount", 0.0),
            seller=data.get("seller", ""),
            indexed_at=data.get("indexed_at"),
            corrupted=data.get("corrupted", False),
            fractured=data.get("fractured", False),
            influences=data.get("influences", []),
            explicit_mods=data.get("explicit_mods", []),
            implicit_mods=data.get("implicit_mods", []),
        )


class OnDemandScanner:
    """
    Fase 7: Scanner de Mercado Sob Demanda.
    Permite busca ativa via filtros na GGG Trade API e avalia automaticamente o "EV"
    do item utilizando o Oráculo de Machine Learning.
    """

    def __init__(self, league: str = "Standard"):
        self.api_client = MarketAPIClient(league=league)
        self.oracle = PricePredictor()

        self.currency_rates = self.api_client.sync_ninja_economy()
        logger.info(
            f"[{league}] Scanner inicializado com {len(self.currency_rates)} taxas de moeda"
        )

    def build_trade_query(
        self,
        item_class: str = "",
        ilvl_min: int = 1,
        rarity: str = "rare",
        is_influenced: bool = False,
    ) -> dict:
        """
        Constrói o payload em tempo-real para o POST /api/trade/search
        """
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
        """
        Transforma o payload JSON do servidor no nosso Hashable 'ItemState' nativo do A* Graph.
        """
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

        is_fractured = (
            True
            if item_data.get("fractured", False) or item_data.get("influences", {})
            else False
        )

        return ItemState(
            base_type=base_type,
            ilvl=ilvl,
            prefixes=frozenset(prefixes),
            suffixes=frozenset(suffixes),
            is_fractured=is_fractured,
        )

    def extract_price_chaos(self, listing_json: dict) -> Optional[float]:
        """
        Converte o preço de listagem do Item da conta do utilizador para a unidade padrão Chaos Orb.
        """
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

        if ninja_key not in self.currency_rates:
            logger.warning(f"Moeda não encontrada nas taxas: {currency} ({ninja_key})")
            return None

        rate = self.currency_rates.get(ninja_key)
        if rate is None:
            return None

        return amount * rate

    def _is_probable_price_fix(
        self,
        listed_price_chaos: float,
        ml_value: float,
        indexed_at: str,
        stale_hours: float,
    ) -> bool:
        """
        Detecta prováveis tentativas de price-fixing.
        Retorna True se o item deve ser pulado.
        """
        try:
            indexed_dt = datetime.fromisoformat(indexed_at.replace("Z", "+00:00"))
            now = datetime.now(timezone.utc)
            age_hours = (now - indexed_dt).total_seconds() / 3600
        except (ValueError, TypeError, AttributeError):
            return False

        if age_hours > stale_hours and listed_price_chaos <= 2.0:
            if ml_value / listed_price_chaos >= 8.0:
                return True

        return False

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
        """
        Executa uma pesquisa on-demand. Avalia listagens com a IA, e retorna Arbitragens Livres (Profit > 0).
        Retorna: Tupla (results, stats) onde results é lista de dicionários e stats é ScanStats.
        Mantém compatibilidade com CLI retornando dicionários via to_dict().
        """
        if max_items <= 0:
            return [], ScanStats()

        logger.info(
            f"[{self.api_client.league}] Iniciando scan: item_class={item_class}, "
            f"max_items={max_items}, min_profit={min_profit}, anti_fix={anti_fix}"
        )

        query = self.build_trade_query(item_class, ilvl_min, rarity, False)

        query_id, result_ids = self.api_client.search_items(query)
        if not query_id or not result_ids:
            logger.warning(
                f"[{self.api_client.league}] Nenhum resultado encontrado para a query"
            )
            return [], ScanStats()

        league_encoded = quote(self.api_client.league, safe="")

        process_limit = min(max_items, len(result_ids))
        target_ids = result_ids[:process_limit]

        scan_results: List[ScanResult] = []
        batch_size = 10

        filtered_anti_fix = 0
        filtered_min_profit = 0
        skipped_invalid_currency = 0
        filtered_safe_buy_confidence = 0
        filtered_safe_buy_age = 0
        filtered_safe_buy_price = 0
        total_evaluated = 0

        for i in range(0, len(target_ids), batch_size):
            batch = target_ids[i : i + batch_size]
            details = self.api_client.fetch_item_details(batch, query_id)

            for item_json in details:
                listing = item_json.get("listing", {})
                whisper = listing.get("whisper", "")
                if not whisper:
                    continue

                listed_price_chaos = self.extract_price_chaos(listing)
                if listed_price_chaos is None:
                    skipped_invalid_currency += 1
                    continue

                assert listed_price_chaos is not None, (
                    "listed_price_chaos should not be None here"
                )

                price_info = listing.get("price", {})
                listing_currency = price_info.get("currency", "chaos")
                listing_amount = price_info.get("amount", 0.0)

                item_data = item_json.get("item", {})
                item_id = item_data.get("id", "")
                indexed_at = listing.get("indexed", "")

                state = self.parse_api_to_state(item_json)
                if not state:
                    continue

                ml_value, ml_confidence = self.oracle.predict_value(state)
                total_evaluated += 1

                if anti_fix:
                    if self._is_probable_price_fix(
                        listed_price_chaos, ml_value, indexed_at, stale_hours
                    ):
                        filtered_anti_fix += 1
                        continue

                if safe_buy:
                    if ml_confidence < 0.7:
                        filtered_safe_buy_confidence += 1
                        continue

                    try:
                        indexed_dt = datetime.fromisoformat(
                            indexed_at.replace("Z", "+00:00")
                        )
                        now = datetime.now(timezone.utc)
                        age_hours = (now - indexed_dt).total_seconds() / 3600
                        if age_hours > 24:
                            filtered_safe_buy_age += 1
                            continue
                    except (ValueError, TypeError, AttributeError):
                        pass

                    if listed_price_chaos < 5:
                        filtered_safe_buy_price += 1
                        continue

                profit = ml_value - listed_price_chaos

                if min_profit > float("-inf") and profit < min_profit:
                    filtered_min_profit += 1
                    continue

                explicit_mods = item_data.get("explicitMods", [])
                implicit_mods = item_data.get("implicitMods", [])
                corrupted = item_data.get("corrupted", False)
                fractured = item_data.get("fractured", False)
                influences_dict = item_data.get("influences", {})
                influences = list(influences_dict.keys()) if influences_dict else []

                search_link = f"https://www.pathofexile.com/trade/search/{league_encoded}/{query_id}"
                trade_link = f"{search_link}#{item_id}"

                scan_result = ScanResult(
                    base_type=state.base_type,
                    ilvl=state.ilvl,
                    listed_price=round(listed_price_chaos, 1),
                    ml_value=round(ml_value, 1),
                    ml_confidence=round(ml_confidence, 2),
                    profit=round(profit, 1),
                    whisper=whisper,
                    trade_link=trade_link,
                    trade_search_link=search_link,
                    item_id=item_id,
                    listing_currency=listing_currency,
                    listing_amount=listing_amount,
                    seller=listing.get("account", {}).get("name", ""),
                    indexed_at=indexed_at if indexed_at else None,
                    corrupted=corrupted,
                    fractured=fractured,
                    influences=influences,
                    explicit_mods=explicit_mods,
                    implicit_mods=implicit_mods,
                )
                scan_results.append(scan_result)

        scan_results.sort(key=lambda x: x.profit, reverse=True)

        stats = ScanStats(
            total_found=len(result_ids),
            total_evaluated=total_evaluated,
            filtered_anti_fix=filtered_anti_fix,
            filtered_min_profit=filtered_min_profit,
            skipped_invalid_currency=skipped_invalid_currency,
            filtered_safe_buy_confidence=filtered_safe_buy_confidence,
            filtered_safe_buy_age=filtered_safe_buy_age,
            filtered_safe_buy_price=filtered_safe_buy_price,
            avg_profit=round(sum(r.profit for r in scan_results) / len(scan_results), 1)
            if scan_results
            else 0.0,
            max_profit=max((r.profit for r in scan_results), default=0.0),
        )

        if filtered_anti_fix > 0:
            logger.info(
                f"[{self.api_client.league}] Itens filtrados por anti-fix: {filtered_anti_fix}"
            )

        if min_profit > float("-inf") and filtered_min_profit > 0:
            logger.info(
                f"[{self.api_client.league}] Itens filtrados por min-profit: {filtered_min_profit}"
            )

        if skipped_invalid_currency > 0:
            logger.warning(
                f"[{self.api_client.league}] Itens pulados por conversão de moeda inválida: {skipped_invalid_currency}"
            )

        logger.info(
            f"[{self.api_client.league}] Scan completado: {len(scan_results)} resultados encontrados"
        )

        return [result.to_dict() for result in scan_results], stats
