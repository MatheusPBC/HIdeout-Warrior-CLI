import os
import json
import time
import re
import logging
import requests
from typing import List, Dict, Tuple, Any, Optional
from urllib.parse import quote

logger = logging.getLogger(__name__)


class MarketAPIClient:
    """
    Fase 1: Motor da Fundaçao do Data Layer.
    Integração dual: Web-Scraping Defensivo do poe.ninja (Sistema de Cache) e
    Cliente Seguro e Tolerante à Falhas da Trade API GGG (Circuit Breaker).
    """

    def __init__(
        self,
        league: str = "Mirage",
        user_agent: str = "HideoutWarrior_CLI/1.0 (Contact: me@example.com)",
        data_dir: str = "data",
    ):
        self.data_dir = data_dir

        self.ggg_base_url = "https://www.pathofexile.com/api/trade"
        self.ninja_base_url = "https://poe.ninja/api/data"

        self.headers = {
            "User-Agent": user_agent,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)

        os.makedirs(self.data_dir, exist_ok=True)

        self._available_leagues: Optional[List[str]] = None
        self.league = self._resolve_trade_league(league)
        self.market_cache_file = os.path.join(
            self.data_dir, self._league_cache_filename(self.league)
        )

    def _league_cache_filename(self, league: str) -> str:
        """Gera um nome de arquivo sanitizado baseado na liga."""
        sanitized = re.sub(r"[^a-z0-9]+", "_", league.lower()).strip("_")
        return f"market_prices_{sanitized}.json"

    def _fetch_trade_leagues(self) -> List[str]:
        """Busca as ligas disponíveis na API de trade da GGG."""
        if self._available_leagues is not None:
            return self._available_leagues

        url = f"{self.ggg_base_url}/data/leagues"
        try:
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            payload = response.json()

            if isinstance(payload, dict):
                leagues_payload = payload.get("result", [])
            elif isinstance(payload, list):
                leagues_payload = payload
            else:
                leagues_payload = []

            leagues: List[str] = []
            for entry in leagues_payload:
                if not isinstance(entry, dict):
                    continue
                league_id = entry.get("id")
                if league_id:
                    leagues.append(league_id)

            self._available_leagues = leagues
            return self._available_leagues or []
        except requests.exceptions.RequestException as e:
            league_name = getattr(self, "league", "unknown")
            logger.warning(f"[{league_name}] Falha ao buscar ligas: {e}")
            return []

    def _resolve_trade_league(self, requested: str) -> str:
        """Resolve a liga informada, com fallback para Standard se não existir."""
        available = self._fetch_trade_leagues()
        if not available:
            logger.warning(
                f"[{requested}] Não foi possível buscar ligas. Usando '{requested}' como informado."
            )
            return requested

        if requested in available:
            return requested

        logger.warning(
            f"[{requested}] Liga não encontrada. Fazendo fallback para 'Standard'."
        )
        return "Standard"

    # ----------------------------------------------------
    #  PoE Ninja API - Economy Sync & 4-Hour Caching
    # ----------------------------------------------------

    def _is_cache_valid(self, filepath: str, max_age_hours: float = 4.0) -> bool:
        """Determina se o cache local em disco expirou baseando-se no timestamp da Geração."""
        if not os.path.exists(filepath):
            return False

        file_mod_time = os.path.getmtime(filepath)
        age_hours = (time.time() - file_mod_time) / 3600.0
        return age_hours < max_age_hours

    def sync_ninja_economy(self, force_update: bool = False) -> Dict[str, float]:
        """
        Bate no portal da poe.ninja e traz o 'CurrencyOverview'.
        Sempre retornará uma dict com o Ratio {Currency: ChaosEquivalent}.
        Caches da requisição duram 4h para proteger o rate limit do site terceiro.
        """
        if not force_update and self._is_cache_valid(
            self.market_cache_file, max_age_hours=4.0
        ):
            logger.info(
                f"[{self.league}] Economia do cache carregada: Menos de 4h desde o último log."
            )
            with open(self.market_cache_file, "r", encoding="utf-8") as f:
                return json.load(f)

        logger.info(f"[{self.league}] Sincronização viva de Economy com Poe.Ninja...")
        url = (
            f"{self.ninja_base_url}/currencyoverview?league={self.league}&type=Currency"
        )

        try:
            response = requests.get(
                url, headers={"User-Agent": self.headers["User-Agent"]}, timeout=20
            )
            response.raise_for_status()
            data = response.json()

            lines = data.get("lines", [])
            currency_rates = {}
            for line in lines:
                currency_name = line.get("currencyTypeName")
                rate = line.get("chaosEquivalent", 0.0)
                if currency_name:
                    currency_rates[currency_name] = rate

            with open(self.market_cache_file, "w", encoding="utf-8") as f:
                json.dump(currency_rates, f, indent=2)

            logger.info(
                f"[{self.league}] Economia sincronizada com {len(currency_rates)} moedas"
            )
            return currency_rates
        except requests.exceptions.RequestException as e:
            logger.error(
                f"[{self.league}] Falha na rede ao conectar no Economy Scraper: {e}"
            )
            return {}

    # ----------------------------------------------------
    #  GGG Official Trade API - The Safe Engine (TOS)
    # ----------------------------------------------------

    def _circuit_breaker(self, response: requests.Response):
        """
        O 'Freio de Mão' de rede absoluto da Aplicação.
        Lê headers proprietários da GGG 'X-Rate-Limit-Ip-State' e 'Retry-After'.
        Pause a 'Thread' usando time.sleep() preemptivamente ou agressivamente (HTTP 429).
        """
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After", 60)
            try:
                wait_time = int(retry_after)
            except ValueError:
                wait_time = 60
            logger.error(
                f"[{self.league}] HTTP 429! Too Many Requests. Pausando por {wait_time}s."
            )
            time.sleep(wait_time)
            return

        state_header = response.headers.get("X-Rate-Limit-Ip-State")
        limit_header = response.headers.get("X-Rate-Limit-Ip")

        if state_header and limit_header:
            rules_state = state_header.split(",")
            for rule in rules_state:
                parts = rule.split(":")
                if len(parts) >= 3:
                    try:
                        current_hits = int(parts[0])
                        max_hits = int(parts[1])
                    except (ValueError, TypeError):
                        continue

                    if (max_hits - current_hits) <= 1:
                        logger.warning(
                            f"[{self.league}] Limite ({max_hits}) arriscadamente próximo ({current_hits}). Esfriando por 3s."
                        )
                        time.sleep(3)

    def search_items(self, query_json: dict) -> Tuple[str, List[str]]:
        """
        Emite um Market Scan profundo (POST) com base na classe/Pydantic `query_json`.
        """
        league_encoded = quote(self.league, safe="")
        url = f"{self.ggg_base_url}/search/{league_encoded}"
        try:
            response = self.session.post(url, json=query_json, timeout=15)
            self._circuit_breaker(response)

            if response.status_code == 200:
                data = response.json()
                query_id = data.get("id", "")
                result_count = len(data.get("result", []))
                logger.info(
                    f"[{self.league}] Search completed with {result_count} items"
                )
                return query_id, data.get("result", [])
            else:
                logger.error(
                    f"[{self.league}] Search Error {response.status_code}: {response.text}"
                )
                return "", []
        except requests.exceptions.RequestException as e:
            logger.error(f"[{self.league}] Sub-Error Fatal de Rede nas Searches: {e}")
            return "", []

    def fetch_item_details(
        self, item_ids: List[str], query_id: str
    ) -> List[Dict[str, Any]]:
        """
        Baixa os detalhes do MetaData Trade (Preço, Conta do Usuário) em blocos.
        Envia um GET para {id1,id2,id3}?query={query_id}.
        Hard Limit de apenas 10 instâncias por chamada para não quebrar o endpoint.
        """
        if not item_ids:
            return []

        if len(item_ids) > 10:
            logger.info(
                f"[{self.league}] A API suporta apenas blocos de 10. Processando 10 primários..."
            )
            item_ids = item_ids[:10]

        ids_str = ",".join(item_ids)
        url = f"{self.ggg_base_url}/fetch/{ids_str}?query={query_id}"

        time.sleep(0.5)

        try:
            response = self.session.get(url, timeout=15)
            self._circuit_breaker(response)

            if response.status_code == 200:
                data = response.json()
                return data.get("result", [])
            else:
                logger.error(
                    f"[{self.league}] Fetch Error {response.status_code}: {response.text}"
                )
                return []
        except requests.exceptions.RequestException as e:
            logger.error(f"[{self.league}] Fetch FATAL Error de Conexão: {e}")
            return []


if __name__ == "__main__":
    print("--- Teste de Stress/Cache da Market API ---")
    client = MarketAPIClient(league="Standard")

    prices = client.sync_ninja_economy()
    print(
        f"Total Currências em Cache (ChaosRatio): {len(prices)}. Divine: {prices.get('Divine Orb')}"
    )

    test_query = {
        "query": {
            "status": {"option": "online"},
            "type": "Simple Robe",
            "name": "Tabula Rasa",
        },
        "sort": {"price": "asc"},
    }

    q_id, items = client.search_items(test_query)
    if q_id and items:
        res = client.fetch_item_details(items[:2], q_id)
        print(f"Forças puxadas via GET: {len(res)} itens lidos.")
