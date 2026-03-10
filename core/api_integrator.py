import json
import logging
import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import requests

logger = logging.getLogger(__name__)


class MarketAPIClient:
    """
    Cliente de mercado para Trade API da GGG e poe.ninja.
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
        sanitized = re.sub(r"[^a-z0-9]+", "_", league.lower()).strip("_")
        return f"market_prices_{sanitized}.json"

    def _fetch_trade_leagues(self) -> List[str]:
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
            return leagues
        except requests.exceptions.RequestException as exc:
            logger.warning("Falha ao buscar ligas da trade API: %s", exc)
            return []

    def _pick_auto_league(self, available: List[str]) -> str:
        if not available:
            return "Standard"

        preferred = [
            league
            for league in available
            if league.lower() != "standard"
            and "ssf" not in league.lower()
            and "hardcore" not in league.lower()
            and "ruthless" not in league.lower()
        ]
        if preferred:
            return preferred[0]

        if "Standard" in available:
            return "Standard"

        return available[0]

    def _resolve_trade_league(self, requested: str) -> str:
        available = self._fetch_trade_leagues()
        normalized = (requested or "").strip()

        if normalized.lower() in {"auto", "current", "current-league"}:
            resolved = self._pick_auto_league(available)
            logger.info("[auto] Liga resolvida automaticamente para '%s'.", resolved)
            return resolved

        if not available:
            logger.warning(
                "[%s] Nao foi possivel buscar ligas. Usando a liga informada.",
                requested,
            )
            return requested

        if requested in available:
            return requested

        logger.warning(
            "[%s] Liga nao encontrada. Fazendo fallback para 'Standard'.",
            requested,
        )
        return "Standard"

    def _is_cache_valid(self, filepath: str, max_age_hours: float = 4.0) -> bool:
        if not os.path.exists(filepath):
            return False
        file_mod_time = os.path.getmtime(filepath)
        age_hours = (time.time() - file_mod_time) / 3600.0
        return age_hours < max_age_hours

    def sync_ninja_economy(self, force_update: bool = False) -> Dict[str, float]:
        if not force_update and self._is_cache_valid(self.market_cache_file, 4.0):
            logger.info("[%s] Economia carregada do cache.", self.league)
            with open(self.market_cache_file, "r", encoding="utf-8") as handle:
                return json.load(handle)

        url = f"{self.ninja_base_url}/currencyoverview?league={self.league}&type=Currency"
        try:
            response = requests.get(
                url,
                headers={"User-Agent": self.headers["User-Agent"]},
                timeout=20,
            )
            response.raise_for_status()
            data = response.json()
            currency_rates = {
                line.get("currencyTypeName"): line.get("chaosEquivalent", 0.0)
                for line in data.get("lines", [])
                if line.get("currencyTypeName")
            }
            with open(self.market_cache_file, "w", encoding="utf-8") as handle:
                json.dump(currency_rates, handle, indent=2)
            return currency_rates
        except requests.exceptions.RequestException as exc:
            logger.error("[%s] Falha ao sincronizar economia: %s", self.league, exc)
            return {}

    def _circuit_breaker(self, response: requests.Response):
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After", 60)
            try:
                wait_time = int(retry_after)
            except ValueError:
                wait_time = 60
            logger.error("[%s] HTTP 429. Pausando por %ss.", self.league, wait_time)
            time.sleep(wait_time)
            return

        state_header = response.headers.get("X-Rate-Limit-Ip-State")
        limit_header = response.headers.get("X-Rate-Limit-Ip")
        if state_header and limit_header:
            for rule in state_header.split(","):
                parts = rule.split(":")
                if len(parts) < 3:
                    continue
                try:
                    current_hits = int(parts[0])
                    max_hits = int(parts[1])
                except (ValueError, TypeError):
                    continue
                if (max_hits - current_hits) <= 1:
                    logger.warning("[%s] Limite de requests proximo. Esfriando por 3s.", self.league)
                    time.sleep(3)

    def search_items(self, query_json: dict) -> Tuple[str, List[str]]:
        league_encoded = quote(self.league, safe="")
        url = f"{self.ggg_base_url}/search/{league_encoded}"
        try:
            response = self.session.post(url, json=query_json, timeout=15)
            self._circuit_breaker(response)
            if response.status_code == 200:
                data = response.json()
                return data.get("id", ""), data.get("result", [])
            logger.error("[%s] Search Error %s: %s", self.league, response.status_code, response.text)
            return "", []
        except requests.exceptions.RequestException as exc:
            logger.error("[%s] Erro na busca da trade API: %s", self.league, exc)
            return "", []

    def fetch_item_details(self, item_ids: List[str], query_id: str) -> List[Dict[str, Any]]:
        if not item_ids:
            return []

        if len(item_ids) > 10:
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
            logger.error("[%s] Fetch Error %s: %s", self.league, response.status_code, response.text)
            return []
        except requests.exceptions.RequestException as exc:
            logger.error("[%s] Erro no fetch da trade API: %s", self.league, exc)
            return []


if __name__ == "__main__":
    client = MarketAPIClient(league="Standard")
    prices = client.sync_ninja_economy()
    print(f"Moedas em cache: {len(prices)}")
