import json
import logging
import os
import random
import re
import time
from email.utils import parsedate_to_datetime
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

        self._trade_max_retries = 5
        self._trade_base_backoff_seconds = 0.4
        self._trade_dynamic_delay_seconds = 0.0
        self._trade_next_allowed_at = 0.0
        self._trade_last_request_at = 0.0

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

        url = (
            f"{self.ninja_base_url}/currencyoverview?league={self.league}&type=Currency"
        )
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
                    logger.warning(
                        "[%s] Limite de requests proximo. Esfriando por 3s.",
                        self.league,
                    )
                    time.sleep(3)

    def _parse_retry_after_seconds(
        self, retry_after_value: Optional[str]
    ) -> Optional[float]:
        if not retry_after_value:
            return None

        value = retry_after_value.strip()
        if not value:
            return None

        try:
            return max(0.0, float(value))
        except ValueError:
            pass

        try:
            target_time = parsedate_to_datetime(value)
            now = time.time()
            return max(0.0, target_time.timestamp() - now)
        except (TypeError, ValueError, OverflowError):
            return None

    def _compute_header_throttle_seconds(self, response: requests.Response) -> float:
        state_header = response.headers.get("X-Rate-Limit-Ip-State", "")
        if not state_header:
            return 0.0

        best_delay = 0.0
        for rule in state_header.split(","):
            parts = [part.strip() for part in rule.split(":")]
            if len(parts) < 3:
                continue

            try:
                current_hits = int(parts[0])
                max_hits = int(parts[1])
                period_seconds = float(parts[2])
            except (ValueError, TypeError):
                continue

            if max_hits <= 0 or period_seconds <= 0:
                continue

            utilization = current_hits / max_hits
            per_request_delay = period_seconds / max_hits

            if utilization >= 0.95:
                best_delay = max(best_delay, max(1.0, per_request_delay * 2.5))
            elif utilization >= 0.85:
                best_delay = max(best_delay, max(0.2, per_request_delay * 1.4))
            elif utilization >= 0.70:
                best_delay = max(best_delay, max(0.05, per_request_delay * 0.8))

        return best_delay

    def _apply_pre_request_throttle(self) -> None:
        now = time.time()
        scheduled_at = max(self._trade_next_allowed_at, self._trade_last_request_at)
        delay_from_dynamic_window = self._trade_dynamic_delay_seconds
        wait_seconds = max(0.0, scheduled_at + delay_from_dynamic_window - now)
        if wait_seconds > 0:
            time.sleep(wait_seconds)

    def _update_rate_state_from_response(self, response: requests.Response) -> None:
        now = time.time()

        if response.status_code == 429:
            retry_after = self._parse_retry_after_seconds(
                response.headers.get("Retry-After")
            )
            if retry_after is None:
                retry_after = 2.0
            self._trade_next_allowed_at = max(
                self._trade_next_allowed_at, now + retry_after
            )
            self._trade_dynamic_delay_seconds = max(
                self._trade_dynamic_delay_seconds,
                min(3.0, retry_after / 2),
            )
            return

        header_delay = self._compute_header_throttle_seconds(response)
        if header_delay > 0:
            self._trade_dynamic_delay_seconds = max(
                self._trade_dynamic_delay_seconds * 0.85,
                header_delay,
            )
        else:
            self._trade_dynamic_delay_seconds *= 0.92

        self._trade_dynamic_delay_seconds = max(
            0.0, min(3.0, self._trade_dynamic_delay_seconds)
        )

    def _calculate_retry_delay(
        self,
        attempt_index: int,
        response: Optional[requests.Response],
    ) -> Optional[float]:
        if response is not None and response.status_code == 429:
            retry_after = self._parse_retry_after_seconds(
                response.headers.get("Retry-After")
            )
            if retry_after is not None:
                return retry_after

        exponential = self._trade_base_backoff_seconds * (2**attempt_index)
        jitter = random.uniform(0.05, 0.35)
        return min(8.0, exponential + jitter)

    def _trade_request(
        self, method: str, url: str, **kwargs: Any
    ) -> Optional[requests.Response]:
        retriable_statuses = {429, 500, 502, 503, 504}

        for attempt in range(self._trade_max_retries):
            self._apply_pre_request_throttle()
            self._trade_last_request_at = time.time()

            try:
                response = self.session.request(
                    method=method, url=url, timeout=15, **kwargs
                )
            except requests.exceptions.RequestException as exc:
                if attempt >= self._trade_max_retries - 1:
                    logger.error(
                        "[%s] Erro de request na trade API: %s", self.league, exc
                    )
                    return None

                retry_delay = self._calculate_retry_delay(attempt, response=None)
                if retry_delay is not None and retry_delay > 0:
                    time.sleep(retry_delay)
                continue

            self._update_rate_state_from_response(response)

            if response.status_code not in retriable_statuses:
                return response

            if attempt >= self._trade_max_retries - 1:
                return response

            retry_delay = self._calculate_retry_delay(attempt, response=response)
            if retry_delay is not None and retry_delay > 0:
                time.sleep(retry_delay)

        return None

    def search_items(self, query_json: dict) -> Tuple[str, List[str]]:
        league_encoded = quote(self.league, safe="")
        url = f"{self.ggg_base_url}/search/{league_encoded}"
        try:
            response = self._trade_request("POST", url, json=query_json)
            if response is None:
                return "", []
            if response.status_code == 200:
                data = response.json()
                return data.get("id", ""), data.get("result", [])
            logger.error(
                "[%s] Search Error %s: %s",
                self.league,
                response.status_code,
                response.text,
            )
            return "", []
        except requests.exceptions.RequestException as exc:
            logger.error("[%s] Erro na busca da trade API: %s", self.league, exc)
            return "", []

    def fetch_item_details(
        self, item_ids: List[str], query_id: str
    ) -> List[Dict[str, Any]]:
        if not item_ids:
            return []

        if len(item_ids) > 10:
            item_ids = item_ids[:10]

        ids_str = ",".join(item_ids)
        url = f"{self.ggg_base_url}/fetch/{ids_str}?query={query_id}"

        try:
            response = self._trade_request("GET", url)
            if response is None:
                return []
            if response.status_code == 200:
                data = response.json()
                return data.get("result", [])
            logger.error(
                "[%s] Fetch Error %s: %s",
                self.league,
                response.status_code,
                response.text,
            )
            return []
        except requests.exceptions.RequestException as exc:
            logger.error("[%s] Erro no fetch da trade API: %s", self.league, exc)
            return []


if __name__ == "__main__":
    client = MarketAPIClient(league="Standard")
    prices = client.sync_ninja_economy()
    print(f"Moedas em cache: {len(prices)}")
