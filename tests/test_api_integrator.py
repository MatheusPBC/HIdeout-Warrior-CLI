import pytest
import sys
import os
import requests
from unittest.mock import patch, MagicMock
from urllib.parse import quote

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.api_integrator import MarketAPIClient


class TestLeagueCacheFilename:
    @patch("core.api_integrator.requests.Session")
    def test_league_cache_filename_standard(self, mock_session):
        with patch.object(
            MarketAPIClient, "_resolve_trade_league", return_value="Standard"
        ):
            client = MarketAPIClient(league="Standard", data_dir="/tmp/test_data")
            client._available_leagues = ["Standard"]
            filename = client._league_cache_filename("Standard")
            assert filename == "market_prices_standard.json"

    @patch("core.api_integrator.requests.Session")
    def test_league_cache_filename_with_spaces(self, mock_session):
        with patch.object(
            MarketAPIClient, "_resolve_trade_league", return_value="Hardcore"
        ):
            client = MarketAPIClient(league="Hardcore", data_dir="/tmp/test_data")
            client._available_leagues = ["Standard", "Hardcore"]
            filename = client._league_cache_filename("Hardcore Affliction")
            assert filename == "market_prices_hardcore_affliction.json"

    @patch("core.api_integrator.requests.Session")
    def test_league_cache_filename_special_chars(self, mock_session):
        with patch.object(
            MarketAPIClient, "_resolve_trade_league", return_value="Standard"
        ):
            client = MarketAPIClient(league="Standard", data_dir="/tmp/test_data")
            client._available_leagues = ["Standard"]
            filename = client._league_cache_filename("Mirage (SSF)")
            assert filename == "market_prices_mirage_ssf.json"


class TestResolveTradeLeague:
    @patch.object(MarketAPIClient, "_fetch_trade_leagues")
    def test_resolve_trade_league_exact(self, mock_fetch):
        mock_fetch.return_value = ["Standard", "Hardcore", "Mirage", "Affliction"]

        client = MarketAPIClient(league="Mirage", data_dir="/tmp/test_data")
        client._available_leagues = mock_fetch.return_value
        result = client._resolve_trade_league("Mirage")

        assert result == "Mirage"

    @patch.object(MarketAPIClient, "_fetch_trade_leagues")
    def test_resolve_trade_league_fallback(self, mock_fetch):
        mock_fetch.return_value = ["Standard", "Hardcore", "Mirage"]

        client = MarketAPIClient(league="NonExistentLeague", data_dir="/tmp/test_data")
        client._available_leagues = mock_fetch.return_value
        result = client._resolve_trade_league("NonExistentLeague")

        assert result == "Standard"

    @patch.object(MarketAPIClient, "_fetch_trade_leagues")
    def test_resolve_trade_league_empty_list(self, mock_fetch):
        mock_fetch.return_value = []

        client = MarketAPIClient(league="TestLeague", data_dir="/tmp/test_data")
        client._available_leagues = mock_fetch.return_value
        result = client._resolve_trade_league("TestLeague")

        assert result == "TestLeague"


class TestUrlEncodeLeague:
    def test_url_encode_league_basic(self):
        result = quote("Standard", safe="")
        assert result == "Standard"

    def test_url_encode_league_with_spaces(self):
        result = quote("Hardcore Affliction", safe="")
        assert result == "Hardcore%20Affliction"

    def test_url_encode_league_special_chars(self):
        result = quote("Mirage (SSF)", safe="")
        assert result == "Mirage%20%28SSF%29"


class TestFetchTradeLeagues:
    @patch("core.api_integrator.requests.Session")
    def test_fetch_trade_leagues_parses_result_payload(self, mock_session):
        with patch.object(
            MarketAPIClient, "_resolve_trade_league", return_value="Standard"
        ):
            client = MarketAPIClient(league="Standard", data_dir="/tmp/test_data")

        response = MagicMock()
        response.raise_for_status.return_value = None
        response.json.return_value = {
            "result": [{"id": "Standard"}, {"id": "Hardcore"}, {"id": "SSF Standard"}]
        }
        client.session.get = MagicMock(return_value=response)
        client._available_leagues = None

        leagues = client._fetch_trade_leagues()

        assert leagues == ["Standard", "Hardcore", "SSF Standard"]

    @patch("core.api_integrator.requests.Session")
    def test_fetch_trade_leagues_invalid_payload_returns_empty_list(self, mock_session):
        with patch.object(
            MarketAPIClient, "_resolve_trade_league", return_value="Standard"
        ):
            client = MarketAPIClient(league="Standard", data_dir="/tmp/test_data")

        response = MagicMock()
        response.raise_for_status.return_value = None
        response.json.return_value = "invalid-payload"
        client.session.get = MagicMock(return_value=response)
        client._available_leagues = None

        leagues = client._fetch_trade_leagues()

        assert leagues == []

    @patch("core.api_integrator.requests.Session")
    def test_init_survives_fetch_trade_leagues_request_error(self, mock_session):
        session = MagicMock()
        session.get.side_effect = requests.exceptions.RequestException("network error")
        mock_session.return_value = session

        client = MarketAPIClient(league="Mirage", data_dir="/tmp/test_data")

        assert client is not None
        assert client.league == "Mirage"


class TestFetchItemDetails:
    @patch("core.api_integrator.requests.Session")
    def test_fetch_item_details_url_without_league_param(self, mock_session):
        with patch.object(
            MarketAPIClient, "_resolve_trade_league", return_value="Standard"
        ):
            client = MarketAPIClient(league="Standard", data_dir="/tmp/test_data")

        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {"result": []}
        client.session.request = MagicMock(return_value=response)

        client.fetch_item_details(["id-1", "id-2"], "query-123")

        called_url = client.session.request.call_args.kwargs["url"]
        assert "?query=query-123" in called_url
        assert "&league=" not in called_url


class TestTradeRateLimitRetry:
    @patch("core.api_integrator.requests.Session")
    @patch.object(MarketAPIClient, "_resolve_trade_league", return_value="Standard")
    @patch("core.api_integrator.time.sleep", return_value=None)
    def test_search_respects_retry_after_on_429_then_succeeds(
        self,
        mock_sleep,
        mock_resolve,
        mock_session,
    ):
        client = MarketAPIClient(league="Standard", data_dir="/tmp/test_data")

        resp_429 = MagicMock()
        resp_429.status_code = 429
        resp_429.headers = {"Retry-After": "2"}

        resp_200 = MagicMock()
        resp_200.status_code = 200
        resp_200.headers = {}
        resp_200.json.return_value = {"id": "q1", "result": ["a", "b"]}

        client.session.request = MagicMock(side_effect=[resp_429, resp_200])

        query_id, ids = client.search_items({"query": {}})

        assert query_id == "q1"
        assert ids == ["a", "b"]
        sleep_args = [call.args[0] for call in mock_sleep.call_args_list]
        assert any(arg >= 2 for arg in sleep_args)

    @patch("core.api_integrator.requests.Session")
    @patch.object(MarketAPIClient, "_resolve_trade_league", return_value="Standard")
    @patch("core.api_integrator.time.sleep", return_value=None)
    def test_fetch_retries_on_5xx_and_recovers(
        self,
        mock_sleep,
        mock_resolve,
        mock_session,
    ):
        client = MarketAPIClient(league="Standard", data_dir="/tmp/test_data")

        resp_503 = MagicMock()
        resp_503.status_code = 503
        resp_503.headers = {}

        resp_200 = MagicMock()
        resp_200.status_code = 200
        resp_200.headers = {}
        resp_200.json.return_value = {"result": [{"id": "item-1"}]}

        client.session.request = MagicMock(side_effect=[resp_503, resp_200])

        result = client.fetch_item_details(["item-1"], "query-1")

        assert result == [{"id": "item-1"}]
        assert mock_sleep.called

    @patch("core.api_integrator.requests.Session")
    @patch.object(MarketAPIClient, "_resolve_trade_league", return_value="Standard")
    @patch("core.api_integrator.time.sleep", return_value=None)
    def test_fetch_applies_pre_request_throttle_from_rate_headers(
        self,
        mock_sleep,
        mock_resolve,
        mock_session,
    ):
        client = MarketAPIClient(league="Standard", data_dir="/tmp/test_data")

        resp_200 = MagicMock()
        resp_200.status_code = 200
        resp_200.headers = {
            "X-Rate-Limit-Ip-State": "9:10:6",
            "X-Rate-Limit-Ip": "10:10:6",
        }
        resp_200.json.return_value = {"result": []}

        client.session.request = MagicMock(return_value=resp_200)
        client.fetch_item_details(["a"], "q")
        client.fetch_item_details(["b"], "q")

        assert mock_sleep.called


class TestAutoResolveTradeLeague:
    @patch.object(MarketAPIClient, "_fetch_trade_leagues")
    def test_auto_prefers_non_standard_trade_league(self, mock_fetch):
        mock_fetch.return_value = ["Standard", "Necropolis", "Hardcore Necropolis"]

        client = MarketAPIClient(league="auto", data_dir="/tmp/test_data")
        client._available_leagues = mock_fetch.return_value
        result = client._resolve_trade_league("auto")

        assert result == "Necropolis"

    @patch.object(MarketAPIClient, "_fetch_trade_leagues")
    def test_auto_falls_back_to_standard_when_no_better_option(self, mock_fetch):
        mock_fetch.return_value = ["Standard", "Hardcore", "SSF Standard"]

        client = MarketAPIClient(league="auto", data_dir="/tmp/test_data")
        client._available_leagues = mock_fetch.return_value
        result = client._resolve_trade_league("auto")

        assert result == "Standard"
