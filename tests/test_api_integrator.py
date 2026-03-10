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
    @patch("core.api_integrator.time.sleep", return_value=None)
    def test_fetch_item_details_url_without_league_param(
        self, mock_sleep, mock_session
    ):
        with patch.object(
            MarketAPIClient, "_resolve_trade_league", return_value="Standard"
        ):
            client = MarketAPIClient(league="Standard", data_dir="/tmp/test_data")

        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {"result": []}
        client.session.get = MagicMock(return_value=response)

        client.fetch_item_details(["id-1", "id-2"], "query-123")

        called_url = client.session.get.call_args[0][0]
        assert "?query=query-123" in called_url
        assert "&league=" not in called_url


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
