from typer.testing import CliRunner

from scripts import discover_trade_bases


def test_discover_trade_bases_cli_runs_discovery(tmp_path, monkeypatch) -> None:
    captured = {}

    class _FakeClient:
        def __init__(self, league: str) -> None:
            self.league = league

    def _fake_discover(**kwargs):
        captured.update(kwargs)
        return {"searched": 1, "fetched": 2, "candidates": 12, "base_types": 4}

    monkeypatch.setattr(discover_trade_bases, "MarketAPIClient", _FakeClient)
    monkeypatch.setattr(discover_trade_bases, "discover_trade_base_types", _fake_discover)

    result = CliRunner().invoke(
        discover_trade_bases.app,
        [
            "--db-path",
            str(tmp_path / "firehose.db"),
            "--league",
            "Mirage",
            "--max-results",
            "20",
            "--max-fetches",
            "2",
        ],
    )

    assert result.exit_code == 0
    assert captured["league"] == "Mirage"
    assert captured["client"].league == "Mirage"
    assert captured["max_results"] == 20
    assert captured["max_fetches"] == 2
    assert "base_types=4" in result.output
