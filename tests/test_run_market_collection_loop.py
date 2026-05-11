from pathlib import Path

from typer.testing import CliRunner

from scripts import run_market_collection_loop


def test_run_cycle_executes_market_collection_steps(monkeypatch, tmp_path) -> None:
    calls = []

    monkeypatch.setattr(
        run_market_collection_loop.discover_trade_bases,
        "discover",
        lambda **kwargs: calls.append(("discover", kwargs)),
    )
    monkeypatch.setattr(
        run_market_collection_loop.trade_bucket_collector,
        "main",
        lambda **kwargs: calls.append(("collect", kwargs)),
    )
    monkeypatch.setattr(
        run_market_collection_loop.build_training_snapshot,
        "build_training_snapshot",
        lambda **kwargs: calls.append(("snapshot", kwargs)),
    )
    monkeypatch.setattr(
        run_market_collection_loop.build_market_intelligence,
        "build_market_intelligence",
        lambda **kwargs: calls.append(("intelligence", kwargs)),
    )

    config = run_market_collection_loop.CollectionConfig(
        db_path=tmp_path / "firehose.db",
        league="Mirage",
        output_dir=tmp_path / "training_snapshots",
        intelligence_output=tmp_path / "latest.json",
    )

    result = run_market_collection_loop.run_collection_cycle(config)

    assert result == {"discover": "ok", "collect": "ok", "snapshot": "ok", "intelligence": "ok"}
    assert [name for name, _ in calls] == ["discover", "collect", "snapshot", "intelligence"]
    assert calls[0][1]["league"] == "Mirage"
    assert calls[1][1]["dynamic_bases"] is True
    assert calls[2][1]["league"] == "Mirage"
    assert calls[3][1]["gold_path"] == tmp_path / "training_snapshots" / "gold"
    assert calls[3][1]["risk_profile"] == "balanced"


def test_run_cycle_continues_after_non_critical_step_error(monkeypatch, tmp_path) -> None:
    calls = []

    def _raise_discovery(**kwargs):
        _ = kwargs
        raise RuntimeError("rate limited")

    monkeypatch.setattr(run_market_collection_loop.discover_trade_bases, "discover", _raise_discovery)
    monkeypatch.setattr(
        run_market_collection_loop.trade_bucket_collector,
        "main",
        lambda **kwargs: calls.append(("collect", kwargs)),
    )
    monkeypatch.setattr(
        run_market_collection_loop.build_training_snapshot,
        "build_training_snapshot",
        lambda **kwargs: calls.append(("snapshot", kwargs)),
    )
    monkeypatch.setattr(
        run_market_collection_loop.build_market_intelligence,
        "build_market_intelligence",
        lambda **kwargs: calls.append(("intelligence", kwargs)),
    )

    config = run_market_collection_loop.CollectionConfig(
        db_path=tmp_path / "firehose.db",
        league="Mirage",
        output_dir=tmp_path / "training_snapshots",
        intelligence_output=tmp_path / "latest.json",
    )

    result = run_market_collection_loop.run_collection_cycle(config)

    assert result["discover"] == "error"
    assert result["collect"] == "ok"
    assert [name for name, _ in calls] == ["collect", "snapshot", "intelligence"]


def test_cli_once_runs_without_sleep(monkeypatch, tmp_path) -> None:
    captured = {}

    def _fake_cycle(config):
        captured["config"] = config
        return {"discover": "ok"}

    monkeypatch.setattr(run_market_collection_loop, "run_collection_cycle", _fake_cycle)
    monkeypatch.setattr(run_market_collection_loop.time, "sleep", lambda seconds: captured.setdefault("sleep", seconds))

    result = CliRunner().invoke(
        run_market_collection_loop.app,
        [
            "--db-path",
            str(tmp_path / "firehose.db"),
            "--league",
            "Mirage",
            "--once",
        ],
    )

    assert result.exit_code == 0
    assert captured["config"].league == "Mirage"
    assert isinstance(captured["config"].db_path, Path)
    assert "sleep" not in captured


def test_model_cycle_runs_train_and_simulation(monkeypatch, tmp_path) -> None:
    calls = []

    monkeypatch.setattr(
        run_market_collection_loop.train_oracle,
        "train_xgboost_oracle",
        lambda **kwargs: calls.append(("train", kwargs)),
    )
    monkeypatch.setattr(
        run_market_collection_loop.xgboost_market_simulation_report,
        "build_latest_simulation_report",
        lambda **kwargs: calls.append(("simulation", kwargs)),
    )

    config = run_market_collection_loop.CollectionConfig(
        db_path=tmp_path / "firehose.db",
        league="Mirage",
        output_dir=tmp_path / "training_snapshots",
        intelligence_output=tmp_path / "latest.json",
    )

    result = run_market_collection_loop.run_model_cycle(config)

    assert result == {"train": "ok", "simulation": "ok"}
    assert [name for name, _ in calls] == ["train", "simulation"]
    assert calls[0][1]["source"] == "parquet"
    assert calls[0][1]["league"] == "Mirage"
    assert calls[1][1]["league"] == "Mirage"


def test_model_cycle_error_does_not_raise(monkeypatch, tmp_path) -> None:
    def _raise_train(**kwargs):
        _ = kwargs
        raise RuntimeError("quality gate failed")

    monkeypatch.setattr(
        run_market_collection_loop.train_oracle,
        "train_xgboost_oracle",
        _raise_train,
    )
    monkeypatch.setattr(
        run_market_collection_loop.xgboost_market_simulation_report,
        "build_latest_simulation_report",
        lambda **kwargs: None,
    )

    config = run_market_collection_loop.CollectionConfig(
        db_path=tmp_path / "firehose.db",
        league="Mirage",
        output_dir=tmp_path / "training_snapshots",
        intelligence_output=tmp_path / "latest.json",
    )

    result = run_market_collection_loop.run_model_cycle(config)

    assert result == {"train": "error", "simulation": "ok"}


def test_cli_runs_model_cycle_every_four_cycles(monkeypatch, tmp_path) -> None:
    calls = []

    def _fake_collection(config):
        _ = config
        calls.append("collection")
        return {"collect": "ok"}

    def _fake_model(config):
        _ = config
        calls.append("model")
        return {"train": "ok", "simulation": "ok"}

    def _fake_sleep(seconds):
        _ = seconds
        if calls.count("collection") >= 4:
            raise KeyboardInterrupt

    monkeypatch.setattr(run_market_collection_loop, "run_collection_cycle", _fake_collection)
    monkeypatch.setattr(run_market_collection_loop, "run_model_cycle", _fake_model)
    monkeypatch.setattr(run_market_collection_loop.time, "sleep", _fake_sleep)

    result = CliRunner().invoke(
        run_market_collection_loop.app,
        [
            "--db-path",
            str(tmp_path / "firehose.db"),
            "--league",
            "Mirage",
            "--cycle-sleep-seconds",
            "1",
            "--train-every-cycles",
            "4",
        ],
    )

    assert result.exit_code != 0
    assert calls == ["collection", "collection", "collection", "collection", "model"]
