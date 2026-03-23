import subprocess
import sys
from pathlib import Path

import pytest

from scripts import ops_cycle


def test_ops_cycle_help_executes_as_direct_script() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [sys.executable, "scripts/ops_cycle.py", "--help"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )

    output = f"{result.stdout}\n{result.stderr}"
    assert result.returncode == 0, output
    assert "No module named 'core'" not in output


def test_ops_cycle_runs_in_sequence_and_emits_metric(monkeypatch, tmp_path) -> None:
    calls = []
    captured_metric = {}
    captured_miner_kwargs = {}

    def _fake_miner(**kwargs):
        calls.append("miner")
        captured_miner_kwargs.update(kwargs)

    def _fake_snapshot(**_kwargs):
        calls.append("snapshot")

    def _fake_train(**kwargs):
        calls.append("train")
        captured_metric["train_kwargs"] = kwargs

    def _fake_metric(**kwargs):
        captured_metric.update(kwargs)

    monkeypatch.setattr("scripts.ops_cycle.run_firehose_miner", _fake_miner)
    monkeypatch.setattr("scripts.ops_cycle.build_training_snapshot", _fake_snapshot)
    monkeypatch.setattr("scripts.ops_cycle.train_xgboost_oracle", _fake_train)
    monkeypatch.setattr("scripts.ops_cycle.append_metric_event", _fake_metric)

    ops_cycle.run(
        db_path=str(tmp_path / "firehose.db"),
        snapshot_output_dir=str(tmp_path / "snapshots"),
        train_source="parquet",
        oauth_token="token-abc",
    )

    assert calls == ["miner", "snapshot", "train"]
    assert captured_metric["component"] == "ops_cycle.daily_run"
    assert captured_metric["status"] == "ok"
    assert captured_metric["error_count"] == 0
    assert captured_metric["payload"]["effective_parquet_path"] == str(
        tmp_path / "snapshots" / "gold"
    )
    assert captured_miner_kwargs["oauth_token"] == "token-abc"
    assert captured_metric["train_kwargs"]["parquet_path"] == str(
        tmp_path / "snapshots" / "gold"
    )


def test_ops_cycle_forwards_oauth_client_credentials(monkeypatch, tmp_path) -> None:
    captured_miner_kwargs = {}

    def _fake_miner(**kwargs):
        captured_miner_kwargs.update(kwargs)

    monkeypatch.setattr("scripts.ops_cycle.run_firehose_miner", _fake_miner)
    monkeypatch.setattr(
        "scripts.ops_cycle.build_training_snapshot",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        "scripts.ops_cycle.train_xgboost_oracle",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        "scripts.ops_cycle.append_metric_event",
        lambda **_kwargs: None,
    )

    ops_cycle.run(
        db_path=str(tmp_path / "firehose.db"),
        snapshot_output_dir=str(tmp_path / "snapshots"),
        oauth_client_id="client-id",
        oauth_client_secret="client-secret",
        oauth_scope="service:psapi",
        oauth_token_url="https://www.pathofexile.com/oauth/token",
    )

    assert captured_miner_kwargs["oauth_client_id"] == "client-id"
    assert captured_miner_kwargs["oauth_client_secret"] == "client-secret"
    assert captured_miner_kwargs["oauth_scope"] == "service:psapi"


def test_ops_cycle_fail_fast_stops_on_first_error(monkeypatch, tmp_path) -> None:
    calls = []
    captured_metric = {}

    def _fake_miner(**_kwargs):
        calls.append("miner")

    def _fake_snapshot(**_kwargs):
        calls.append("snapshot")
        raise RuntimeError("snapshot failed")

    def _fake_train(**_kwargs):
        calls.append("train")

    def _fake_metric(**kwargs):
        captured_metric.update(kwargs)

    monkeypatch.setattr("scripts.ops_cycle.run_firehose_miner", _fake_miner)
    monkeypatch.setattr("scripts.ops_cycle.build_training_snapshot", _fake_snapshot)
    monkeypatch.setattr("scripts.ops_cycle.train_xgboost_oracle", _fake_train)
    monkeypatch.setattr("scripts.ops_cycle.append_metric_event", _fake_metric)

    with pytest.raises(RuntimeError, match="snapshot failed"):
        ops_cycle.run(
            db_path=str(tmp_path / "firehose.db"),
            snapshot_output_dir=str(tmp_path / "snapshots"),
            continue_on_error=False,
        )

    assert calls == ["miner", "snapshot"]
    assert captured_metric["status"] == "error"
    assert captured_metric["error_count"] == 1
    assert len(captured_metric["payload"]["steps"]) == 2


def test_ops_cycle_continue_on_error_runs_all_steps(monkeypatch, tmp_path) -> None:
    calls = []
    captured_metric = {}

    def _fake_miner(**_kwargs):
        calls.append("miner")

    def _fake_snapshot(**_kwargs):
        calls.append("snapshot")
        raise RuntimeError("snapshot failed")

    def _fake_train(**_kwargs):
        calls.append("train")

    def _fake_metric(**kwargs):
        captured_metric.update(kwargs)

    monkeypatch.setattr("scripts.ops_cycle.run_firehose_miner", _fake_miner)
    monkeypatch.setattr("scripts.ops_cycle.build_training_snapshot", _fake_snapshot)
    monkeypatch.setattr("scripts.ops_cycle.train_xgboost_oracle", _fake_train)
    monkeypatch.setattr("scripts.ops_cycle.append_metric_event", _fake_metric)

    ops_cycle.run(
        db_path=str(tmp_path / "firehose.db"),
        snapshot_output_dir=str(tmp_path / "snapshots"),
        continue_on_error=True,
    )

    assert calls == ["miner", "snapshot", "train"]
    assert captured_metric["status"] == "error"
    assert captured_metric["error_count"] == 1
    assert len(captured_metric["payload"]["steps"]) == 3
