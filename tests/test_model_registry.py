import json
from pathlib import Path

from scripts.model_registry import (
    load_registry,
    promote_if_better,
    register_candidate,
)


def test_register_candidate_creates_registry_family_index(tmp_path: Path) -> None:
    registry_path = tmp_path / "data" / "model_registry" / "registry.json"

    version = register_candidate(
        family="wand_caster",
        run_id="run-001",
        model_path="data/price_oracle_wand_caster.xgb",
        model_sha256="abc123",
        metrics={"rmse": 10.0, "baseline_rmse": 12.0},
        registry_path=registry_path,
    )

    payload = json.loads(registry_path.read_text(encoding="utf-8"))
    assert version["status"] == "candidate"
    assert payload["families"]["wand_caster"]["active_version"] is None
    assert len(payload["families"]["wand_caster"]["versions"]) == 1


def test_promote_if_better_sets_active_version(tmp_path: Path) -> None:
    registry_path = tmp_path / "registry.json"
    register_candidate(
        family="generic",
        run_id="run-002",
        model_path="data/price_oracle_generic.xgb",
        model_sha256="sha-1",
        metrics={"rmse": 8.0, "baseline_rmse": 10.0},
        registry_path=registry_path,
    )

    decision = promote_if_better(
        family="generic", run_id="run-002", registry_path=registry_path
    )
    registry = load_registry(registry_path)
    family_entry = registry["families"]["generic"]

    assert decision["promoted"] is True
    assert decision["decision_reason"] == "promotion_policy_satisfied"
    assert decision["policy"]["max_rmse_ratio"] == 1.0
    assert decision["policy"]["min_abs_improvement"] == 0.0
    assert family_entry["active_version"] == "run-002"
    assert family_entry["versions"][0]["status"] == "active"
    assert (
        family_entry["versions"][0]["decision_reason"] == "promotion_policy_satisfied"
    )


def test_promote_if_better_rejects_when_rmse_not_lower(tmp_path: Path) -> None:
    registry_path = tmp_path / "registry.json"
    register_candidate(
        family="generic",
        run_id="run-003",
        model_path="data/price_oracle_generic.xgb",
        model_sha256="sha-2",
        metrics={"rmse": 12.0, "baseline_rmse": 10.0},
        registry_path=registry_path,
    )

    decision = promote_if_better(
        family="generic", run_id="run-003", registry_path=registry_path
    )
    registry = load_registry(registry_path)

    assert decision["promoted"] is False
    assert decision["decision_reason"] == "rmse_above_ratio_threshold"
    assert registry["families"]["generic"]["active_version"] is None
    assert registry["families"]["generic"]["versions"][0]["status"] == "rejected"


def test_promote_if_better_rejects_when_improvement_below_threshold(
    tmp_path: Path,
) -> None:
    registry_path = tmp_path / "registry.json"
    register_candidate(
        family="generic",
        run_id="run-004",
        model_path="data/price_oracle_generic.xgb",
        model_sha256="sha-4",
        metrics={"rmse": 9.5, "baseline_rmse": 10.0},
        registry_path=registry_path,
    )

    decision = promote_if_better(
        family="generic",
        run_id="run-004",
        max_rmse_ratio=1.0,
        min_abs_improvement=1.0,
        registry_path=registry_path,
    )
    registry = load_registry(registry_path)

    assert decision["promoted"] is False
    assert decision["decision_reason"] == "abs_improvement_below_threshold"
    assert registry["families"]["generic"]["versions"][0]["decision_reason"] == (
        "abs_improvement_below_threshold"
    )


def test_register_candidate_triggers_cloud_registry_sync(
    tmp_path: Path,
    monkeypatch,
) -> None:
    registry_path = tmp_path / "registry.json"
    captured = {"file_sync": 0, "state_sync": 0}

    def _fake_file_sync(*args, **kwargs):
        captured["file_sync"] += 1
        return None

    def _fake_state_sync(*args, **kwargs):
        captured["state_sync"] += 1
        return 1

    monkeypatch.setattr("scripts.model_registry.sync_file_to_supabase", _fake_file_sync)
    monkeypatch.setattr(
        "scripts.model_registry.sync_registry_state_to_supabase",
        _fake_state_sync,
    )

    register_candidate(
        family="generic",
        run_id="run-cloud-001",
        model_path="data/price_oracle_generic.xgb",
        model_sha256="sha-cloud",
        metrics={"rmse": 1.0, "baseline_rmse": 2.0},
        registry_path=registry_path,
    )

    assert captured["file_sync"] >= 1
    assert captured["state_sync"] >= 1
