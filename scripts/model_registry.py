import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, cast


REGISTRY_PATH = Path("data/model_registry/registry.json")


def _now_utc_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _default_registry() -> Dict[str, Any]:
    return {"families": {}}


def load_registry(registry_path: Path = REGISTRY_PATH) -> Dict[str, Any]:
    if not registry_path.exists():
        return _default_registry()
    try:
        payload = json.loads(registry_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return _default_registry()
    if not isinstance(payload, dict):
        return _default_registry()
    families = payload.get("families")
    if not isinstance(families, dict):
        return _default_registry()
    return payload


def save_registry(
    registry: Dict[str, Any], registry_path: Path = REGISTRY_PATH
) -> Path:
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    registry_path.write_text(
        json.dumps(registry, ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return registry_path


def register_candidate(
    *,
    family: str,
    run_id: str,
    model_path: str,
    model_sha256: str,
    metrics: Dict[str, Any],
    registry_path: Path = REGISTRY_PATH,
) -> Dict[str, Any]:
    registry = load_registry(registry_path)
    family_entry = registry["families"].setdefault(
        family,
        {"active_version": None, "versions": []},
    )
    versions = family_entry.setdefault("versions", [])
    if not isinstance(versions, list):
        versions = []
        family_entry["versions"] = versions

    created_at = _now_utc_iso()
    version = {
        "run_id": run_id,
        "model_path": model_path,
        "model_sha256": model_sha256,
        "metrics": metrics,
        "status": "candidate",
        "created_at": created_at,
    }
    versions.append(version)
    save_registry(registry, registry_path)
    return version


def _find_version(versions: List[Dict[str, Any]], run_id: str) -> Dict[str, Any] | None:
    for version in versions:
        if str(version.get("run_id", "")) == run_id:
            return version
    return None


def promote_if_better(
    *,
    family: str,
    run_id: str,
    max_rmse_ratio: float = 1.0,
    min_abs_improvement: float = 0.0,
    registry_path: Path = REGISTRY_PATH,
) -> Dict[str, Any]:
    registry = load_registry(registry_path)
    family_entry = registry["families"].setdefault(
        family,
        {"active_version": None, "versions": []},
    )
    versions = family_entry.setdefault("versions", [])
    if not isinstance(versions, list):
        versions = []
        family_entry["versions"] = versions

    target = _find_version(versions, run_id)
    policy = {
        "max_rmse_ratio": float(max_rmse_ratio),
        "min_abs_improvement": float(min_abs_improvement),
    }
    if target is None:
        return {
            "family": family,
            "run_id": run_id,
            "status": "not_found",
            "promoted": False,
            "reason": "candidate_not_found",
            "decision_reason": "candidate_not_found",
            "policy": policy,
        }
    assert target is not None

    metrics_raw = target.get("metrics")
    metrics = cast(Dict[str, Any], metrics_raw if isinstance(metrics_raw, dict) else {})
    rmse = metrics.get("rmse")
    baseline_rmse = metrics.get("baseline_rmse")
    rmse_value: float | None = None
    baseline_value: float | None = None
    abs_improvement: float | None = None
    rmse_threshold: float | None = None
    ratio_ok = False
    improvement_ok = False
    decision_reason = "invalid_metrics"
    should_promote = False

    if isinstance(rmse, (int, float)) and isinstance(baseline_rmse, (int, float)):
        rmse_value = float(rmse)
        baseline_value = float(baseline_rmse)
        rmse_threshold = baseline_value * float(max_rmse_ratio)
        abs_improvement = baseline_value - rmse_value
        ratio_ok = rmse_value <= rmse_threshold
        improvement_ok = abs_improvement >= float(min_abs_improvement)
        should_promote = ratio_ok and improvement_ok
        if should_promote:
            decision_reason = "promotion_policy_satisfied"
        elif not ratio_ok:
            decision_reason = "rmse_above_ratio_threshold"
        else:
            decision_reason = "abs_improvement_below_threshold"

    if should_promote:
        assert rmse_value is not None and baseline_value is not None
        for version in versions:
            if version is target:
                version["status"] = "active"
            elif str(version.get("status", "")) == "active":
                version["status"] = "archived"
        family_entry["active_version"] = run_id
        decision = {
            "family": family,
            "run_id": run_id,
            "status": "active",
            "promoted": True,
            "reason": decision_reason,
            "decision_reason": decision_reason,
            "policy": policy,
            "rmse": rmse_value,
            "baseline_rmse": baseline_value,
            "abs_improvement": abs_improvement,
            "rmse_threshold": rmse_threshold,
        }
    else:
        rmse_rejected = float(rmse) if isinstance(rmse, (int, float)) else None
        baseline_rejected = (
            float(baseline_rmse) if isinstance(baseline_rmse, (int, float)) else None
        )
        target["status"] = "rejected"
        decision = {
            "family": family,
            "run_id": run_id,
            "status": "rejected",
            "promoted": False,
            "reason": decision_reason,
            "decision_reason": decision_reason,
            "policy": policy,
            "rmse": rmse_rejected,
            "baseline_rmse": baseline_rejected,
            "abs_improvement": abs_improvement,
            "rmse_threshold": rmse_threshold,
        }

    target["policy"] = policy
    target["decision_reason"] = decision_reason

    save_registry(registry, registry_path)
    return decision


def register_and_evaluate_candidate(
    *,
    family: str,
    run_id: str,
    model_path: str,
    model_sha256: str,
    metrics: Dict[str, Any],
    max_rmse_ratio: float = 1.0,
    min_abs_improvement: float = 0.0,
    registry_path: Path = REGISTRY_PATH,
) -> Dict[str, Any]:
    register_candidate(
        family=family,
        run_id=run_id,
        model_path=model_path,
        model_sha256=model_sha256,
        metrics=metrics,
        registry_path=registry_path,
    )
    return promote_if_better(
        family=family,
        run_id=run_id,
        max_rmse_ratio=max_rmse_ratio,
        min_abs_improvement=min_abs_improvement,
        registry_path=registry_path,
    )
