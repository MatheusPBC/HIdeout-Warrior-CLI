import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List


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
    if target is None:
        return {
            "family": family,
            "run_id": run_id,
            "status": "not_found",
            "promoted": False,
            "reason": "candidate_not_found",
        }

    metrics = target.get("metrics") if isinstance(target.get("metrics"), dict) else {}
    rmse = metrics.get("rmse")
    baseline_rmse = metrics.get("baseline_rmse")
    should_promote = (
        isinstance(rmse, (int, float))
        and isinstance(baseline_rmse, (int, float))
        and float(rmse) < float(baseline_rmse)
    )

    if should_promote:
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
            "reason": "rmse_lt_baseline",
            "rmse": float(rmse),
            "baseline_rmse": float(baseline_rmse),
        }
    else:
        target["status"] = "rejected"
        decision = {
            "family": family,
            "run_id": run_id,
            "status": "rejected",
            "promoted": False,
            "reason": "rmse_gte_baseline",
            "rmse": float(rmse) if isinstance(rmse, (int, float)) else None,
            "baseline_rmse": (
                float(baseline_rmse)
                if isinstance(baseline_rmse, (int, float))
                else None
            ),
        }

    save_registry(registry, registry_path)
    return decision


def register_and_evaluate_candidate(
    *,
    family: str,
    run_id: str,
    model_path: str,
    model_sha256: str,
    metrics: Dict[str, Any],
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
    return promote_if_better(family=family, run_id=run_id, registry_path=registry_path)
