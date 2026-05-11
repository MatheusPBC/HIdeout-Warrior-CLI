import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import typer


app = typer.Typer(help="Build XGBoost market simulation reports")


@dataclass(frozen=True)
class SimulationReportResult:
    json_path: Path
    log_path: Path


def build_latest_simulation_report(
    *,
    league: str = "Mirage",
    metadata_dir: Path = Path("data/model_metadata"),
    registry_path: Path = Path("data/model_registry/registry.json"),
    latest_path: Path = Path("data/market_intelligence/latest.json"),
    gold_path: Path = Path("data/training_snapshots/gold"),
    logs_dir: Path = Path("logs"),
) -> SimulationReportResult:
    metadata_files = sorted(metadata_dir.glob("oracle_training_*.json"))
    if not metadata_files:
        raise FileNotFoundError(f"No training metadata found in {metadata_dir}")
    return build_simulation_report(
        metadata_path=metadata_files[-1],
        registry_path=registry_path,
        latest_path=latest_path,
        gold_path=gold_path,
        logs_dir=logs_dir,
        league=league,
    )


def build_simulation_report(
    *,
    metadata_path: Path,
    registry_path: Path,
    latest_path: Path,
    gold_path: Path,
    logs_dir: Path,
    league: str = "Mirage",
    timestamp: str | None = None,
) -> SimulationReportResult:
    metadata = _load_json(metadata_path)
    registry = _load_json(registry_path) if registry_path.exists() else {"families": {}}
    latest = _load_json(latest_path)
    gold_rows = _count_gold_rows(gold_path, league)
    model_by_family = _models_by_family(metadata, registry)
    candidates = _rank_candidates(latest, model_by_family)
    generated_at = _now_iso()
    suffix = timestamp or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    summary = _build_summary(
        generated_at=generated_at,
        league=league,
        metadata_path=metadata_path,
        registry_path=registry_path,
        latest_path=latest_path,
        gold_rows=gold_rows,
        model_by_family=model_by_family,
        candidates=candidates,
    )
    logs_dir.mkdir(parents=True, exist_ok=True)
    json_path = logs_dir / f"xgboost_market_simulation_{suffix}.json"
    log_path = logs_dir / f"xgboost_market_simulation_{suffix}.log"
    json_path.write_text(
        json.dumps(summary, ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    log_path.write_text(_format_log(summary, candidates), encoding="utf-8")
    return SimulationReportResult(json_path=json_path, log_path=log_path)


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return payload


def _count_gold_rows(gold_path: Path, league: str) -> int:
    frame = pd.read_parquet(gold_path)
    if "league" in frame.columns:
        frame = frame[frame["league"].astype(str) == league]
    return int(len(frame))


def _models_by_family(metadata: dict[str, Any], registry: dict[str, Any]) -> dict[str, dict[str, Any]]:
    models = {}
    for report in metadata.get("models", []):
        if not isinstance(report, dict):
            continue
        family = str(report.get("family", ""))
        if not family:
            continue
        metrics = report.get("metrics", {})
        if not isinstance(metrics, dict):
            metrics = {}
        baseline_rmse = float(metrics.get("baseline_rmse") or 0.0)
        rmse = float(metrics.get("rmse") or 0.0)
        models[family] = {
            "rows_total": int(report.get("rows_total") or 0),
            "rmse": rmse,
            "mae": float(metrics.get("mae") or 0.0),
            "baseline_rmse": baseline_rmse,
            "rmse_ratio": round(rmse / baseline_rmse, 4) if baseline_rmse > 0 else None,
            "model_path": report.get("model_path"),
            "registry_decision": report.get("registry_decision", {}),
        }

    family_registry = registry.get("families", {}) if isinstance(registry, dict) else {}
    for family, model in models.items():
        entry = family_registry.get(family, {}) if isinstance(family_registry, dict) else {}
        model["active_version"] = entry.get("active_version") if isinstance(entry, dict) else None
    return models


def _rank_candidates(latest: dict[str, Any], model_by_family: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    candidates = []
    for entry in latest.get("segments", []):
        if not isinstance(entry, dict):
            continue
        segment = entry.get("segment", {}) if isinstance(entry.get("segment"), dict) else {}
        score = entry.get("score", {}) if isinstance(entry.get("score"), dict) else {}
        metrics = entry.get("metrics", {}) if isinstance(entry.get("metrics"), dict) else {}
        family = str(segment.get("item_family") or "generic")
        model = model_by_family.get(family)
        for opportunity in entry.get("opportunities", []):
            if isinstance(opportunity, dict):
                candidates.append(_candidate_row(segment, score, metrics, opportunity, family, model))

    priority = {
        "valid_for_manual_review": 0,
        "watch_only": 1,
        "skip_weak_model": 2,
        "skip_no_model": 3,
        "skip_no_edge": 4,
    }
    return sorted(
        candidates,
        key=lambda row: (
            priority.get(str(row["decision"]), 9),
            -float(row["confidence_margin_after_mae"]),
            -float(row["estimated_upside"]),
        ),
    )


def _candidate_row(
    segment: dict[str, Any],
    score: dict[str, Any],
    metrics: dict[str, Any],
    opportunity: dict[str, Any],
    family: str,
    model: dict[str, Any] | None,
) -> dict[str, Any]:
    listed = float(opportunity.get("listed_price") or 0.0)
    reference = float(opportunity.get("reference_price") or 0.0)
    upside = float(opportunity.get("estimated_upside") or 0.0)
    expected_profit = reference - listed
    model_mae = float(model.get("mae") or 0.0) if model else 0.0
    confidence_margin = expected_profit - model_mae
    status = str(score.get("status") or "watch")
    return {
        "decision": _decision(model, confidence_margin, listed, expected_profit, status),
        "item_id": str(opportunity.get("item_id") or "unknown"),
        "family": family,
        "base_type": str(segment.get("base_type") or opportunity.get("base_type") or "unknown"),
        "segment_status": status,
        "market_score": float(score.get("market_score") or 0.0),
        "segment_sample_count": int(metrics.get("sample_count") or 0),
        "listed_price": listed,
        "reference_price": reference,
        "expected_profit": round(expected_profit, 2),
        "estimated_upside": round(upside, 4),
        "model_rmse": float(model.get("rmse") or 0.0) if model else None,
        "model_mae": model_mae if model else None,
        "confidence_margin_after_mae": round(confidence_margin, 2),
        "model_rmse_ratio": model.get("rmse_ratio") if model else None,
    }


def _decision(
    model: dict[str, Any] | None,
    confidence_margin: float,
    listed: float,
    expected_profit: float,
    status: str,
) -> str:
    if not model:
        return "skip_no_model"
    rmse_ratio = model.get("rmse_ratio")
    if rmse_ratio is not None and float(rmse_ratio) > 0.95:
        return "skip_weak_model"
    if confidence_margin >= max(10.0, listed * 0.15) and status in {
        "strong_candidate",
        "evaluation_candidate",
        "emerging",
        "watch",
    }:
        return "valid_for_manual_review"
    if expected_profit > 0:
        return "watch_only"
    return "skip_no_edge"


def _build_summary(
    *,
    generated_at: str,
    league: str,
    metadata_path: Path,
    registry_path: Path,
    latest_path: Path,
    gold_rows: int,
    model_by_family: dict[str, dict[str, Any]],
    candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    counts = {}
    for row in candidates:
        counts[row["decision"]] = counts.get(row["decision"], 0) + 1
    return {
        "generated_at_utc": generated_at,
        "league": league,
        "metadata_path": str(metadata_path),
        "registry_path": str(registry_path),
        "market_intelligence_path": str(latest_path),
        "gold_rows": int(gold_rows),
        "models": model_by_family,
        "candidate_counts": counts,
        "top_candidates": candidates[:25],
    }


def _format_log(summary: dict[str, Any], candidates: list[dict[str, Any]]) -> str:
    lines = [
        f"XGBoost market simulation - {summary['league']} - {summary['generated_at_utc']}",
        f"metadata={summary['metadata_path']}",
        f"gold_rows={summary['gold_rows']}",
        "",
        "Models:",
    ]
    for family, model in sorted(summary["models"].items()):
        lines.append(
            f"- {family}: rows={model['rows_total']} rmse={model['rmse']:.2f} mae={model['mae']:.2f} "
            f"baseline_rmse={model['baseline_rmse']:.2f} rmse_ratio={model['rmse_ratio']} active={model.get('active_version')}"
        )
    lines.extend(["", f"Candidate counts: {summary['candidate_counts']}", "", "Top candidates:"])
    for row in candidates[:15]:
        lines.append(
            f"- {row['decision']} | {row['item_id']} | {row['family']} | {row['base_type']} | "
            f"listed={row['listed_price']:.1f}c ref={row['reference_price']:.1f}c "
            f"profit={row['expected_profit']:.1f}c after_mae={row['confidence_margin_after_mae']:.1f}c "
            f"upside={row['estimated_upside']:.0%} status={row['segment_status']}"
        )
    return "\n".join(lines) + "\n"


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


@app.callback(invoke_without_command=True)
def main(
    league: str = typer.Option("Mirage", "--league", help="PoE league"),
    metadata_dir: Path = typer.Option(Path("data/model_metadata"), "--metadata-dir"),
    registry_path: Path = typer.Option(Path("data/model_registry/registry.json"), "--registry-path"),
    latest_path: Path = typer.Option(Path("data/market_intelligence/latest.json"), "--latest-path"),
    gold_path: Path = typer.Option(Path("data/training_snapshots/gold"), "--gold-path"),
    logs_dir: Path = typer.Option(Path("logs"), "--logs-dir"),
) -> None:
    result = build_latest_simulation_report(
        league=league,
        metadata_dir=metadata_dir,
        registry_path=registry_path,
        latest_path=latest_path,
        gold_path=gold_path,
        logs_dir=logs_dir,
    )
    typer.echo(f"json={result.json_path}")
    typer.echo(f"log={result.log_path}")


if __name__ == "__main__":
    app()
