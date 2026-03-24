import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import typer
from rich import print

app = typer.Typer(help="Operational report builder")


def _now_utc_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _percentile(values: List[float], percentile: float) -> float | None:
    if not values:
        return None
    sorted_values = sorted(values)
    rank = int(math.ceil((percentile / 100.0) * len(sorted_values))) - 1
    rank = min(max(rank, 0), len(sorted_values) - 1)
    return float(sorted_values[rank])


def _load_metrics(metrics_dir: Path) -> Dict[str, Dict[str, Any]]:
    aggregate: Dict[str, Dict[str, Any]] = {}
    for metrics_file in sorted(metrics_dir.glob("*.jsonl")):
        try:
            lines = metrics_file.read_text(encoding="utf-8").splitlines()
        except OSError:
            continue
        for line in lines:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(row, dict):
                continue
            component = str(row.get("component", "")).strip()
            if not component:
                continue

            current = aggregate.setdefault(
                component,
                {
                    "runs": 0,
                    "ok": 0,
                    "error": 0,
                    "durations": [],
                    "last_ts_utc": None,
                },
            )
            current["runs"] += 1

            status = str(row.get("status", "")).strip().lower()
            if status == "ok":
                current["ok"] += 1
            elif status == "error":
                current["error"] += 1

            duration_raw = row.get("duration_ms")
            if isinstance(duration_raw, (int, float)):
                current["durations"].append(float(duration_raw))

            ts_raw = row.get("ts_utc")
            if isinstance(ts_raw, str):
                if current["last_ts_utc"] is None or ts_raw > current["last_ts_utc"]:
                    current["last_ts_utc"] = ts_raw

    report: Dict[str, Dict[str, Any]] = {}
    for component, data in aggregate.items():
        durations = list(data["durations"])
        runs = int(data["runs"])
        errors = int(data["error"])
        report[component] = {
            "runs": runs,
            "ok": int(data["ok"]),
            "error": errors,
            "error_rate": (errors / runs) if runs else 0.0,
            "duration_ms": {
                "avg": (sum(durations) / len(durations)) if durations else None,
                "p50": _percentile(durations, 50),
                "p95": _percentile(durations, 95),
            },
            "last_ts_utc": data["last_ts_utc"],
        }
    return report


def _load_snapshot_metrics(metrics_dir: Path) -> Dict[str, Any]:
    """Carrega métricas de snapshot mais recentes."""
    snapshot_files = sorted(metrics_dir.glob("snapshot_*.json"))
    if not snapshot_files:
        return {}

    latest = snapshot_files[-1]
    try:
        payload = json.loads(latest.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}

    if not isinstance(payload, dict):
        return {}

    return {
        "snapshot_date": payload.get("snapshot_date"),
        "run_id": payload.get("run_id"),
        "ts_utc": payload.get("ts_utc"),
        "bronze": payload.get("bronze", {}),
        "silver": payload.get("silver", {}),
        "gold": payload.get("gold", {}),
        "source_file": str(latest),
    }


def _load_registry_state(registry_path: Path) -> Dict[str, Any]:
    if not registry_path.exists():
        return {}
    try:
        payload = json.loads(registry_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    families = payload.get("families")
    if not isinstance(families, dict):
        return {}

    active_by_family: Dict[str, Any] = {}
    for family, entry in families.items():
        if not isinstance(entry, dict):
            continue
        active_by_family[str(family)] = entry.get("active_version")
    return active_by_family


@app.command()
def build(
    metrics_dir: str = typer.Option(
        "data/ops_metrics",
        "--metrics-dir",
        help="Directory with ops metric JSONL files",
    ),
    registry_path: str = typer.Option(
        "data/model_registry/registry.json",
        "--registry-path",
        help="Model registry file path",
    ),
    output_path: str | None = typer.Option(
        None,
        "--output-path",
        help="Report JSON output path",
    ),
) -> None:
    metrics_root = Path(metrics_dir)
    registry_file = Path(registry_path)
    destination = (
        Path(output_path)
        if output_path
        else Path("data/ops_reports") / f"ops_report_{_now_utc_compact()}.json"
    )

    components = _load_metrics(metrics_root)
    active_registry_by_family = _load_registry_state(registry_file)
    snapshot_metrics = _load_snapshot_metrics(metrics_root)

    report_payload = {
        "generated_at_utc": datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
        "metrics": {
            "components": components,
            "components_count": len(components),
            "snapshot": snapshot_metrics,
        },
        "registry": {
            "path": str(registry_file),
            "active_by_family": active_registry_by_family,
        },
    }

    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(report_payload, ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    print(f"[bold cyan]Ops report salvo[/bold cyan] {destination}")
    print(
        "[cyan]Resumo[/cyan] "
        f"components={len(components)} "
        f"families={len(active_registry_by_family)}"
    )
    for component, stats in sorted(components.items()):
        print(
            f"- {component}: runs={stats['runs']} ok={stats['ok']} "
            f"error={stats['error']} error_rate={stats['error_rate']:.1%}"
        )

    if snapshot_metrics:
        bronze = snapshot_metrics.get("bronze", {})
        silver = snapshot_metrics.get("silver", {})
        gold = snapshot_metrics.get("gold", {})
        print(
            f"[cyan]Snapshot metrics[/cyan] date={snapshot_metrics.get('snapshot_date', 'N/A')} "
            f"bronze={bronze.get('rows', 0)} silver={silver.get('rows', 0)} gold={gold.get('rows', 0)}"
        )


if __name__ == "__main__":
    app()
