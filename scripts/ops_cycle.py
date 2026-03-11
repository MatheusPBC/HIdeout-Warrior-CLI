import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import typer
from rich import print
from typer.models import OptionInfo

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.ops_metrics import append_metric_event
from scripts.build_training_snapshot import build_training_snapshot
from scripts.firehose_miner import run as run_firehose_miner
from scripts.train_oracle import train_xgboost_oracle

app = typer.Typer(help="Operational cycle orchestrator")


def _utc_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def _clean_optional_str(value: Optional[str]) -> Optional[str]:
    if value is None or isinstance(value, OptionInfo):
        return None
    return str(value)


def _run_snapshot_step(
    db_path: str,
    snapshot_output_dir: str,
    snapshot_date: Optional[str],
) -> None:
    build_training_snapshot(
        db_path=db_path,
        output_dir=snapshot_output_dir,
        snapshot_date=snapshot_date,
    )


@app.command()
def run(
    db_path: str = typer.Option("data/firehose.db", "--db-path", help="SQLite path"),
    start_change_id: Optional[str] = typer.Option(
        None,
        "--start-change-id",
        help="Optional miner start change id",
    ),
    oauth_token: Optional[str] = typer.Option(
        None,
        "--oauth-token",
        help="OAuth bearer token for firehose miner",
    ),
    max_pages: int = typer.Option(0, "--max-pages", help="Miner max pages"),
    sleep_seconds: float = typer.Option(
        1.5,
        "--sleep-seconds",
        help="Miner delay between pages",
    ),
    snapshot_output_dir: str = typer.Option(
        "data/training_snapshots",
        "--snapshot-output-dir",
        help="Snapshot output root",
    ),
    snapshot_date: Optional[str] = typer.Option(
        None,
        "--snapshot-date",
        help="Snapshot date override",
    ),
    league: str = typer.Option("Standard", "--league", help="PoE league"),
    items: int = typer.Option(500, "--items", help="Items per base"),
    train_source: str = typer.Option(
        "parquet",
        "--train-source",
        help="Train source: api|sqlite|parquet",
    ),
    parquet_path: Optional[str] = typer.Option(
        None,
        "--parquet-path",
        help="Train parquet path",
    ),
    sqlite_path: str = typer.Option(
        "data/firehose.db",
        "--sqlite-path",
        help="Train SQLite source path",
    ),
    promotion_max_rmse_ratio: float = typer.Option(
        1.0,
        "--promotion-max-rmse-ratio",
        help="Registry promotion policy: max RMSE ratio",
    ),
    promotion_min_abs_improvement: float = typer.Option(
        0.0,
        "--promotion-min-abs-improvement",
        help="Registry promotion policy: min absolute RMSE improvement",
    ),
    registry_path: str = typer.Option(
        "data/model_registry/registry.json",
        "--registry-path",
        help="Model registry path",
    ),
    continue_on_error: bool = typer.Option(
        False,
        "--continue-on-error",
        help="Continue pipeline on step failure",
    ),
) -> None:
    run_id = _utc_run_id()
    started_at = time.time()
    step_results: List[Dict[str, Any]] = []
    failed_steps = 0
    first_error: Exception | None = None

    effective_parquet_path = _clean_optional_str(parquet_path)
    if not effective_parquet_path and train_source == "parquet":
        effective_parquet_path = str(Path(snapshot_output_dir) / "gold")

    steps: List[tuple[str, Callable[[], None]]] = [
        (
            "firehose_miner.run",
            lambda: run_firehose_miner(
                db_path=db_path,
                start_change_id=_clean_optional_str(start_change_id),
                max_pages=max_pages,
                sleep_seconds=sleep_seconds,
                oauth_token=_clean_optional_str(oauth_token),
            ),
        ),
        (
            "build_training_snapshot.build",
            lambda: _run_snapshot_step(
                db_path=db_path,
                snapshot_output_dir=snapshot_output_dir,
                snapshot_date=_clean_optional_str(snapshot_date),
            ),
        ),
        (
            "train_oracle.train",
            lambda: train_xgboost_oracle(
                league=league,
                items_per_base=items,
                source=train_source,
                parquet_path=effective_parquet_path or "data/firehose.parquet",
                sqlite_path=sqlite_path,
                promotion_max_rmse_ratio=promotion_max_rmse_ratio,
                promotion_min_abs_improvement=promotion_min_abs_improvement,
                registry_path=registry_path,
            ),
        ),
    ]

    for step_name, step_fn in steps:
        step_started = time.time()
        print(f"[cyan]Executando {step_name}[/cyan]")
        try:
            step_fn()
            step_results.append(
                {
                    "step": step_name,
                    "status": "ok",
                    "duration_ms": int(max((time.time() - step_started) * 1000, 0)),
                }
            )
        except Exception as exc:
            failed_steps += 1
            step_results.append(
                {
                    "step": step_name,
                    "status": "error",
                    "duration_ms": int(max((time.time() - step_started) * 1000, 0)),
                    "error": str(exc),
                }
            )
            if first_error is None:
                first_error = exc
            print(f"[red]Falha em {step_name}: {exc}[/red]")
            if not continue_on_error:
                break

    total_duration_ms = max((time.time() - started_at) * 1000, 0.0)
    status = "ok" if failed_steps == 0 else "error"
    append_metric_event(
        component="ops_cycle.daily_run",
        run_id=run_id,
        duration_ms=total_duration_ms,
        status=status,
        error_count=failed_steps,
        payload={
            "continue_on_error": continue_on_error,
            "train_source": train_source,
            "snapshot_output_dir": snapshot_output_dir,
            "effective_parquet_path": effective_parquet_path,
            "steps": step_results,
        },
    )

    if first_error is not None and not continue_on_error:
        raise first_error


if __name__ == "__main__":
    app()
