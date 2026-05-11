import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import typer

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts import build_market_intelligence
from scripts import build_training_snapshot
from scripts import discover_trade_bases
from scripts import train_oracle
from scripts import trade_bucket_collector
from scripts import xgboost_market_simulation_report


app = typer.Typer(help="Run conservative PoE market collection cycles")


@dataclass(frozen=True)
class CollectionConfig:
    db_path: Path = Path("data/firehose.db")
    league: str = "Mirage"
    output_dir: Path = Path("data/training_snapshots")
    intelligence_output: Path = Path("data/market_intelligence/latest.json")
    discovery_max_results: int = 80
    discovery_max_fetches: int = 8
    discovery_min_price: int = 1
    discovery_max_price: int = 500
    dynamic_base_limit: int = 12
    max_items_per_bucket: int = 4
    max_searches_per_run: int = 12
    max_fetches_per_run: int = 14
    search_delay_seconds: float = 14.0
    fetch_delay_seconds: float = 6.0
    intelligence_top: int = 20
    registry_path: Path = Path("data/model_registry/registry.json")
    train_every_cycles: int = 4


def run_collection_cycle(config: CollectionConfig) -> dict[str, str]:
    logging.info("starting market collection cycle league=%s", config.league)
    results = {
        "discover": _run_step("discover", lambda: _run_discovery(config)),
        "collect": _run_step("collect", lambda: _run_collector(config)),
        "snapshot": _run_step("snapshot", lambda: _run_snapshot(config)),
        "intelligence": _run_step("intelligence", lambda: _run_intelligence(config)),
    }
    logging.info("finished market collection cycle results=%s", results)
    return results


def run_model_cycle(config: CollectionConfig) -> dict[str, str]:
    logging.info("starting model cycle league=%s", config.league)
    results = {
        "train": _run_step("train", lambda: _run_training(config)),
        "simulation": _run_step("simulation", lambda: _run_simulation(config)),
    }
    logging.info("finished model cycle results=%s", results)
    return results


def _run_step(name: str, step: Callable[[], None]) -> str:
    try:
        step()
    except Exception:
        logging.exception("market collection step failed step=%s", name)
        return "error"
    return "ok"


def _run_discovery(config: CollectionConfig) -> None:
    discover_trade_bases.discover(
        db_path=str(config.db_path),
        league=config.league,
        max_results=config.discovery_max_results,
        max_fetches=config.discovery_max_fetches,
        min_price=config.discovery_min_price,
        max_price=config.discovery_max_price,
    )


def _run_collector(config: CollectionConfig) -> None:
    trade_bucket_collector.main(
        db_path=str(config.db_path),
        league=config.league,
        max_items_per_bucket=config.max_items_per_bucket,
        max_searches_per_run=config.max_searches_per_run,
        max_fetches_per_run=config.max_fetches_per_run,
        dynamic_bases=True,
        dynamic_base_limit=config.dynamic_base_limit,
        search_delay_seconds=config.search_delay_seconds,
        fetch_delay_seconds=config.fetch_delay_seconds,
    )


def _run_snapshot(config: CollectionConfig) -> None:
    build_training_snapshot.build_training_snapshot(
        db_path=str(config.db_path),
        output_dir=str(config.output_dir),
        league=config.league,
    )


def _run_intelligence(config: CollectionConfig) -> None:
    build_market_intelligence.build_market_intelligence(
        gold_path=config.output_dir / "gold",
        output=config.intelligence_output,
        risk_profile="balanced",
        league=config.league,
        top=config.intelligence_top,
    )


def _run_training(config: CollectionConfig) -> None:
    train_oracle.train_xgboost_oracle(
        league=config.league,
        source="parquet",
        parquet_path=str(config.output_dir / "gold"),
        registry_path=str(config.registry_path),
    )


def _run_simulation(config: CollectionConfig) -> None:
    xgboost_market_simulation_report.build_latest_simulation_report(
        league=config.league,
        registry_path=config.registry_path,
        latest_path=config.intelligence_output,
        gold_path=config.output_dir / "gold",
        logs_dir=Path("logs"),
    )


def _configure_logging(log_file: Path | None) -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=handlers,
        force=True,
    )


@app.callback(invoke_without_command=True)
def main(
    db_path: Path = typer.Option(Path("data/firehose.db"), "--db-path", help="SQLite path"),
    league: str = typer.Option("Mirage", "--league", help="PoE league"),
    output_dir: Path = typer.Option(
        Path("data/training_snapshots"),
        "--output-dir",
        help="Training snapshot output root",
    ),
    intelligence_output: Path = typer.Option(
        Path("data/market_intelligence/latest.json"),
        "--intelligence-output",
        help="Market intelligence JSON output",
    ),
    cycle_sleep_seconds: float = typer.Option(
        1800.0,
        "--cycle-sleep-seconds",
        min=1.0,
        help="Delay between cycles when not using --once",
    ),
    once: bool = typer.Option(False, "--once", help="Run one cycle and exit"),
    log_file: Path | None = typer.Option(None, "--log-file", help="Optional log file"),
    train_every_cycles: int = typer.Option(
        4,
        "--train-every-cycles",
        min=0,
        help="Run XGBoost train+simulation every N collection cycles. Use 0 to disable.",
    ),
) -> None:
    _configure_logging(log_file)
    config = CollectionConfig(
        db_path=db_path,
        league=league,
        output_dir=output_dir,
        intelligence_output=intelligence_output,
        train_every_cycles=train_every_cycles,
    )
    cycle_number = 0
    while True:
        cycle_number += 1
        results = run_collection_cycle(config)
        typer.echo(f"market collection cycle results={results}")
        if config.train_every_cycles and cycle_number % config.train_every_cycles == 0:
            model_results = run_model_cycle(config)
            typer.echo(f"model cycle results={model_results}")
        if once:
            return
        time.sleep(cycle_sleep_seconds)


if __name__ == "__main__":
    app()
