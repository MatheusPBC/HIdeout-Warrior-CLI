import os
import sqlite3
import sys
import time
from pathlib import Path

import typer
from rich import print

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.api_integrator import MarketAPIClient
from core.trade_base_discovery import discover_trade_base_types

app = typer.Typer(help="Discover promising base types from broad Trade API samples")


@app.callback(invoke_without_command=True)
def discover(
    db_path: str = typer.Option("data/firehose.db", "--db-path", help="SQLite path"),
    league: str = typer.Option("Mirage", "--league", help="PoE league"),
    max_results: int = typer.Option(100, "--max-results", help="Max search result IDs to inspect"),
    max_fetches: int = typer.Option(10, "--max-fetches", help="Max Trade API fetch calls"),
    min_price: int = typer.Option(1, "--min-price", help="Minimum chaos price"),
    max_price: int = typer.Option(500, "--max-price", help="Maximum chaos price"),
) -> None:
    db_file = Path(db_path)
    db_file.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_file))
    client = MarketAPIClient(league=league)
    try:
        totals = discover_trade_base_types(
            client=client,
            conn=conn,
            league=client.league,
            run_id=str(int(time.time() * 1000)),
            max_results=max_results,
            max_fetches=max_fetches,
            min_price=min_price,
            max_price=max_price,
        )
    finally:
        conn.close()
        session = getattr(client, "session", None)
        if session is not None:
            session.close()

    print(
        "[bold cyan]trade base discovery finalizado[/bold cyan] "
        f"league={client.league} searched={totals['searched']} fetched={totals['fetched']} "
        f"candidates={totals['candidates']} base_types={totals['base_types']}"
    )


if __name__ == "__main__":
    app()
