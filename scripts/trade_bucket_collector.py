import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import typer
from rich import print

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.api_integrator import MarketAPIClient
from core.ops_metrics import append_metric_event

DEFAULT_BUCKETS: Tuple[Tuple[int, int], ...] = (
    (1, 15),
    (16, 50),
    (51, 150),
    (151, 500),
    (501, 5000),
)

DEFAULT_BASE_TYPES: Tuple[str, ...] = (
    "Imbued Wand",
    "Titanium Spirit Shield",
    "Vaal Regalia",
    "Sadist Garb",
    "Large Cluster Jewel",
    "Opal Ring",
)

DEFAULT_MAX_SEARCHES_PER_RUN = 60
DEFAULT_MAX_FETCHES_PER_RUN = 300


def _utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _as_chaos(price_amount: float, price_currency: str) -> Optional[float]:
    if price_amount <= 0:
        return None
    rates = {
        "chaos": 1.0,
        "c": 1.0,
        "divine": 125.0,
        "div": 125.0,
        "exalted": 60.0,
        "exa": 60.0,
        "alch": 0.25,
        "alchemy": 0.25,
    }
    normalized = str(price_currency or "").strip().lower()
    rate = rates.get(normalized)
    if rate is None:
        return None
    return round(float(price_amount) * float(rate), 2)


def build_trade_query(
    base_type: str, bucket_min: int, bucket_max: int
) -> Dict[str, Any]:
    return {
        "query": {
            "status": {"option": "online"},
            "type": base_type,
            "filters": {
                "trade_filters": {
                    "filters": {
                        "price": {
                            "min": int(bucket_min),
                            "max": int(bucket_max),
                            "option": "chaos",
                        }
                    }
                }
            },
        },
        "sort": {"indexed": "desc"},
    }


def initialize_trade_bucket_database(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS trade_bucket_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            league TEXT NOT NULL,
            base_type TEXT NOT NULL,
            bucket_min INTEGER NOT NULL,
            bucket_max INTEGER NOT NULL,
            query_id TEXT NOT NULL,
            item_id TEXT NOT NULL,
            indexed TEXT NOT NULL,
            account_name TEXT,
            price_amount REAL NOT NULL,
            price_currency TEXT NOT NULL,
            price_chaos REAL NOT NULL,
            raw_item_json TEXT NOT NULL,
            collected_at TEXT NOT NULL,
            UNIQUE(league, item_id, indexed, price_chaos)
        )
        """
    )
    conn.commit()


def ingest_trade_bucket_rows(
    conn: sqlite3.Connection,
    rows: Sequence[Dict[str, Any]],
) -> Tuple[int, int]:
    inserted = 0
    duplicates = 0
    with conn:
        for row in rows:
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO trade_bucket_events (
                    run_id,
                    league,
                    base_type,
                    bucket_min,
                    bucket_max,
                    query_id,
                    item_id,
                    indexed,
                    account_name,
                    price_amount,
                    price_currency,
                    price_chaos,
                    raw_item_json,
                    collected_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["run_id"],
                    row["league"],
                    row["base_type"],
                    row["bucket_min"],
                    row["bucket_max"],
                    row["query_id"],
                    row["item_id"],
                    row["indexed"],
                    row["account_name"],
                    row["price_amount"],
                    row["price_currency"],
                    row["price_chaos"],
                    row["raw_item_json"],
                    row["collected_at"],
                ),
            )
            if cur.rowcount == 1:
                inserted += 1
            else:
                duplicates += 1
    return inserted, duplicates


def _event_from_trade_detail(
    detail: Dict[str, Any],
    *,
    run_id: str,
    league: str,
    base_type: str,
    bucket_min: int,
    bucket_max: int,
    query_id: str,
    collected_at: str,
) -> Optional[Dict[str, Any]]:
    listing = detail.get("listing") or {}
    item_payload = detail.get("item") or {}
    if not isinstance(listing, dict) or not isinstance(item_payload, dict):
        return None

    price_info = listing.get("price") or {}
    if not isinstance(price_info, dict):
        return None

    price_amount = float(price_info.get("amount") or 0.0)
    price_currency = str(price_info.get("currency") or "")
    price_chaos = _as_chaos(price_amount, price_currency)
    if price_chaos is None or price_chaos <= 0:
        return None

    item_id = str(detail.get("id") or item_payload.get("id") or "").strip()
    indexed = str(listing.get("indexed") or item_payload.get("indexed") or "").strip()
    if not item_id or not indexed:
        return None

    account_name = str((listing.get("account") or {}).get("name") or "")
    return {
        "run_id": run_id,
        "league": league,
        "base_type": base_type,
        "bucket_min": int(bucket_min),
        "bucket_max": int(bucket_max),
        "query_id": query_id,
        "item_id": item_id,
        "indexed": indexed,
        "account_name": account_name,
        "price_amount": price_amount,
        "price_currency": price_currency,
        "price_chaos": float(price_chaos),
        "raw_item_json": json.dumps(
            item_payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True
        ),
        "collected_at": collected_at,
    }


def _batched_ids(item_ids: Sequence[str], batch_size: int = 10) -> Iterable[List[str]]:
    for idx in range(0, len(item_ids), batch_size):
        yield list(item_ids[idx : idx + batch_size])


def collect_trade_bucket_events(
    *,
    client: MarketAPIClient,
    conn: sqlite3.Connection,
    league: str,
    base_types: Sequence[str],
    buckets: Sequence[Tuple[int, int]],
    run_id: str,
    max_items_per_bucket: int,
    max_searches_per_run: int,
    max_fetches_per_run: int,
) -> Dict[str, int]:
    totals = {
        "searches": 0,
        "fetches": 0,
        "inserted": 0,
        "duplicates": 0,
        "bucket_errors": 0,
        "buckets_processed": 0,
    }
    seen_keys: Set[Tuple[str, str, str, float]] = set()

    for base_type in base_types:
        for bucket_min, bucket_max in buckets:
            if totals["searches"] >= max_searches_per_run:
                return totals

            totals["buckets_processed"] += 1
            collected_at = _utc_now_iso()
            query = build_trade_query(base_type, bucket_min, bucket_max)

            try:
                query_id, result_ids = client.search_items(query)
                totals["searches"] += 1
                if not query_id or not result_ids:
                    continue

                rows_to_insert: List[Dict[str, Any]] = []
                selected_ids = list(result_ids)[: max(max_items_per_bucket, 0)]

                for batch in _batched_ids(selected_ids, batch_size=10):
                    if totals["fetches"] >= max_fetches_per_run:
                        break
                    details = client.fetch_item_details(batch, query_id)
                    totals["fetches"] += 1

                    for detail in details:
                        event = _event_from_trade_detail(
                            detail,
                            run_id=run_id,
                            league=league,
                            base_type=base_type,
                            bucket_min=bucket_min,
                            bucket_max=bucket_max,
                            query_id=query_id,
                            collected_at=collected_at,
                        )
                        if event is None:
                            continue

                        key = (
                            event["league"],
                            event["item_id"],
                            event["indexed"],
                            float(event["price_chaos"]),
                        )
                        if key in seen_keys:
                            totals["duplicates"] += 1
                            continue
                        seen_keys.add(key)
                        rows_to_insert.append(event)

                inserted, duplicates = ingest_trade_bucket_rows(conn, rows_to_insert)
                totals["inserted"] += inserted
                totals["duplicates"] += duplicates
            except Exception:
                totals["bucket_errors"] += 1
                continue
    return totals


def main(
    db_path: str = typer.Option("data/firehose.db", "--db-path", help="SQLite path"),
    league: str = typer.Option("Standard", "--league", help="PoE league"),
    max_items_per_bucket: int = typer.Option(
        30, "--max-items-per-bucket", help="Max items fetched per base+bucket"
    ),
    max_searches_per_run: int = typer.Option(
        DEFAULT_MAX_SEARCHES_PER_RUN,
        "--max-searches-per-run",
        help="Max Trade API searches per run",
    ),
    max_fetches_per_run: int = typer.Option(
        DEFAULT_MAX_FETCHES_PER_RUN,
        "--max-fetches-per-run",
        help="Max Trade API fetch calls per run",
    ),
) -> None:
    run_id = str(int(time.time() * 1000))
    started_at = time.time()
    db_file = Path(db_path)
    db_file.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_file))
    initialize_trade_bucket_database(conn)
    client = MarketAPIClient(league=league)

    status = "ok"
    totals: Dict[str, int] = {
        "searches": 0,
        "fetches": 0,
        "inserted": 0,
        "duplicates": 0,
        "bucket_errors": 0,
        "buckets_processed": 0,
    }
    try:
        totals = collect_trade_bucket_events(
            client=client,
            conn=conn,
            league=client.league,
            base_types=DEFAULT_BASE_TYPES,
            buckets=DEFAULT_BUCKETS,
            run_id=run_id,
            max_items_per_bucket=max_items_per_bucket,
            max_searches_per_run=max_searches_per_run,
            max_fetches_per_run=max_fetches_per_run,
        )
    except Exception:
        status = "error"
        raise
    finally:
        conn.close()
        try:
            client.session.close()
        except Exception:
            pass

    elapsed_ms = max((time.time() - started_at) * 1000.0, 1.0)
    metric_status = (
        "error" if status == "error" or totals["bucket_errors"] > 0 else "ok"
    )
    append_metric_event(
        component="trade_bucket_collector.run",
        run_id=run_id,
        duration_ms=elapsed_ms,
        status=metric_status,
        error_count=int(totals["bucket_errors"]),
        payload={
            "league": client.league,
            "db_path": db_path,
            "max_items_per_bucket": max_items_per_bucket,
            "max_searches_per_run": max_searches_per_run,
            "max_fetches_per_run": max_fetches_per_run,
            **totals,
        },
    )

    print(
        "[bold cyan]trade bucket collector finalizado[/bold cyan] "
        f"league={client.league} inserted={totals['inserted']} "
        f"duplicates={totals['duplicates']} searches={totals['searches']} fetches={totals['fetches']}"
    )


if __name__ == "__main__":
    typer.run(main)
