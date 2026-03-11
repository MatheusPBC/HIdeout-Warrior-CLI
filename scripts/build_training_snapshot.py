import os
import sys
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from uuid import uuid4

import pandas as pd
import typer
from rich import print

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.item_normalizer import normalize_trade_item
from scripts.train_oracle import (
    _trade_item_from_firehose_row,
    parse_trade_item_to_features,
)

app = typer.Typer(help="Build bronze/silver/gold Parquet training snapshots")


def _safe_json_load(raw_payload: Any) -> Optional[Dict[str, Any]]:
    if not raw_payload:
        return None
    if isinstance(raw_payload, dict):
        return raw_payload
    if not isinstance(raw_payload, str):
        return None
    try:
        parsed = json.loads(raw_payload)
    except (TypeError, json.JSONDecodeError):
        return None
    if not isinstance(parsed, dict):
        return None
    return parsed


def _stable_event_key(row: Dict[str, Any], normalized_raw_json: str) -> str:
    candidate = "|".join(
        [
            str(row.get("source_table", "")),
            str(row.get("change_id", "")),
            str(row.get("query_id", "")),
            str(row.get("item_id", "")),
            str(row.get("indexed", "")),
            str(row.get("price_chaos", "")),
            normalized_raw_json,
        ]
    )
    return hashlib.sha1(candidate.encode("utf-8")).hexdigest()


def _normalize_snapshot_date(indexed: Any, default_date: str) -> str:
    if not indexed:
        return default_date
    try:
        dt = datetime.fromisoformat(str(indexed).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return default_date
    return dt.date().isoformat()


def _write_partitioned_parquet(
    frame: pd.DataFrame,
    layer_dir: Path,
    partition_cols: Sequence[str],
) -> int:
    layer_dir.mkdir(parents=True, exist_ok=True)
    if frame.empty:
        return 0

    partitions_written = 0
    grouped = frame.groupby(list(partition_cols), dropna=False, sort=True)
    for partition_values, partition_df in grouped:
        if not isinstance(partition_values, tuple):
            partition_values = (partition_values,)

        partition_path = layer_dir
        for col_name, col_value in zip(partition_cols, partition_values):
            safe_value = str(col_value) if col_value is not None else "unknown"
            partition_path = partition_path / f"{col_name}={safe_value}"

        partition_path.mkdir(parents=True, exist_ok=True)
        output_file = partition_path / f"part-{uuid4().hex}.parquet"
        payload = partition_df.drop(columns=list(partition_cols), errors="ignore")
        try:
            payload.to_parquet(output_file, index=False)
        except ImportError as exc:
            raise RuntimeError(
                "Escrita Parquet requer engine instalada (pyarrow ou fastparquet)."
            ) from exc
        partitions_written += 1
    return partitions_written


def build_bronze_dataframe(
    db_path: str, snapshot_date: str
) -> Tuple[pd.DataFrame, Dict[str, int]]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    stats = {
        "rows_read": 0,
        "invalid_json_skipped": 0,
        "rows_valid": 0,
        "rows_deduped": 0,
    }
    records: List[Dict[str, Any]] = []

    def _table_exists(conn_ref: sqlite3.Connection, table_name: str) -> bool:
        row = conn_ref.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
            (table_name,),
        ).fetchone()
        return row is not None

    def _load_rows_from_table(table_name: str) -> List[sqlite3.Row]:
        if table_name == "stash_events":
            return conn.execute(
                """
                SELECT
                    change_id,
                    '' AS query_id,
                    item_id,
                    league,
                    account_name,
                    indexed,
                    price_amount,
                    price_currency,
                    price_chaos,
                    raw_item_json,
                    'stash_events' AS source_table
                FROM stash_events
                """
            ).fetchall()
        return conn.execute(
            """
            SELECT
                '' AS change_id,
                query_id,
                item_id,
                league,
                account_name,
                indexed,
                price_amount,
                price_currency,
                price_chaos,
                raw_item_json,
                'trade_bucket_events' AS source_table
            FROM trade_bucket_events
            """
        ).fetchall()

    try:
        rows: List[sqlite3.Row] = []
        if _table_exists(conn, "stash_events"):
            rows.extend(_load_rows_from_table("stash_events"))
        if _table_exists(conn, "trade_bucket_events"):
            rows.extend(_load_rows_from_table("trade_bucket_events"))

        stats["rows_read"] = len(rows)
        for row in rows:
            row_map = dict(row)
            parsed_item = _safe_json_load(row_map.get("raw_item_json"))
            if parsed_item is None:
                stats["invalid_json_skipped"] += 1
                continue

            normalized_raw_json = json.dumps(
                parsed_item, ensure_ascii=True, separators=(",", ":"), sort_keys=True
            )
            price_chaos = float(row_map.get("price_chaos") or 0.0)
            record = {
                "event_key": _stable_event_key(row_map, normalized_raw_json),
                "change_id": str(row_map.get("change_id") or ""),
                "item_id": str(row_map.get("item_id") or parsed_item.get("id") or ""),
                "league": str(row_map.get("league") or "Unknown"),
                "account_name": str(row_map.get("account_name") or ""),
                "source_table": str(row_map.get("source_table") or "unknown"),
                "indexed": str(
                    row_map.get("indexed") or parsed_item.get("indexed") or ""
                ),
                "price_amount": float(
                    row_map.get("price_amount") or price_chaos or 0.0
                ),
                "price_currency": str(row_map.get("price_currency") or "chaos"),
                "price_chaos": price_chaos,
                "raw_item_json": normalized_raw_json,
                "snapshot_date": _normalize_snapshot_date(
                    row_map.get("indexed"), default_date=snapshot_date
                ),
            }
            records.append(record)
            stats["rows_valid"] += 1
    finally:
        conn.close()

    frame = pd.DataFrame(records)
    if frame.empty:
        return frame, stats

    before_dedupe = len(frame)
    frame = frame.drop_duplicates(subset=["event_key"], keep="last").reset_index(
        drop=True
    )
    stats["rows_deduped"] = before_dedupe - len(frame)
    return frame, stats


def build_silver_dataframe(bronze_df: pd.DataFrame) -> pd.DataFrame:
    if bronze_df.empty:
        return pd.DataFrame()

    records: List[Dict[str, Any]] = []
    for _, row in bronze_df.iterrows():
        row_map = row.to_dict()
        trade_item = _trade_item_from_firehose_row(row_map)
        if trade_item is None:
            continue

        normalized = normalize_trade_item(
            trade_item,
            listed_price=float(row_map.get("price_chaos", 0.0) or 0.0),
            listing_currency=str(row_map.get("price_currency", "chaos") or "chaos"),
            listing_amount=float(
                row_map.get("price_amount", row_map.get("price_chaos", 0.0)) or 0.0
            ),
        )
        if normalized is None:
            continue

        normalized_row = normalized.to_dict()
        normalized_row.update(
            {
                "event_key": row_map.get("event_key", ""),
                "league": row_map.get("league", "Unknown"),
                "snapshot_date": row_map.get("snapshot_date"),
                "indexed": row_map.get("indexed", ""),
                "raw_item_json": row_map.get("raw_item_json", ""),
                "price_chaos": float(row_map.get("price_chaos", 0.0) or 0.0),
            }
        )
        records.append(normalized_row)

    frame = pd.DataFrame(records)
    if frame.empty:
        return frame

    if "indexed" in frame.columns:
        frame["_indexed_dt"] = pd.to_datetime(
            frame["indexed"], errors="coerce", utc=True
        )
        frame = frame.sort_values("_indexed_dt")

    if "item_id" in frame.columns:
        with_item_id = frame.loc[frame["item_id"].astype(str).str.len() > 0].copy()
        without_item_id = frame.loc[frame["item_id"].astype(str).str.len() == 0].copy()
        with_item_id = with_item_id.drop_duplicates(subset=["item_id"], keep="last")
        frame = pd.concat([with_item_id, without_item_id], ignore_index=True)

    return frame.drop(columns=["_indexed_dt"], errors="ignore").reset_index(drop=True)


def build_gold_dataframe(silver_df: pd.DataFrame) -> pd.DataFrame:
    if silver_df.empty:
        return pd.DataFrame()

    records: List[Dict[str, Any]] = []
    for _, row in silver_df.iterrows():
        row_map = row.to_dict()
        raw_payload = _safe_json_load(row_map.get("raw_item_json"))
        if raw_payload is None:
            continue

        item_data = {
            "listing": {
                "whisper": str(row_map.get("whisper") or "@placeholder hi"),
                "indexed": str(
                    row_map.get("listed_at") or row_map.get("indexed") or ""
                ),
                "account": {
                    "name": str(
                        row_map.get("seller") or row_map.get("account_name") or ""
                    )
                },
                "price": {
                    "currency": str(
                        row_map.get("listing_currency")
                        or row_map.get("price_currency")
                        or "chaos"
                    ),
                    "amount": float(
                        row_map.get("listing_amount", row_map.get("price_chaos", 0.0))
                        or 0.0
                    ),
                },
            },
            "item": raw_payload,
        }
        features = parse_trade_item_to_features(
            item_data,
            currency_rates={},
            meta_scores=None,
            listed_price_chaos_override=float(row_map.get("price_chaos", 0.0) or 0.0),
        )
        if not features:
            continue

        features["league"] = str(row_map.get("league") or "Unknown")
        features["snapshot_date"] = row_map.get("snapshot_date")
        features["item_family"] = str(
            features.get("item_family") or row_map.get("item_family") or "generic"
        )
        records.append(features)

    frame = pd.DataFrame(records)
    if frame.empty:
        return frame

    dedupe_subset = [
        col for col in frame.columns if col not in {"snapshot_date", "league"}
    ]
    frame = frame.drop_duplicates(subset=dedupe_subset, keep="last")
    return frame.reset_index(drop=True)


def build_training_snapshot(
    db_path: str, output_dir: str, snapshot_date: Optional[str] = None
) -> Dict[str, Any]:
    effective_snapshot_date = (
        snapshot_date or datetime.now(timezone.utc).date().isoformat()
    )
    target_root = Path(output_dir)

    print(f"[cyan]Iniciando snapshot[/cyan] sqlite={db_path} output={output_dir}")
    bronze_df, bronze_stats = build_bronze_dataframe(db_path, effective_snapshot_date)
    print(
        "[blue]Bronze[/blue] "
        f"rows_read={bronze_stats['rows_read']} "
        f"valid={bronze_stats['rows_valid']} "
        f"invalid_json={bronze_stats['invalid_json_skipped']} "
        f"deduped={bronze_stats['rows_deduped']}"
    )

    silver_df = build_silver_dataframe(bronze_df)
    print(f"[magenta]Silver[/magenta] rows={len(silver_df)}")

    gold_df = build_gold_dataframe(silver_df)
    print(f"[green]Gold[/green] rows={len(gold_df)}")

    bronze_partitions = _write_partitioned_parquet(
        bronze_df,
        target_root / "bronze",
        partition_cols=("snapshot_date", "league"),
    )
    silver_partitions = _write_partitioned_parquet(
        silver_df,
        target_root / "silver",
        partition_cols=("snapshot_date", "league", "item_family"),
    )
    gold_partitions = _write_partitioned_parquet(
        gold_df,
        target_root / "gold",
        partition_cols=("snapshot_date", "league", "item_family"),
    )

    summary = {
        "snapshot_date": effective_snapshot_date,
        "bronze_rows": int(len(bronze_df)),
        "silver_rows": int(len(silver_df)),
        "gold_rows": int(len(gold_df)),
        "invalid_json_skipped": bronze_stats["invalid_json_skipped"],
        "bronze_partitions": bronze_partitions,
        "silver_partitions": silver_partitions,
        "gold_partitions": gold_partitions,
    }
    print(
        "[bold cyan]Snapshot concluído[/bold cyan] "
        f"bronze={summary['bronze_rows']} silver={summary['silver_rows']} gold={summary['gold_rows']}"
    )
    return summary


@app.command()
def build(
    db_path: str = typer.Option(
        "data/firehose.db", "--db-path", help="SQLite stash_events path"
    ),
    output_dir: str = typer.Option(
        "data/training_snapshots", "--output-dir", help="Output root directory"
    ),
    snapshot_date: Optional[str] = typer.Option(
        None, "--snapshot-date", help="Override snapshot date (YYYY-MM-DD)"
    ),
) -> None:
    build_training_snapshot(
        db_path=db_path,
        output_dir=output_dir,
        snapshot_date=snapshot_date,
    )


if __name__ == "__main__":
    app()
