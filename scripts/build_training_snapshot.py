import os
import sys
import hashlib
import json
import sqlite3
import time as time_module
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from uuid import uuid4

import pandas as pd
import typer
from rich import print

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.item_normalizer import normalize_trade_item
from core.ops_metrics import emit_snapshot_metrics
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
            str(row.get("account_name", "")),
            str(row.get("change_id", "")),
            str(row.get("query_id", "")),
            str(row.get("item_id", "")),
            str(row.get("base_type", "")),
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


def _parse_timestamp(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def _timestamp_to_iso(value: Optional[datetime]) -> str:
    if value is None:
        return ""
    return (
        value.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _calculate_listing_age_seconds(
    indexed_at: Any, collected_at: Any
) -> Optional[float]:
    indexed_dt = _parse_timestamp(indexed_at)
    collected_dt = _parse_timestamp(collected_at)
    if indexed_dt is None or collected_dt is None:
        return None
    age_seconds = (collected_dt - indexed_dt).total_seconds()
    if age_seconds < 0:
        return 0.0
    return round(age_seconds, 3)


def _classify_freshness_band(listing_age_seconds: Any) -> str:
    if listing_age_seconds is None:
        return "unknown"
    try:
        age_seconds = float(listing_age_seconds)
    except (TypeError, ValueError):
        return "unknown"
    if age_seconds <= 900:
        return "fresh"
    if age_seconds <= 21600:
        return "active"
    if age_seconds <= 172800:
        return "aging"
    return "stale"


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    return {
        str(row[1])
        for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    }


def _select_sql_expr(
    available_columns: set[str],
    column_name: str,
    *,
    default_sql: str,
    alias: Optional[str] = None,
) -> str:
    target_alias = alias or column_name
    if column_name in available_columns:
        if target_alias == column_name:
            return column_name
        return f"{column_name} AS {target_alias}"
    return f"{default_sql} AS {target_alias}"


def _build_query_context(row_map: Dict[str, Any]) -> str:
    context: Dict[str, Any] = {
        "source_table": str(row_map.get("source_table") or "unknown"),
    }
    if row_map.get("source_table") == "stash_events":
        context.update(
            {
                "change_id": str(row_map.get("change_id") or ""),
                "stash_name": str(row_map.get("stash_name") or ""),
                "oauth_source": str(row_map.get("oauth_source") or ""),
                "oauth_scope": str(row_map.get("oauth_scope") or ""),
            }
        )
    else:
        context.update(
            {
                "query_id": str(row_map.get("query_id") or ""),
                "base_type": str(row_map.get("base_type") or ""),
                "bucket_min": int(row_map.get("bucket_min") or 0),
                "bucket_max": int(row_map.get("bucket_max") or 0),
                "bucket_label": str(row_map.get("bucket_label") or ""),
                "scan_profile": str(row_map.get("scan_profile") or ""),
                "query_shape": str(row_map.get("query_shape") or ""),
                "search_batch": int(row_map.get("search_batch") or 0),
                "fetch_batch": int(row_map.get("fetch_batch") or 0),
            }
        )
    return json.dumps(context, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def _aggregate_bronze_metrics(
    frame: pd.DataFrame, bronze_stats: Dict[str, int]
) -> Dict[str, Any]:
    """Agrega métricas descritivas da camada Bronze."""
    if frame.empty:
        return {
            "source_distribution": {},
            "freshness_distribution": {},
            "dedup_rate": 0.0,
        }

    total_rows = len(frame)
    dedup_rate = (
        bronze_stats["rows_deduped"] / float(bronze_stats["rows_read"])
        if bronze_stats["rows_read"] > 0
        else 0.0
    )

    source_dist: Dict[str, int] = {}
    if "source" in frame.columns:
        source_dist = frame["source"].value_counts(dropna=False).to_dict()
        source_dist = {str(k): int(v) for k, v in source_dist.items()}

    freshness_dist: Dict[str, int] = {}
    if "freshness_band" in frame.columns:
        freshness_dist = frame["freshness_band"].value_counts(dropna=False).to_dict()
        freshness_dist = {str(k): int(v) for k, v in freshness_dist.items()}

    return {
        "source_distribution": source_dist,
        "freshness_distribution": freshness_dist,
        "dedup_rate": round(dedup_rate, 4),
    }


def _aggregate_silver_metrics(
    bronze_rows: int, silver_rows: int, normalization_failures: int
) -> Dict[str, Any]:
    """Agrega métricas descritivas da camada Silver."""
    return {
        "rows_input": bronze_rows,
        "rows_output": silver_rows,
        "normalization_failures": normalization_failures,
    }


def _aggregate_gold_metrics(
    silver_rows: int, gold_rows: int, feature_extraction_failures: int
) -> Dict[str, Any]:
    """Agrega métricas descritivas da camada Gold."""
    return {
        "rows_input": silver_rows,
        "rows_output": gold_rows,
        "feature_extraction_failures": feature_extraction_failures,
    }


def _enrich_bronze_observations(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame

    enriched = frame.copy()
    enriched["indexed_at"] = enriched["indexed_at"].fillna(enriched["indexed"])
    enriched["indexed"] = enriched["indexed_at"]
    enriched["collected_at"] = enriched["collected_at"].fillna(enriched["indexed_at"])
    enriched["_indexed_dt"] = pd.to_datetime(
        enriched["indexed_at"], errors="coerce", utc=True
    )
    enriched["_collected_dt"] = pd.to_datetime(
        enriched["collected_at"], errors="coerce", utc=True
    )
    enriched["_observed_dt"] = enriched["_indexed_dt"].fillna(enriched["_collected_dt"])

    enriched["first_seen_at"] = enriched["indexed_at"].fillna("")
    enriched["last_seen_at"] = enriched["indexed_at"].fillna("")
    enriched["seen_count"] = 1
    enriched["source_count"] = 1
    enriched["price_fix_suspected"] = False

    valid_item_mask = enriched["item_id"].astype(str).str.len() > 0
    if valid_item_mask.any():
        grouped = enriched.loc[valid_item_mask].groupby(
            ["league", "item_id"], dropna=False
        )
        first_seen = grouped["_observed_dt"].transform("min")
        last_seen = grouped["_observed_dt"].transform("max")
        seen_count = grouped["event_key"].transform("size")
        source_count = grouped["source"].transform("nunique")

        enriched.loc[valid_item_mask, "first_seen_at"] = [
            _timestamp_to_iso(
                value.to_pydatetime() if hasattr(value, "to_pydatetime") else value
            )
            if pd.notna(value)
            else ""
            for value in first_seen
        ]
        enriched.loc[valid_item_mask, "last_seen_at"] = [
            _timestamp_to_iso(
                value.to_pydatetime() if hasattr(value, "to_pydatetime") else value
            )
            if pd.notna(value)
            else ""
            for value in last_seen
        ]
        enriched.loc[valid_item_mask, "seen_count"] = seen_count.astype(int)
        enriched.loc[valid_item_mask, "source_count"] = source_count.astype(int)
        enriched.loc[valid_item_mask & (enriched["source_count"] > 1), "source"] = (
            "both"
        )

        price_min = grouped["price_chaos"].transform("min")
        price_max = grouped["price_chaos"].transform("max")
        has_price_anomaly = (
            (price_max > 0) & (price_min > 0) & (price_max > 1.5 * price_min)
        )
        enriched.loc[valid_item_mask, "price_fix_suspected"] = has_price_anomaly

    enriched["listing_age_seconds"] = [
        _calculate_listing_age_seconds(indexed_at, collected_at)
        for indexed_at, collected_at in zip(
            enriched["indexed_at"], enriched["collected_at"]
        )
    ]
    enriched["freshness_band"] = enriched["listing_age_seconds"].map(
        _classify_freshness_band
    )
    enriched["snapshot_date"] = [
        _normalize_snapshot_date(indexed_at, str(snapshot_date))
        for indexed_at, snapshot_date in zip(
            enriched["indexed_at"], enriched["snapshot_date"]
        )
    ]

    return enriched.drop(
        columns=["_indexed_dt", "_collected_dt", "_observed_dt"], errors="ignore"
    )


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
            available_columns = _table_columns(conn, table_name)
            return conn.execute(
                f"""
                SELECT
                    change_id,
                    '' AS query_id,
                    item_id,
                    league,
                    account_name,
                    {_select_sql_expr(available_columns, "stash_name", default_sql="''")},
                    indexed,
                    price_amount,
                    price_currency,
                    price_chaos,
                    raw_item_json,
                    {_select_sql_expr(available_columns, "collected_at", default_sql="indexed")},
                    {_select_sql_expr(available_columns, "oauth_source", default_sql="''")},
                    {_select_sql_expr(available_columns, "oauth_scope", default_sql="''")},
                    '' AS base_type,
                    0 AS bucket_min,
                    0 AS bucket_max,
                    '' AS bucket_label,
                    '' AS scan_profile,
                    '' AS query_shape,
                    0 AS search_batch,
                    0 AS fetch_batch,
                    'stash_events' AS source_table
                FROM stash_events
                """
            ).fetchall()
        available_columns = _table_columns(conn, table_name)
        return conn.execute(
            f"""
            SELECT
                '' AS change_id,
                query_id,
                item_id,
                league,
                account_name,
                '' AS stash_name,
                indexed,
                price_amount,
                price_currency,
                price_chaos,
                raw_item_json,
                {_select_sql_expr(available_columns, "collected_at", default_sql="indexed")},
                '' AS oauth_source,
                '' AS oauth_scope,
                {_select_sql_expr(available_columns, "base_type", default_sql="''")},
                {_select_sql_expr(available_columns, "bucket_min", default_sql="0")},
                {_select_sql_expr(available_columns, "bucket_max", default_sql="0")},
                {_select_sql_expr(available_columns, "bucket_label", default_sql="''")},
                {_select_sql_expr(available_columns, "scan_profile", default_sql="''")},
                {_select_sql_expr(available_columns, "query_shape", default_sql="''")},
                {_select_sql_expr(available_columns, "search_batch", default_sql="0")},
                {_select_sql_expr(available_columns, "fetch_batch", default_sql="0")},
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
            source_name = (
                "stash"
                if str(row_map.get("source_table") or "") == "stash_events"
                else "trade"
            )
            indexed_at = str(row_map.get("indexed") or parsed_item.get("indexed") or "")
            collected_at = str(row_map.get("collected_at") or indexed_at)
            record = {
                "event_key": _stable_event_key(row_map, normalized_raw_json),
                "change_id": str(row_map.get("change_id") or ""),
                "query_id": str(row_map.get("query_id") or ""),
                "item_id": str(row_map.get("item_id") or parsed_item.get("id") or ""),
                "league": str(row_map.get("league") or "Unknown"),
                "account_name": str(row_map.get("account_name") or ""),
                "stash_name": str(row_map.get("stash_name") or ""),
                "source_table": str(row_map.get("source_table") or "unknown"),
                "source": source_name,
                "indexed": indexed_at,
                "indexed_at": indexed_at,
                "collected_at": collected_at,
                "oauth_source": str(row_map.get("oauth_source") or ""),
                "oauth_scope": str(row_map.get("oauth_scope") or ""),
                "base_type": str(
                    row_map.get("base_type") or parsed_item.get("baseType") or ""
                ),
                "bucket_min": int(row_map.get("bucket_min") or 0),
                "bucket_max": int(row_map.get("bucket_max") or 0),
                "bucket_label": str(row_map.get("bucket_label") or ""),
                "scan_profile": str(row_map.get("scan_profile") or ""),
                "query_shape": str(row_map.get("query_shape") or ""),
                "search_batch": int(row_map.get("search_batch") or 0),
                "fetch_batch": int(row_map.get("fetch_batch") or 0),
                "price_amount": float(
                    row_map.get("price_amount") or price_chaos or 0.0
                ),
                "price_currency": str(row_map.get("price_currency") or "chaos"),
                "price_chaos": price_chaos,
                "raw_item_json": normalized_raw_json,
                "query_context": _build_query_context(row_map),
                "snapshot_date": _normalize_snapshot_date(
                    indexed_at or collected_at, default_date=snapshot_date
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
    return _enrich_bronze_observations(frame), stats


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
                "indexed_at": row_map.get("indexed_at", row_map.get("indexed", "")),
                "collected_at": row_map.get("collected_at", ""),
                "raw_item_json": row_map.get("raw_item_json", ""),
                "price_chaos": float(row_map.get("price_chaos", 0.0) or 0.0),
                "source": row_map.get("source", "unknown"),
                "source_count": int(row_map.get("source_count", 1) or 1),
                "seen_count": int(row_map.get("seen_count", 1) or 1),
                "first_seen_at": row_map.get("first_seen_at", ""),
                "last_seen_at": row_map.get("last_seen_at", ""),
                "listing_age_seconds": row_map.get("listing_age_seconds"),
                "freshness_band": row_map.get("freshness_band", "unknown"),
                "query_context": row_map.get("query_context", "{}"),
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
    feature_extraction_failures = 0
    for _, row in silver_df.iterrows():
        row_map = row.to_dict()
        raw_payload = _safe_json_load(row_map.get("raw_item_json"))
        if raw_payload is None:
            feature_extraction_failures += 1
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
            feature_extraction_failures += 1
            continue

        features["league"] = str(row_map.get("league") or "Unknown")
        features["snapshot_date"] = row_map.get("snapshot_date")
        features["event_key"] = str(row_map.get("event_key") or "")
        features["item_family"] = str(
            features.get("item_family") or row_map.get("item_family") or "generic"
        )
        features["source"] = str(row_map.get("source") or "unknown")
        features["source_count"] = int(row_map.get("source_count") or 1)
        features["seen_count"] = int(row_map.get("seen_count") or 1)
        features["first_seen_at"] = str(row_map.get("first_seen_at") or "")
        features["last_seen_at"] = str(row_map.get("last_seen_at") or "")
        features["listing_age_seconds"] = row_map.get("listing_age_seconds")
        features["freshness_band"] = str(row_map.get("freshness_band") or "unknown")
        features["query_context"] = str(row_map.get("query_context") or "{}")
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

    normalization_failures = max(0, bronze_stats["rows_valid"] - len(silver_df))
    feature_extraction_failures = max(0, len(silver_df) - len(gold_df))

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

    bronze_aggregated = _aggregate_bronze_metrics(bronze_df, bronze_stats)
    silver_aggregated = _aggregate_silver_metrics(
        bronze_stats["rows_valid"], len(silver_df), normalization_failures
    )
    gold_aggregated = _aggregate_gold_metrics(
        len(silver_df), len(gold_df), feature_extraction_failures
    )

    summary = {
        "snapshot_date": effective_snapshot_date,
        "bronze": {
            "rows": int(len(bronze_df)),
            "rows_read": bronze_stats["rows_read"],
            "rows_valid": bronze_stats["rows_valid"],
            "rows_deduped": bronze_stats["rows_deduped"],
            "invalid_json_skipped": bronze_stats["invalid_json_skipped"],
            "partitions": bronze_partitions,
            "source_distribution": bronze_aggregated["source_distribution"],
            "freshness_distribution": bronze_aggregated["freshness_distribution"],
            "dedup_rate": bronze_aggregated["dedup_rate"],
        },
        "silver": {
            "rows": int(len(silver_df)),
            "rows_input": silver_aggregated["rows_input"],
            "rows_output": silver_aggregated["rows_output"],
            "normalization_failures": silver_aggregated["normalization_failures"],
            "partitions": silver_partitions,
        },
        "gold": {
            "rows": int(len(gold_df)),
            "rows_input": gold_aggregated["rows_input"],
            "rows_output": gold_aggregated["rows_output"],
            "feature_extraction_failures": gold_aggregated[
                "feature_extraction_failures"
            ],
            "partitions": gold_partitions,
        },
        "invalid_json_skipped": bronze_stats["invalid_json_skipped"],
        "bronze_rows": int(len(bronze_df)),
        "silver_rows": int(len(silver_df)),
        "gold_rows": int(len(gold_df)),
    }
    print(
        "[bold cyan]Snapshot concluído[/bold cyan] "
        f"bronze={summary['bronze']['rows']} silver={summary['silver']['rows']} gold={summary['gold']['rows']}"
    )

    snapshot_metrics_path = emit_snapshot_metrics(
        snapshot_summary=summary,
        run_id=f"snapshot_{effective_snapshot_date}",
    )
    print(f"[dim]Métricas de snapshot salvas em {snapshot_metrics_path}[/dim]")
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
