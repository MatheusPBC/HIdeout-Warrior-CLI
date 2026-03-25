import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Set, Tuple, cast

import numpy as np
import pandas as pd
import xgboost as xgb
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.model_selection import train_test_split

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.api_integrator import MarketAPIClient
from core.item_normalizer import ITEM_FAMILIES, normalize_trade_item
from core.meta_analyzer import LadderAnalyzer, MetaScores, calculate_meta_utility_score
from core.ml_oracle import FAMILY_FEATURE_SCHEMAS, PricePredictor
from core.supabase_cloud import (
    download_file_from_supabase,
    list_artifacts_from_supabase,
    sync_file_to_supabase,
    get_artifact_checksum_info,
)
from scripts.model_registry import register_and_evaluate_candidate


class TrainingGateError(Exception):
    pass


ILVL_BANDS: Dict[str, Tuple[Optional[int], Optional[int]]] = {
    "low": (None, 74),
    "mid": (75, 83),
    "high": (84, None),
}
MIN_ROWS_PER_ILVL_BAND = 20


def classify_ilvl_band(ilvl: float) -> str:
    value = int(ilvl)
    if value <= 74:
        return "low"
    if value <= 83:
        return "mid"
    return "high"


def _extract_price_chaos(listing: dict, currency_rates: dict) -> Optional[float]:
    price_info = listing.get("price", {})
    currency = price_info.get("currency", "")
    amount = price_info.get("amount", 0.0)
    if not amount or amount <= 0:
        return None
    if currency == "chaos":
        return float(amount)

    ninja_key_map = {
        "divine": "Divine Orb",
        "exalted": "Exalted Orb",
        "mirror": "Mirror of Kalandra",
        "alch": "Orb of Alchemy",
    }
    ninja_key = ninja_key_map.get(currency, currency.title() + " Orb")
    if ninja_key in currency_rates:
        return float(amount) * float(currency_rates[ninja_key])
    if currency == "divine":
        return float(amount) * 125.0
    return None


def parse_trade_item_to_features(
    item_data: dict,
    currency_rates: dict,
    meta_scores: Optional[MetaScores] = None,
    listed_price_chaos_override: Optional[float] = None,
) -> Optional[dict]:
    listing = item_data.get("listing", {})
    listed_price = listed_price_chaos_override
    if listed_price is None:
        listed_price = _extract_price_chaos(listing, currency_rates)
    if listed_price is None:
        return None

    price_info = listing.get("price", {})
    normalized = normalize_trade_item(
        item_data,
        listed_price=listed_price,
        listing_currency=price_info.get("currency", "chaos"),
        listing_amount=float(price_info.get("amount", 0.0) or 0.0),
    )
    if normalized is None:
        return None

    predictor = PricePredictor()
    feature_frame = predictor._build_inference_dataframe(
        normalized, family=normalized.item_family
    )
    feature_row = feature_frame.iloc[0].to_dict()

    meta_utility_score = 0.0
    if meta_scores and normalized.tag_tokens:
        meta_utility_score = calculate_meta_utility_score(
            normalized.tag_tokens,
            meta_scores,
            aggregation="mean",
        )

    feature_row.update(
        {
            "item_family": normalized.item_family,
            "ilvl_band": classify_ilvl_band(normalized.ilvl),
            "base_type": normalized.base_type,
            "listed_at": normalized.listed_at,
            "price_chaos": round(normalized.listed_price, 1),
            "meta_utility_score": meta_utility_score,
        }
    )
    return feature_row


def parse_listing_timestamp(listed_at: str) -> Optional[datetime]:
    if not listed_at:
        return None
    try:
        return datetime.fromisoformat(listed_at.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def is_listing_stale(listed_at: str, hours_threshold: float = 48.0) -> bool:
    parsed_time = parse_listing_timestamp(listed_at)
    if not parsed_time:
        return False
    try:
        age = datetime.now(parsed_time.tzinfo) - parsed_time
    except TypeError:
        age = datetime.now() - parsed_time.replace(tzinfo=None)
    return age > timedelta(hours=hours_threshold)


def remove_price_outliers_iqr(
    df: pd.DataFrame,
    group_col: str = "base_type",
    price_col: str = "price_chaos",
    multiplier: float = 1.5,
) -> pd.DataFrame:
    if df.empty:
        return df

    def calculate_iqr_bounds(group: pd.Series) -> Tuple[float, float]:
        q1 = group.quantile(0.25)
        q3 = group.quantile(0.75)
        iqr = q3 - q1
        return (q1 - multiplier * iqr, q3 + multiplier * iqr)

    bounds = df.groupby(group_col)[price_col].apply(calculate_iqr_bounds).to_dict()

    def is_within_bounds(row: pd.Series) -> bool:
        if row[group_col] not in bounds:
            return True
        lower, upper = bounds[row[group_col]]
        return lower <= row[price_col] <= upper

    return df.loc[df.apply(is_within_bounds, axis=1)].copy()


def remove_stale_listings(
    df: pd.DataFrame, hours_threshold: float = 48.0, min_signal_count: int = 2
) -> pd.DataFrame:
    if df.empty or "listed_at" not in df.columns:
        return df

    signal_columns = [
        col
        for col in (
            "has_life",
            "has_resist",
            "has_spell_damage",
            "has_cast_speed",
            "has_spell_crit",
            "has_suppress",
        )
        if col in df.columns
    ]
    if not signal_columns:
        return df

    tmp = df.copy()
    tmp["signal_count"] = tmp[signal_columns].sum(axis=1)
    low_price_threshold = tmp["price_chaos"].quantile(0.25)

    def is_suspicious(row: pd.Series) -> bool:
        if not is_listing_stale(str(row.get("listed_at", "")), hours_threshold):
            return False
        return (
            row["signal_count"] >= min_signal_count
            and row["price_chaos"] <= low_price_threshold
        )

    return tmp.loc[~tmp.apply(is_suspicious, axis=1)].drop(
        columns=["signal_count"], errors="ignore"
    )


def _feature_columns(df: pd.DataFrame, target_col: str = "price_chaos") -> List[str]:
    return [col for col in df.columns if col != target_col]


def _feature_fingerprints(df: pd.DataFrame, feature_cols: List[str]) -> Set[str]:
    if df.empty or not feature_cols:
        return set()
    row_fingerprints = df[feature_cols].astype(str).agg("|".join, axis=1)
    return set(row_fingerprints.tolist())


def audit_dataset(df: pd.DataFrame, target_col: str = "price_chaos") -> Dict[str, int]:
    if df.empty:
        return {"rows": 0, "exact_duplicates": 0}

    feature_cols = _feature_columns(df, target_col=target_col)
    exact_duplicates = int(df.duplicated(subset=feature_cols + [target_col]).sum())
    return {"rows": int(len(df)), "exact_duplicates": exact_duplicates}


def run_quality_gates(
    df: pd.DataFrame,
    target_col: str = "price_chaos",
    min_rows: int = 50,
    min_unique_targets: int = 10,
    max_duplicate_ratio: float = 0.20,
    min_family_rows: int = 20,
) -> Dict[str, Any]:
    if target_col not in df.columns:
        raise TrainingGateError(
            f"Quality gate falhou: target obrigatório ausente ({target_col})."
        )

    target_series = cast(pd.Series, df[target_col])
    if target_series.isna().any():
        raise TrainingGateError("Quality gate falhou: target price_chaos contém NaN.")
    if (target_series <= 0).any():
        raise TrainingGateError(
            "Quality gate falhou: target price_chaos deve ser estritamente positivo."
        )

    rows = int(len(df))
    if rows < min_rows:
        raise TrainingGateError(
            f"Quality gate falhou: volume insuficiente ({rows} < {min_rows})."
        )

    unique_targets = int(target_series.nunique(dropna=True))
    target_std = float(np.std(target_series.to_numpy(dtype=float), ddof=0))
    if unique_targets < min_unique_targets:
        raise TrainingGateError(
            "Quality gate falhou: baixa variância do target "
            f"(nunique={unique_targets} < {min_unique_targets})."
        )
    if target_std <= 0:
        raise TrainingGateError(
            "Quality gate falhou: desvio padrão do target deve ser > 0."
        )

    audit = audit_dataset(df, target_col=target_col)
    duplicate_ratio = float(audit["exact_duplicates"]) / float(rows) if rows else 0.0
    if duplicate_ratio > max_duplicate_ratio:
        raise TrainingGateError(
            "Quality gate falhou: duplicatas exatas acima do limite "
            f"({duplicate_ratio:.1%} > {max_duplicate_ratio:.1%})."
        )

    if "item_family" not in df.columns:
        raise TrainingGateError(
            "Quality gate falhou: coluna item_family ausente para validação por família."
        )
    family_counts = df["item_family"].value_counts(dropna=False)
    max_family_count = int(family_counts.max()) if not family_counts.empty else 0
    if max_family_count < min_family_rows:
        raise TrainingGateError(
            "Quality gate falhou: nenhuma família com volume mínimo de treino "
            f"({max_family_count} < {min_family_rows})."
        )

    return {
        "rows": rows,
        "target_unique": unique_targets,
        "target_std": target_std,
        "duplicate_ratio": duplicate_ratio,
        "max_family_rows": max_family_count,
        "audit": audit,
    }


def _hash_dataframe(df: pd.DataFrame) -> str:
    if df.empty:
        return hashlib.sha256(b"empty").hexdigest()
    normalized = df.copy()
    normalized = normalized.reindex(sorted(normalized.columns), axis=1)
    payload = cast(
        str,
        normalized.to_json(orient="records", date_format="iso", default_handler=str),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _hash_file(file_path: Path) -> str:
    digest = hashlib.sha256()
    with file_path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8192), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _hash_schema(feature_schema: List[str]) -> str:
    payload = json.dumps(feature_schema, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def persist_model_metadata(
    source: str,
    league: str,
    items_per_base: int,
    trained_at_utc: str,
    dataset_df: pd.DataFrame,
    dataset_audit: Dict[str, Any],
    model_reports: List[Dict[str, Any]],
    output_dir: Path = Path("data/model_metadata"),
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    run_suffix = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    metadata_path = output_dir / f"oracle_training_{run_suffix}.json"

    snapshot_date: Optional[str] = None
    if "snapshot_date" in dataset_df.columns:
        non_null = dataset_df["snapshot_date"].dropna()
        if not non_null.empty:
            snapshot_date = str(non_null.iloc[0])

    payload = {
        "run": {
            "trained_at_utc": trained_at_utc,
            "source": source,
            "league": league,
            "items_per_base": items_per_base,
        },
        "dataset": {
            "rows": int(len(dataset_df)),
            "snapshot_date": snapshot_date,
            "dataset_hash": _hash_dataframe(dataset_df),
            "audit": dataset_audit,
        },
        "models": model_reports,
    }
    metadata_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    try:
        sync_file_to_supabase(
            metadata_path,
            artifact_type="model_metadata",
            metadata={
                "source": source,
                "league": league,
                "snapshot_date": snapshot_date or "",
            },
        )
    except Exception:
        pass
    return metadata_path


def split_dataset_for_training(
    df: pd.DataFrame,
    target_col: str = "price_chaos",
    test_size: float = 0.2,
    random_state: int = 42,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series, str]:
    if target_col not in df.columns:
        raise ValueError(f"Missing target column: {target_col}")

    if "listed_at" in df.columns:
        timestamps = pd.to_datetime(df["listed_at"], errors="coerce", utc=True)
        valid_ratio = float(timestamps.notna().mean()) if len(timestamps) else 0.0
        if timestamps.notna().sum() >= 2 and valid_ratio >= 0.8:
            sorted_df = df.loc[timestamps.notna()].copy()
            sorted_df["_listed_at_ts"] = timestamps.loc[timestamps.notna()]
            sorted_df = sorted_df.sort_values("_listed_at_ts")

            split_idx = int(len(sorted_df) * (1 - test_size))
            split_idx = min(max(split_idx, 1), len(sorted_df) - 1)
            train_df = sorted_df.iloc[:split_idx].drop(columns=["_listed_at_ts"])
            test_df = sorted_df.iloc[split_idx:].drop(columns=["_listed_at_ts"])
            return (
                train_df.drop(columns=[target_col]),
                test_df.drop(columns=[target_col]),
                train_df[target_col],
                test_df[target_col],
                "temporal",
            )

    x_df = df.drop(columns=[target_col])
    y_series = df[target_col]
    x_train, x_test, y_train, y_test = train_test_split(
        x_df,
        y_series,
        test_size=test_size,
        random_state=random_state,
    )
    return (
        cast(pd.DataFrame, x_train),
        cast(pd.DataFrame, x_test),
        cast(pd.Series, y_train),
        cast(pd.Series, y_test),
        "random",
    )


def calculate_rmse_by_bucket(
    y_true: pd.Series,
    y_pred: np.ndarray,
) -> Dict[str, Optional[float]]:
    bucket_masks = {
        "<=50": y_true <= 50,
        "50-150": (y_true > 50) & (y_true <= 150),
        ">150": y_true > 150,
    }
    bucket_rmse: Dict[str, Optional[float]] = {}
    for label, mask in bucket_masks.items():
        if int(mask.sum()) == 0:
            bucket_rmse[label] = None
            continue
        bucket_rmse[label] = float(
            np.sqrt(mean_squared_error(y_true[mask], y_pred[mask]))
        )
    return bucket_rmse


def evaluate_predictions(
    y_true: pd.Series,
    y_pred: np.ndarray,
    baseline_value: float,
) -> Dict[str, Any]:
    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    mae = float(mean_absolute_error(y_true, y_pred))
    baseline_pred = np.full(shape=len(y_true), fill_value=baseline_value, dtype=float)
    baseline_rmse = float(np.sqrt(mean_squared_error(y_true, baseline_pred)))
    return {
        "rmse": rmse,
        "mae": mae,
        "baseline_rmse": baseline_rmse,
        "rmse_by_bucket": calculate_rmse_by_bucket(y_true=y_true, y_pred=y_pred),
    }


def calculate_feature_overlap(train_x: pd.DataFrame, test_x: pd.DataFrame) -> int:
    feature_cols = list(train_x.columns)
    train_fingerprints = _feature_fingerprints(train_x, feature_cols)
    test_fingerprints = _feature_fingerprints(test_x, feature_cols)
    return int(len(train_fingerprints.intersection(test_fingerprints)))


def sample_result_ids(result_ids: List[str], items_per_base: int) -> List[str]:
    if items_per_base <= 0 or not result_ids:
        return []
    if len(result_ids) <= items_per_base:
        return list(result_ids)

    sampled: List[str] = []
    last_index = len(result_ids) - 1
    for slot in range(items_per_base):
        ratio = slot / max(items_per_base - 1, 1)
        index = round(ratio * last_index)
        sampled.append(result_ids[index])

    # Preserve order while removing duplicates that can happen due to rounding.
    return list(dict.fromkeys(sampled))


def fetch_training_data(
    target_bases: List[str],
    items_per_base: int = 500,
    league: str = "Standard",
    apply_outlier_filter: bool = True,
    apply_stale_filter: bool = True,
) -> pd.DataFrame:
    client = MarketAPIClient(league=league)
    currency_rates = client.sync_ninja_economy()
    analyzer = LadderAnalyzer(league=league)
    meta_scores = analyzer.fetch_meta_weights(force_refresh=False)

    dataset = []
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TextColumn("({task.completed}/{task.total} iter)"),
    ) as progress:
        total_iterations = len(target_bases) * ((items_per_base + 9) // 10)
        overall_task = progress.add_task(
            "[cyan]Comunicação com API GGG...", total=total_iterations
        )

        for base_type in target_bases:
            query = {
                "query": {
                    "status": {"option": "online"},
                    "type": base_type,
                    "filters": {"trade_filters": {"filters": {"price": {"min": 1}}}},
                },
                "sort": {"price": "asc"},
            }
            query_id, result_ids = client.search_items(query)
            if not query_id or not result_ids:
                continue
            result_ids = sample_result_ids(result_ids, items_per_base)

            for i in range(0, len(result_ids), 10):
                batch_ids = result_ids[i : i + 10]
                details = client.fetch_item_details(batch_ids, query_id)
                for item_json in details:
                    features = parse_trade_item_to_features(
                        item_json,
                        currency_rates,
                        meta_scores=meta_scores,
                    )
                    if features:
                        dataset.append(features)
                progress.advance(overall_task, advance=1)

    df = pd.DataFrame(dataset)
    if df.empty:
        return df
    return _apply_training_filters(
        df,
        apply_outlier_filter=apply_outlier_filter,
        apply_stale_filter=apply_stale_filter,
    )


def _apply_training_filters(
    df: pd.DataFrame,
    apply_outlier_filter: bool,
    apply_stale_filter: bool,
) -> pd.DataFrame:
    if df.empty:
        return df
    if apply_outlier_filter and len(df) > 10:
        df = remove_price_outliers_iqr(
            df, group_col="base_type", price_col="price_chaos"
        )
    if apply_stale_filter and len(df) > 10:
        df = remove_stale_listings(df, hours_threshold=48.0, min_signal_count=2)
    return df


def fetch_latest_snapshot_from_cloud(
    local_dest_dir: Path,
    artifact_type: str = "snapshot_gold",
    snapshot_date: Optional[str] = None,
) -> tuple[Optional[Path], Optional[dict]]:
    """Baixa snapshot mais recente do Supabase Storage.

    Args:
        local_dest_dir: diretório local para salvar o snapshot
        artifact_type: tipo de artefato (snapshot_gold, snapshot_silver, snapshot_bronze)
        snapshot_date: se fornecido, baixa snapshot específico; senão, pega o mais recente

    Returns:
        Tuple of (Path do diretório baixado ou None, dict com info de integridade ou None)
    """
    artifacts = list_artifacts_from_supabase(artifact_type)
    if not artifacts:
        return None, None

    if snapshot_date:
        filtered = [
            a
            for a in artifacts
            if a.get("metadata", {}).get("snapshot_date") == snapshot_date
        ]
        if not filtered:
            return None, None
        candidates = filtered
    else:
        candidates = artifacts

    if not candidates:
        return None, None

    candidates.sort(
        key=lambda a: a.get("object_path", ""),
        reverse=True,
    )
    chosen = candidates[0]
    object_path = chosen.get("object_path", "")
    if not object_path:
        return None, None

    # Get artifact key and check for checksum info (legacy vs validated)
    artifact_key = chosen.get("artifact_key")
    integrity_info = None
    expected_sha256 = None

    if artifact_key:
        integrity_info = get_artifact_checksum_info(artifact_key)
        if integrity_info and integrity_info.stored_sha256:
            expected_sha256 = integrity_info.stored_sha256

    local_dest_dir.mkdir(parents=True, exist_ok=True)

    gold_dir = local_dest_dir / "gold"
    gold_dir.mkdir(parents=True, exist_ok=True)

    cloud_path = object_path.replace(f"hideout-warrior/{artifact_type}/", "")
    local_file = gold_dir / Path(cloud_path).name

    from core.cloud_download import download_file_from_supabase as cloud_download

    download_result = cloud_download(
        remote_path=cloud_path,
        local_destination=local_file,
        artifact_type=artifact_type,
        expected_sha256=expected_sha256,
        validate_checksum=bool(expected_sha256),  # Only validate if we have checksum
    )

    # Build integrity info for caller
    result_integrity = {
        "artifact_key": artifact_key,
        "object_path": object_path,
        "checksum_validated": download_result.checksum_validated,
        "is_legacy": download_result.is_legacy,
        "expected_sha256": download_result.expected_sha256,
        "actual_sha256": download_result.actual_sha256,
        "error_message": download_result.error_message,
    }

    if download_result.success:
        return gold_dir, result_integrity
    return None, result_integrity


def _trade_item_from_firehose_row(row: Mapping[str, Any]) -> Optional[dict]:
    raw_payload = row["raw_item_json"]
    if not raw_payload:
        return None
    try:
        item_payload = json.loads(raw_payload)
    except (TypeError, json.JSONDecodeError):
        return None
    if not isinstance(item_payload, dict):
        return None

    listing_currency = str(row["price_currency"] or "chaos")
    listing_amount = float(row["price_amount"] or row["price_chaos"] or 0.0)
    seller = str(row["account_name"] or item_payload.get("accountName") or "")
    listed_at = str(row["indexed"] or item_payload.get("indexed") or "")

    return {
        "listing": {
            "whisper": "@placeholder hi, I'd like to buy your item",
            "indexed": listed_at,
            "account": {"name": seller},
            "price": {
                "currency": listing_currency,
                "amount": listing_amount,
            },
        },
        "item": item_payload,
    }


def fetch_training_data_from_sqlite(
    db_path: str,
    apply_outlier_filter: bool = True,
    apply_stale_filter: bool = True,
) -> pd.DataFrame:
    def _table_exists(conn_ref: sqlite3.Connection, table_name: str) -> bool:
        row = conn_ref.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
            (table_name,),
        ).fetchone()
        return row is not None

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    dataset: List[dict] = []
    try:
        table_queries: List[str] = []
        if _table_exists(conn, "stash_events"):
            table_queries.append(
                """
                SELECT raw_item_json, price_chaos, price_currency, price_amount, account_name, indexed
                FROM stash_events
                WHERE price_chaos > 0
                """
            )
        if _table_exists(conn, "trade_bucket_events"):
            table_queries.append(
                """
                SELECT raw_item_json, price_chaos, price_currency, price_amount, account_name, indexed
                FROM trade_bucket_events
                WHERE price_chaos > 0
                """
            )

        if not table_queries:
            return pd.DataFrame(dataset)

        union_query = " UNION ALL ".join(table_queries)
        rows = conn.execute(union_query).fetchall()
        for row in rows:
            item_data = _trade_item_from_firehose_row(row)
            if item_data is None:
                continue
            features = parse_trade_item_to_features(
                item_data,
                currency_rates={},
                meta_scores=None,
                listed_price_chaos_override=float(row["price_chaos"]),
            )
            if features:
                dataset.append(features)
    finally:
        conn.close()

    return _apply_training_filters(
        pd.DataFrame(dataset),
        apply_outlier_filter=apply_outlier_filter,
        apply_stale_filter=apply_stale_filter,
    )


def fetch_training_data_from_parquet(
    parquet_path: str,
    apply_outlier_filter: bool = True,
    apply_stale_filter: bool = True,
) -> pd.DataFrame:
    parquet_target = Path(parquet_path)
    if not parquet_target.exists():
        # Cloud-first: try to download from Supabase
        from core.cloud_download import download_directory_from_supabase
        from core.cloud_config import load_cloud_config

        config = load_cloud_config()
        if config.is_configured:
            print(
                f"[cyan]Parquet não encontrado localmente, tentando download do cloud: {parquet_path}[/cyan]"
            )
            parent_dir = parquet_target.parent
            parent_dir.mkdir(parents=True, exist_ok=True)

            downloaded = download_directory_from_supabase(
                artifact_type="training_snapshots",
                prefix=str(parquet_target),
                output_dir=parent_dir,
                config=config,
            )
            if not downloaded:
                raise FileNotFoundError(
                    f"Caminho parquet não encontrado localmente e download do cloud falhou: {parquet_path}"
                )
        else:
            raise FileNotFoundError(f"Caminho parquet não encontrado: {parquet_path}")

    try:
        frame = pd.read_parquet(str(parquet_target))
    except ImportError as exc:
        raise RuntimeError(
            "Leitura de Parquet requer engine instalada (pyarrow ou fastparquet)."
        ) from exc
    except Exception as exc:
        source_kind = "diretório" if parquet_target.is_dir() else "arquivo"
        raise RuntimeError(
            f"Falha ao carregar dataset parquet ({source_kind}): {parquet_path}"
        ) from exc

    if frame.empty:
        return frame

    if "price_chaos" in frame.columns and "item_family" in frame.columns:
        return _apply_training_filters(
            frame.copy(),
            apply_outlier_filter=apply_outlier_filter,
            apply_stale_filter=apply_stale_filter,
        )

    required_raw_columns = {"raw_item_json", "price_chaos"}
    if not required_raw_columns.issubset(set(frame.columns)):
        raise ValueError(
            "Dataset parquet sem colunas suficientes. Esperado features prontas ou raw_item_json + price_chaos."
        )

    dataset: List[dict] = []
    for _, row in frame.iterrows():
        item_payload = row.get("raw_item_json")
        if not item_payload:
            continue
        if isinstance(item_payload, str):
            try:
                parsed_payload = json.loads(item_payload)
            except json.JSONDecodeError:
                continue
        elif isinstance(item_payload, dict):
            parsed_payload = item_payload
        else:
            continue

        item_data = {
            "listing": {
                "whisper": "@placeholder hi, I'd like to buy your item",
                "indexed": row.get("indexed", ""),
                "account": {"name": row.get("account_name", "")},
                "price": {
                    "currency": row.get("price_currency", "chaos"),
                    "amount": float(
                        row.get("price_amount", row.get("price_chaos", 0.0)) or 0.0
                    ),
                },
            },
            "item": parsed_payload,
        }
        features = parse_trade_item_to_features(
            item_data,
            currency_rates={},
            meta_scores=None,
            listed_price_chaos_override=float(row.get("price_chaos", 0.0) or 0.0),
        )
        if features:
            dataset.append(features)

    return _apply_training_filters(
        pd.DataFrame(dataset),
        apply_outlier_filter=apply_outlier_filter,
        apply_stale_filter=apply_stale_filter,
    )


def load_training_dataframe(
    source: str,
    league: str,
    items_per_base: int,
    target_bases: List[str],
    sqlite_path: str,
    parquet_path: str,
) -> pd.DataFrame:
    if source == "api":
        return fetch_training_data(
            target_bases,
            items_per_base=items_per_base,
            league=league,
            apply_outlier_filter=True,
            apply_stale_filter=True,
        )
    if source == "sqlite":
        return fetch_training_data_from_sqlite(
            sqlite_path,
            apply_outlier_filter=True,
            apply_stale_filter=True,
        )
    if source == "parquet":
        return fetch_training_data_from_parquet(
            parquet_path,
            apply_outlier_filter=True,
            apply_stale_filter=True,
        )
    raise ValueError(f"Fonte de treino inválida: {source}")


def _family_feature_columns(family: str) -> List[str]:
    schema = list(FAMILY_FEATURE_SCHEMAS.get(family, FAMILY_FEATURE_SCHEMAS["generic"]))
    if "meta_utility_score" not in schema:
        schema.append("meta_utility_score")
    return schema


def _train_family_model(
    family: str,
    family_df: pd.DataFrame,
    output_dir: Path,
    model_suffix: str = "",
    report_context: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    if len(family_df) < 20:
        return None

    feature_columns = _family_feature_columns(family)
    context_columns = [
        column
        for column in ("listed_at", "base_type", "item_family")
        if column in family_df.columns
    ]
    available_feature_columns = [
        column for column in feature_columns if column in family_df.columns
    ]
    training_columns = available_feature_columns + context_columns + ["price_chaos"]
    training_df = family_df.loc[:, training_columns].copy()

    x_train, x_test, y_train, y_test, split_strategy = split_dataset_for_training(
        training_df
    )
    leakage_columns = [
        col
        for col in ("listed_at", "base_type", "item_family")
        if col in x_train.columns
    ]
    x_train = x_train.drop(columns=leakage_columns, errors="ignore")
    x_test = x_test.drop(columns=leakage_columns, errors="ignore")
    for column in feature_columns:
        if column not in x_train.columns:
            x_train[column] = 0.0
        if column not in x_test.columns:
            x_test[column] = 0.0
    x_train = x_train.reindex(columns=feature_columns, fill_value=0.0)
    x_test = x_test.reindex(columns=feature_columns, fill_value=0.0)

    dtrain = xgb.DMatrix(x_train, label=y_train)
    dtest = xgb.DMatrix(x_test, label=y_test)
    model = xgb.train(
        {
            "max_depth": 5,
            "eta": 0.05,
            "objective": "reg:squarederror",
            "eval_metric": "rmse",
        },
        dtrain,
        num_boost_round=120,
        evals=[(dtrain, "train"), (dtest, "eval")],
        early_stopping_rounds=12,
        verbose_eval=False,
    )
    preds = model.predict(dtest)
    metrics = evaluate_predictions(
        y_test, preds, baseline_value=float(y_train.median())
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    model_path = output_dir / f"price_oracle_{family}{model_suffix}.xgb"
    model.save_model(model_path)
    report = {
        "family": family,
        "rows_total": int(len(family_df)),
        "rows_train": int(len(x_train)),
        "rows_test": int(len(x_test)),
        "feature_schema": feature_columns,
        "feature_schema_hash": _hash_schema(feature_columns),
        "split_strategy": split_strategy,
        "metrics": metrics,
        "model_path": str(model_path),
        "model_sha256": _hash_file(model_path),
    }
    if report_context:
        report.update(report_context)
    return report


def _subset_by_ilvl_band(df: pd.DataFrame, band: str) -> pd.DataFrame:
    min_ilvl, max_ilvl = ILVL_BANDS[band]
    subset = df
    if min_ilvl is not None:
        subset = subset.loc[subset["ilvl"] >= min_ilvl]
    if max_ilvl is not None:
        subset = subset.loc[subset["ilvl"] <= max_ilvl]
    return subset.copy()


def _train_family_band_models(
    family: str,
    family_df: pd.DataFrame,
    output_dir: Path,
    min_rows_per_band: int = MIN_ROWS_PER_ILVL_BAND,
) -> List[Dict[str, Any]]:
    if family_df.empty or "ilvl" not in family_df.columns:
        return []

    reports: List[Dict[str, Any]] = []
    for band in ILVL_BANDS:
        band_df = _subset_by_ilvl_band(family_df, band)
        band_rows = int(len(band_df))
        if band_rows < min_rows_per_band:
            reports.append(
                {
                    "family": family,
                    "ilvl_band": band,
                    "rows_total": band_rows,
                    "trained": False,
                    "fallback_to_family": True,
                    "fallback_reason": "insufficient_band_rows",
                    "min_rows_per_band": min_rows_per_band,
                }
            )
            continue

        report = _train_family_model(
            family,
            band_df,
            output_dir,
            model_suffix=f"__{band}",
            report_context={
                "ilvl_band": band,
                "trained": True,
                "fallback_to_family": False,
            },
        )
        if report is None:
            reports.append(
                {
                    "family": family,
                    "ilvl_band": band,
                    "rows_total": band_rows,
                    "trained": False,
                    "fallback_to_family": True,
                    "fallback_reason": "band_training_failed",
                    "min_rows_per_band": min_rows_per_band,
                }
            )
            continue
        reports.append(report)

    return reports


def train_xgboost_oracle(
    league: str = "Standard",
    items_per_base: int = 500,
    source: str = "api",
    sqlite_path: str = "data/firehose.db",
    parquet_path: str = "data/training_snapshots/gold",
    promotion_max_rmse_ratio: float = 1.0,
    promotion_min_abs_improvement: float = 0.0,
    registry_path: str = "data/model_registry/registry.json",
) -> None:
    print("[Training] Iniciando treino por família")
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    trained_at_utc = (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )
    target_bases = [
        "Imbued Wand",
        "Titanium Spirit Shield",
        "Vaal Regalia",
        "Sadist Garb",
        "Large Cluster Jewel",
        "Opal Ring",
    ]
    df = load_training_dataframe(
        source=source,
        league=league,
        items_per_base=items_per_base,
        target_bases=target_bases,
        sqlite_path=sqlite_path,
        parquet_path=parquet_path,
    )
    try:
        gate_report = run_quality_gates(df)
    except TrainingGateError as exc:
        print(f"[Training][Abort] {exc}")
        sys.exit(1)

    audit = gate_report["audit"]
    print(
        "[Audit] "
        f"rows={audit['rows']} "
        f"exact_duplicates={audit['exact_duplicates']} "
        f"duplicate_ratio={gate_report['duplicate_ratio']:.1%}"
    )

    output_dir = Path("data")
    trained_reports: List[Dict[str, Any]] = []
    for family in ITEM_FAMILIES:
        family_df = df.loc[df.get("item_family") == family].copy()
        report = _train_family_model(family, family_df, output_dir)
        if report is not None:
            report["ilvl_band_models"] = _train_family_band_models(
                family,
                family_df,
                output_dir,
            )
            trained_reports.append(report)

    if not trained_reports:
        print("Nenhuma família teve dados suficientes para treino.")
        sys.exit(1)

    for report in trained_reports:
        metrics = report["metrics"]
        print(
            f"[Family {report['family']}] rows={report['rows_total']} rmse={metrics['rmse']:.2f} mae={metrics['mae']:.2f} model={report['model_path']}"
        )

    for report in trained_reports:
        decision = register_and_evaluate_candidate(
            family=str(report["family"]),
            run_id=run_id,
            model_path=str(report["model_path"]),
            model_sha256=str(report["model_sha256"]),
            metrics=cast(Dict[str, Any], report.get("metrics", {})),
            max_rmse_ratio=promotion_max_rmse_ratio,
            min_abs_improvement=promotion_min_abs_improvement,
            registry_path=Path(registry_path),
        )
        report["registry_decision"] = decision
        try:
            sync_file_to_supabase(
                Path(str(report["model_path"])),
                artifact_type="trained_model",
                metadata={"family": str(report["family"]), "run_id": run_id},
            )
        except Exception:
            pass

    metadata_path = persist_model_metadata(
        source=source,
        league=league,
        items_per_base=items_per_base,
        trained_at_utc=trained_at_utc,
        dataset_df=df,
        dataset_audit={
            **audit,
            "duplicate_ratio": gate_report["duplicate_ratio"],
            "target_unique": gate_report["target_unique"],
            "target_std": gate_report["target_std"],
            "max_family_rows": gate_report["max_family_rows"],
        },
        model_reports=trained_reports,
    )
    print(f"[Metadata] Salvo em {metadata_path}")


if __name__ == "__main__":
    import typer

    app = typer.Typer()

    @app.command()
    def train(
        league: str = typer.Option("Standard", "--league", "-l", help="PoE league"),
        items_per_base: int = typer.Option(
            500, "--items", "-i", help="Items per base type"
        ),
        source: str = typer.Option(
            "api", "--source", help="Fonte do dataset: api|sqlite|parquet"
        ),
        sqlite_path: str = typer.Option(
            "data/firehose.db", "--sqlite-path", help="SQLite source path"
        ),
        parquet_path: str = typer.Option(
            "data/training_snapshots/gold",
            "--parquet-path",
            help="Parquet source path (file or partitioned directory)",
        ),
        promotion_max_rmse_ratio: float = typer.Option(
            1.0,
            "--promotion-max-rmse-ratio",
            help="Registry promotion policy: max RMSE ratio vs baseline",
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
        cloud: bool = typer.Option(
            False,
            "--cloud",
            help="Baixar snapshot mais recente do Supabase Storage antes de treinar",
        ),
        cloud_snapshot_date: Optional[str] = typer.Option(
            None,
            "--cloud-snapshot-date",
            help="Data específica do snapshot cloud (YYYY-MM-DD). Se omitido, usa o mais recente.",
        ),
    ):
        effective_parquet_path = parquet_path
        if cloud:
            print(
                "[cyan]Modo cloud ativado: buscando snapshot do Supabase Storage...[/cyan]"
            )
            from core.cloud_config import load_cloud_config

            cfg = load_cloud_config()
            if cfg.enabled and cfg.is_configured:
                downloaded, integrity_info = fetch_latest_snapshot_from_cloud(
                    local_dest_dir=Path("data/training_snapshots"),
                    artifact_type="snapshot_gold",
                    snapshot_date=cloud_snapshot_date,
                )
                if downloaded:
                    effective_parquet_path = str(downloaded)
                    print(
                        f"[green]Snapshot cloud baixado em: {effective_parquet_path}[/green]"
                    )
                    # Log integrity status (legacy vs validated)
                    if integrity_info:
                        if integrity_info.get("is_legacy"):
                            print(
                                f"[yellow]  Legacy artifact (sem checksum validado)[/yellow]"
                            )
                        elif integrity_info.get("checksum_validated"):
                            print(
                                f"[green]  Checksum validado: {integrity_info.get('actual_sha256', 'N/A')[:16]}...[/green]"
                            )
                else:
                    print(
                        "[yellow]Nenhum snapshot encontrado no cloud; usando fonte local.[/yellow]"
                    )
            else:
                print(
                    "[yellow]Cloud backend não configurado; usando fonte local.[/yellow]"
                )

        train_xgboost_oracle(
            league=league,
            items_per_base=items_per_base,
            source=source,
            sqlite_path=sqlite_path,
            parquet_path=effective_parquet_path,
            promotion_max_rmse_ratio=promotion_max_rmse_ratio,
            promotion_min_abs_improvement=promotion_min_abs_improvement,
            registry_path=registry_path,
        )

    app()
