import os
import sqlite3
import sys
from datetime import datetime, timedelta
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, cast

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


def _trade_item_from_firehose_row(row: sqlite3.Row) -> Optional[dict]:
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
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    dataset: List[dict] = []
    try:
        rows = conn.execute(
            """
            SELECT raw_item_json, price_chaos, price_currency, price_amount, account_name, indexed
            FROM stash_events
            WHERE price_chaos > 0
            """
        ).fetchall()
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
    try:
        frame = pd.read_parquet(parquet_path)
    except ImportError as exc:
        raise RuntimeError(
            "Leitura de Parquet requer engine instalada (pyarrow ou fastparquet)."
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
    model_path = output_dir / f"price_oracle_{family}.xgb"
    model.save_model(model_path)
    return {
        "family": family,
        "rows": len(family_df),
        "split_strategy": split_strategy,
        "metrics": metrics,
        "model_path": str(model_path),
    }


def train_xgboost_oracle(
    league: str = "Standard",
    items_per_base: int = 500,
    source: str = "api",
    sqlite_path: str = "data/firehose.db",
    parquet_path: str = "data/firehose.parquet",
) -> None:
    print("[Training] Iniciando treino por família")
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
    if len(df) < 50:
        print("Dados insuficientes extraídos.")
        sys.exit(1)

    audit = audit_dataset(df)
    print(f"[Audit] rows={audit['rows']} exact_duplicates={audit['exact_duplicates']}")

    output_dir = Path("data")
    trained_reports: List[Dict[str, Any]] = []
    for family in ITEM_FAMILIES:
        family_df = df.loc[df.get("item_family") == family].copy()
        report = _train_family_model(family, family_df, output_dir)
        if report is not None:
            trained_reports.append(report)

    if not trained_reports:
        print("Nenhuma família teve dados suficientes para treino.")
        sys.exit(1)

    for report in trained_reports:
        metrics = report["metrics"]
        print(
            f"[Family {report['family']}] rows={report['rows']} rmse={metrics['rmse']:.2f} mae={metrics['mae']:.2f} model={report['model_path']}"
        )


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
            "data/firehose.parquet", "--parquet-path", help="Parquet source path"
        ),
    ):
        train_xgboost_oracle(
            league=league,
            items_per_base=items_per_base,
            source=source,
            sqlite_path=sqlite_path,
            parquet_path=parquet_path,
        )

    app()
