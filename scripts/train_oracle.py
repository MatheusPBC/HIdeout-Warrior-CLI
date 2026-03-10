import sys
import os
import json
import time
import re
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Tuple, Any, Set, cast

# Adicionar a raiz ao PYTHONPATH para os imports do core funcionarem localmente
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn
import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error

from core.api_integrator import MarketAPIClient
from core.graph_engine import ItemState
from core.meta_analyzer import LadderAnalyzer, MetaScores, calculate_meta_utility_score


PRICE_QUERY_BUCKETS: List[Tuple[float, Optional[float]]] = [
    (1.0, 10.0),
    (10.0, 40.0),
    (40.0, 120.0),
    (120.0, None),
]


def _slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def default_dataset_path(league: str) -> str:
    os.makedirs("data", exist_ok=True)
    return os.path.join("data", f"oracle_dataset_{_slugify(league)}.jsonl")


def default_checkpoint_path(league: str) -> str:
    os.makedirs("data", exist_ok=True)
    return os.path.join("data", f"oracle_collect_checkpoint_{_slugify(league)}.json")


def append_jsonl_records(path: str, records: List[Dict[str, Any]]) -> None:
    if not records:
        return
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "a", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def load_jsonl_records(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        return []

    records: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            try:
                parsed = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict):
                records.append(parsed)
    return records


def read_seen_item_ids(path: str) -> Set[str]:
    seen_ids: Set[str] = set()
    for row in load_jsonl_records(path):
        item_id = row.get("item_id")
        if isinstance(item_id, str) and item_id:
            seen_ids.add(item_id)
    return seen_ids


def save_checkpoint(path: str, checkpoint: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(checkpoint, handle, ensure_ascii=False, indent=2)


def load_checkpoint(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return payload if isinstance(payload, dict) else {}


def deduplicate_by_item_id(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    deduped: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    for row in records:
        item_id = row.get("item_id")
        if isinstance(item_id, str) and item_id:
            if item_id in seen:
                continue
            seen.add(item_id)
        deduped.append(row)
    return deduped


def parse_trade_item_to_features(
    item_data: dict, currency_rates: dict, meta_scores: Optional[MetaScores] = None
) -> Optional[dict]:
    """
    Recebe o JSON de um item gerado pela GGG Trade API e extrai as Features Vetorizadas
    esperadas pelo modelo de XGBoost.

    Args:
        item_data: Raw item JSON from GGG Trade API
        currency_rates: Currency conversion rates to chaos
        meta_scores: Current meta scores for tag weight calculation

    Returns:
        Dictionary of features or None if item should be filtered out
    """
    listing = item_data.get("listing", {})
    price_info = listing.get("price", {})
    currency = price_info.get("currency", "")
    amount = price_info.get("amount", 0.0)

    # Skip items without valid price
    if not amount or amount <= 0:
        return None

    # Conversão Universal de Divisas -> Chaos Orb baseada no poe.ninja
    price_chaos = amount
    if currency != "chaos":
        # Correção Semântica das Tags da GGG vs Poe.Ninja
        ninja_key_map = {
            "divine": "Divine Orb",
            "exalted": "Exalted Orb",
            "mirror": "Mirror of Kalandra",
            "alch": "Orb of Alchemy",
        }
        ninja_key = ninja_key_map.get(currency, currency.title() + " Orb")

        if ninja_key in currency_rates:
            price_chaos = amount * currency_rates[ninja_key]
        elif currency == "divine":
            # Hardcoded Fallback para o Standard caso ninja falhe temporariamente
            price_chaos = amount * 125.0

    item = item_data.get("item", {})
    ilvl = item.get("ilvl", 1)
    base_type = item.get("baseType", "")

    # Extract listing timestamp for stale filter
    listed_at = listing.get("indexed", "")

    # Influência Lógica
    influences = item.get("influences", {})
    is_influenced = 1 if influences else 0
    is_fractured = 1 if item.get("fractured", False) else 0
    feature_influence = max(is_influenced, is_fractured)

    # Parsing de Mods - Heurística Base para o Treino
    mods = item.get("explicitMods", [])
    implicit_mods = item.get("implicitMods", [])
    all_mods = mods + implicit_mods

    tier_life = 0
    tier_speed = 0
    tier_resist = 0
    tier_crit = 0
    total_affixes = len(mods)

    # Extract item tags for meta scoring
    item_tags: List[str] = []

    for mod in all_mods:
        mod_lower = mod.lower()

        # Life tier detection
        if "maximum life" in mod_lower:
            tier_life = 1 if "to maximum" in mod_lower else 2
            item_tags.append("life")

        # Speed tier detection
        if "speed" in mod_lower:
            tier_speed = 1 if "increased" in mod_lower else 2
            item_tags.append("speed")

        # Resistance detection
        if "resistance" in mod_lower or "resist" in mod_lower:
            tier_resist = 1
            item_tags.append("resistance")

        # Crit detection
        if "critical" in mod_lower or "crit" in mod_lower:
            tier_crit = 1
            item_tags.append("crit")

        # Elemental damage detection
        if "fire" in mod_lower or "adds.*fire" in mod_lower:
            item_tags.append("fire")
        if "cold" in mod_lower or "adds.*cold" in mod_lower:
            item_tags.append("cold")
        if "lightning" in mod_lower or "adds.*lightning" in mod_lower:
            item_tags.append("lightning")

        # Physical/Chaos
        if "physical" in mod_lower:
            item_tags.append("physical")
        if "chaos" in mod_lower:
            item_tags.append("chaos")

        # Defense
        if (
            "armor" in mod_lower
            or "evasion" in mod_lower
            or "energy shield" in mod_lower
        ):
            item_tags.append("defense")

        # Attack/Spell
        if "attack" in mod_lower:
            item_tags.append("attack")
        if "spell" in mod_lower:
            item_tags.append("spell")

    # Add influence tags
    if influences:
        for influence_type in influences.keys():
            item_tags.append(influence_type.lower())

    open_affixes = max(0, 6 - total_affixes)

    # Calculate meta utility score
    meta_utility_score = 0.0
    if meta_scores and item_tags:
        meta_utility_score = calculate_meta_utility_score(
            list(set(item_tags)),  # Deduplicate tags
            meta_scores,
            aggregation="mean",
        )

    return {
        "is_influenced": feature_influence,
        "ilvl": ilvl,
        "base_type": base_type,
        "tier_life": tier_life,
        "tier_speed": tier_speed,
        "tier_resist": tier_resist,
        "tier_crit": tier_crit,
        "open_affixes": open_affixes,
        "listed_at": listed_at,
        "meta_utility_score": meta_utility_score,
        "price_chaos": round(price_chaos, 1),
    }


def parse_listing_timestamp(listed_at: str) -> Optional[datetime]:
    """
    Parse GGG Trade API timestamp string to datetime.

    Args:
        listed_at: ISO format timestamp string

    Returns:
        Parsed datetime or None if parsing fails
    """
    if not listed_at:
        return None

    try:
        # GGG uses ISO format: "2024-01-15T10:30:00Z" or similar
        # Handle various ISO formats
        listed_at = listed_at.replace("Z", "+00:00")
        return datetime.fromisoformat(listed_at)
    except (ValueError, TypeError):
        return None


def is_listing_stale(listed_at: str, hours_threshold: float = 48.0) -> bool:
    """
    Check if a listing is stale (listed for too long).

    Args:
        listed_at: ISO timestamp of listing
        hours_threshold: Hours threshold for staleness

    Returns:
        True if listing is stale
    """
    parsed_time = parse_listing_timestamp(listed_at)
    if not parsed_time:
        return False

    # Handle timezone-aware vs naive comparison
    try:
        age = datetime.now(parsed_time.tzinfo) - parsed_time
    except TypeError:
        # Mixed naive/aware comparison
        age = datetime.now() - parsed_time.replace(tzinfo=None)

    return age > timedelta(hours=hours_threshold)


def remove_price_outliers_iqr(
    df: pd.DataFrame,
    group_col: str = "base_type",
    price_col: str = "price_chaos",
    multiplier: float = 1.5,
) -> pd.DataFrame:
    """
    Remove price outliers using IQR method grouped by base type.

    Removes items where price < Q1 - 1.5*IQR (price fixers) or
    price > Q3 + 1.5*IQR (absurd prices).

    Args:
        df: Input DataFrame
        group_col: Column to group by (e.g., "base_type")
        price_col: Price column name
        multiplier: IQR multiplier (default 1.5)

    Returns:
        Filtered DataFrame with outliers removed
    """
    if df.empty:
        return df

    # Vectorized IQR calculation per group
    def calculate_iqr_bounds(group: pd.Series) -> Tuple[float, float]:
        """Calculate IQR bounds for a price group."""
        q1 = group.quantile(0.25)
        q3 = group.quantile(0.75)
        iqr = q3 - q1
        lower_bound = q1 - multiplier * iqr
        upper_bound = q3 + multiplier * iqr
        return lower_bound, upper_bound

    # Calculate bounds for each base_type
    bounds = df.groupby(group_col)[price_col].apply(calculate_iqr_bounds).to_dict()

    # Vectorized filtering
    def is_within_bounds(row: pd.Series) -> bool:
        """Check if price is within IQR bounds for its base_type."""
        base = row[group_col]
        price = row[price_col]

        if base not in bounds:
            return True  # Keep if no bounds calculated

        lower, upper = bounds[base]
        return lower <= price <= upper

    mask = df.apply(is_within_bounds, axis=1)
    filtered_df = df.loc[mask].copy()

    removed_count = len(df) - len(filtered_df)
    if removed_count > 0:
        print(
            f"🧹 [IQR Filter] Removed {removed_count} price outliers ({removed_count / len(df) * 100:.1f}%)"
        )

    return filtered_df


def remove_stale_listings(
    df: pd.DataFrame, hours_threshold: float = 48.0, min_tier_score: int = 2
) -> pd.DataFrame:
    """
    Remove stale listings that are likely fake (good tiers + low price + old).

    If an item has excellent tier scores but very low price AND has been
    listed for > threshold hours, it's likely a fake/scam listing.

    Args:
        df: Input DataFrame
        hours_threshold: Hours threshold for staleness
        min_tier_score: Minimum tier score to be considered "excellent"

    Returns:
        Filtered DataFrame
    """
    if df.empty or "listed_at" not in df.columns:
        return df

    # Calculate total tier score (sum of all tier columns)
    tier_columns = ["tier_life", "tier_speed", "tier_resist", "tier_crit"]
    available_tier_cols = [col for col in tier_columns if col in df.columns]

    if not available_tier_cols:
        return df

    # Vectorized tier score calculation
    df = df.copy()
    df["total_tier_score"] = df[available_tier_cols].sum(axis=1)

    # Get price statistics for "low price" determination
    price_q25 = df["price_chaos"].quantile(0.25)

    # Vectorized staleness check
    def is_stale_row(row: pd.Series) -> bool:
        """Check if row represents a stale fake listing."""
        listed_at = row.get("listed_at", "")

        if not listed_at:
            return False

        # Check if stale
        if not is_listing_stale(listed_at, hours_threshold):
            return False

        # Check if has excellent tiers and low price
        has_excellent_tiers = row["total_tier_score"] >= min_tier_score
        has_low_price = row["price_chaos"] <= price_q25

        return has_excellent_tiers and has_low_price

    mask = ~df.apply(is_stale_row, axis=1)
    filtered_df = df.loc[mask].copy()

    # Drop temporary column
    filtered_df = filtered_df.drop(columns=["total_tier_score"], errors="ignore")

    removed_count = len(df) - len(filtered_df)
    if removed_count > 0:
        print(f"🧹 [Stale Filter] Removed {removed_count} likely fake stale listings")

    return filtered_df


def extract_item_tags_vectorized(df: pd.DataFrame) -> pd.DataFrame:
    """
    Vectorized extraction of item tags for meta scoring.

    Args:
        df: Input DataFrame with item data

    Returns:
        DataFrame with extracted tag information
    """
    if df.empty:
        return df

    df = df.copy()

    # Initialize tag columns (vectorized operations)
    tag_categories = [
        "has_life",
        "has_speed",
        "has_resist",
        "has_crit",
        "has_fire",
        "has_cold",
        "has_lightning",
        "has_physical",
        "has_chaos",
        "has_defense",
    ]

    for tag in tag_categories:
        df[tag] = 0

    # These would be populated from mod parsing - simplified version
    df["has_life"] = (df["tier_life"] > 0).astype(int)
    df["has_speed"] = (df["tier_speed"] > 0).astype(int)
    df["has_resist"] = (df["tier_resist"] > 0).astype(int)
    df["has_crit"] = (df["tier_crit"] > 0).astype(int)

    return df


def _feature_columns(df: pd.DataFrame, target_col: str = "price_chaos") -> List[str]:
    return [col for col in df.columns if col != target_col]


def _feature_fingerprints(df: pd.DataFrame, feature_cols: List[str]) -> Set[str]:
    if df.empty or not feature_cols:
        return set()
    row_fingerprints = df[feature_cols].astype(str).agg("|".join, axis=1)
    return set(row_fingerprints.tolist())


def audit_dataset(df: pd.DataFrame, target_col: str = "price_chaos") -> Dict[str, int]:
    """Audit potential leakage vectors before split."""
    if df.empty:
        return {
            "rows": 0,
            "exact_duplicates": 0,
        }

    feature_cols = _feature_columns(df, target_col=target_col)
    exact_duplicates = int(df.duplicated(subset=feature_cols + [target_col]).sum())

    return {
        "rows": int(len(df)),
        "exact_duplicates": exact_duplicates,
    }


def split_dataset_for_training(
    df: pd.DataFrame,
    target_col: str = "price_chaos",
    test_size: float = 0.2,
    random_state: int = 42,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series, str]:
    """
    Split dataset using temporal strategy when reliable timestamps are present.
    Falls back to random split when timestamp quality is insufficient.
    """
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


def sample_result_ids_stratified(
    result_ids: List[str], sample_size: int, n_strata: int = 10
) -> List[str]:
    """
    Deterministic stratified sampling over ranked result IDs.

    Splits the ranked list into strata and samples proportionally from each
    stratum, preserving rank order in the final output.
    """
    if sample_size <= 0 or not result_ids:
        return []

    total = len(result_ids)
    if sample_size >= total:
        return list(result_ids)

    strata_count = max(1, min(n_strata, total, sample_size))
    boundaries = np.linspace(0, total, strata_count + 1, dtype=int)

    selected_indices: List[int] = []
    base_quota = sample_size // strata_count
    remainder = sample_size % strata_count

    for stratum_idx in range(strata_count):
        start = int(boundaries[stratum_idx])
        end = int(boundaries[stratum_idx + 1])
        if end <= start:
            continue

        stratum_indices = list(range(start, end))
        quota = base_quota + (1 if stratum_idx < remainder else 0)
        if quota <= 0:
            continue
        quota = min(quota, len(stratum_indices))

        if quota == len(stratum_indices):
            selected_indices.extend(stratum_indices)
            continue

        local_positions = np.linspace(0, len(stratum_indices) - 1, quota, dtype=int)
        selected_indices.extend(stratum_indices[pos] for pos in local_positions)

    selected_indices = sorted(set(selected_indices))

    if len(selected_indices) < sample_size:
        missing = sample_size - len(selected_indices)
        selected_set = set(selected_indices)
        for idx in range(total):
            if idx in selected_set:
                continue
            selected_indices.append(idx)
            missing -= 1
            if missing == 0:
                break

    selected_indices = sorted(selected_indices[:sample_size])
    return [result_ids[idx] for idx in selected_indices]


def allocate_items_across_price_buckets(
    items_per_base: int,
    bucket_count: int = len(PRICE_QUERY_BUCKETS),
) -> List[int]:
    if items_per_base <= 0 or bucket_count <= 0:
        return []

    base_quota = items_per_base // bucket_count
    remainder = items_per_base % bucket_count
    return [base_quota + (1 if idx < remainder else 0) for idx in range(bucket_count)]


def build_price_filter_for_query(
    min_price: float,
    max_price: Optional[float],
) -> Dict[str, float | str]:
    price_filter: Dict[str, float | str] = {
        "option": "chaos",
        "min": float(min_price),
    }
    if max_price is not None:
        price_filter["max"] = float(max_price)
    return price_filter


def summarize_price_distribution(
    df: pd.DataFrame,
    target_col: str = "price_chaos",
) -> Dict[str, Any]:
    if target_col not in df.columns or df.empty:
        return {
            "rows": int(len(df)),
            "nunique": 0,
            "bucket_counts": {"<=50": 0, "50-150": 0, ">150": 0},
            "quantiles": {},
        }

    target = df[target_col]
    bucket_counts = {
        "<=50": int((target <= 50).sum()),
        "50-150": int(((target > 50) & (target <= 150)).sum()),
        ">150": int((target > 150).sum()),
    }
    quantiles = {
        "q05": float(target.quantile(0.05)),
        "q25": float(target.quantile(0.25)),
        "q50": float(target.quantile(0.50)),
        "q75": float(target.quantile(0.75)),
        "q95": float(target.quantile(0.95)),
    }

    return {
        "rows": int(len(df)),
        "nunique": int(target.nunique()),
        "bucket_counts": bucket_counts,
        "quantiles": quantiles,
    }


def validate_dataset_distribution_or_raise(
    df: pd.DataFrame,
    target_col: str = "price_chaos",
) -> Dict[str, Any]:
    summary = summarize_price_distribution(df, target_col=target_col)
    bucket_counts: Dict[str, int] = summary["bucket_counts"]
    non_empty_buckets = [label for label, count in bucket_counts.items() if count > 0]
    nunique = int(summary["nunique"])

    if nunique < 5 or len(non_empty_buckets) == 1:
        raise ValueError(
            "Dataset degenerado para treino: distribuição de preço sem variação útil. "
            f"nunique={nunique}, bucket_counts={bucket_counts}."
        )

    return summary


def drop_leakage_columns(
    frame: pd.DataFrame, leakage_columns: List[str]
) -> pd.DataFrame:
    return frame.drop(columns=leakage_columns, errors="ignore")


def validate_training_schema(
    train_x: pd.DataFrame,
    test_x: pd.DataFrame,
    forbidden_columns: List[str],
) -> None:
    train_cols = list(train_x.columns)
    test_cols = list(test_x.columns)

    if train_cols != test_cols:
        raise ValueError(
            "Train/test feature schema mismatch after leakage drop: "
            f"train={train_cols}, test={test_cols}"
        )

    leaked = [col for col in train_cols if col in forbidden_columns]
    if leaked:
        raise ValueError(f"Forbidden columns leaked into model schema: {leaked}")


def transform_target_log1p(y: pd.Series) -> pd.Series:
    safe_target = y.clip(lower=0.0)
    transformed = np.log1p(safe_target)
    return pd.Series(transformed, index=y.index, name=y.name)


def invert_target_log1p(y_pred: np.ndarray) -> np.ndarray:
    return np.expm1(y_pred)


def fetch_training_data(
    target_bases: List[str],
    items_per_base: int = 500,
    league: str = "Standard",
    apply_outlier_filter: bool = True,
    apply_stale_filter: bool = True,
) -> pd.DataFrame:
    """
    Faz consultas Live GGG Trade API com rate limit respeitado e extrai os itens pro DataSet.

    Args:
        target_bases: List of base types to query
        items_per_base: Number of items to fetch per base
        league: PoE league
        apply_outlier_filter: Whether to apply IQR price outlier removal
        apply_stale_filter: Whether to apply stale listing filter

    Returns:
        DataFrame with training data
    """
    client = MarketAPIClient(league=league)
    currency_rates = client.sync_ninja_economy()

    # Initialize meta analyzer and fetch current meta scores
    print("🌐 [Meta] Fetching current ladder meta scores...")
    analyzer = LadderAnalyzer(league=league)
    meta_scores = analyzer.fetch_meta_weights(force_refresh=False)

    if meta_scores.scores:
        print(
            f"✅ [Meta] Loaded {len(meta_scores.scores)} tag weights from meta analysis"
        )
    else:
        print("⚠️ [Meta] Could not fetch meta scores, using default weights")

    dataset = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TextColumn("({task.completed}/{task.total} iter)"),
    ) as progress:
        bucket_allocations = allocate_items_across_price_buckets(
            items_per_base, bucket_count=len(PRICE_QUERY_BUCKETS)
        )
        iterations_per_base = sum((quota + 9) // 10 for quota in bucket_allocations)
        total_iterations = len(target_bases) * iterations_per_base
        overall_task = progress.add_task(
            "[cyan]Comunicação com API GGG...", total=total_iterations
        )

        for base_type in target_bases:
            progress.update(
                overall_task, description=f"[cyan]A Sacar Mercado: {base_type}..."
            )

            base_seen_ids: Set[str] = set()
            fetched_any_bucket = False
            for bucket_idx, (min_price, max_price) in enumerate(PRICE_QUERY_BUCKETS):
                bucket_quota = bucket_allocations[bucket_idx]
                if bucket_quota <= 0:
                    continue

                price_filter = build_price_filter_for_query(
                    min_price=min_price,
                    max_price=max_price,
                )
                query = {
                    "query": {
                        "status": {"option": "online"},
                        "type": base_type,
                        "filters": {
                            "trade_filters": {
                                "filters": {
                                    "price": price_filter,
                                }
                            }
                        },
                    },
                    "sort": {"price": "asc"},
                }

                query_id, result_ids = client.search_items(query)
                if not query_id or not result_ids:
                    progress.console.print(
                        "[yellow]⚠️ Sem liquidez atual para "
                        f"{base_type} no bucket {min_price}-{max_price or '+'}."
                    )
                    continue

                fetched_any_bucket = True
                sampled_ids = sample_result_ids_stratified(result_ids, bucket_quota)
                sampled_ids = [
                    item_id for item_id in sampled_ids if item_id not in base_seen_ids
                ]
                base_seen_ids.update(sampled_ids)

                batch_size = 10
                for i in range(0, len(sampled_ids), batch_size):
                    batch_ids = sampled_ids[i : i + batch_size]
                    details = client.fetch_item_details(batch_ids, query_id)

                    for item_json in details:
                        if (
                            not item_json.get("listing", {})
                            .get("price", {})
                            .get("amount")
                        ):
                            continue

                        features = parse_trade_item_to_features(
                            item_json, currency_rates, meta_scores=meta_scores
                        )

                        if features:
                            dataset.append(features)

                    progress.advance(overall_task, advance=1)

            if not fetched_any_bucket:
                progress.console.print(
                    f"[yellow]⚠️ Sem liquidez atual para {base_type}."
                )

    df = pd.DataFrame(dataset)

    if df.empty:
        print("⚠️ [Training] No data fetched from API")
        return df

    print(f"\n📊 [Training] Fetched {len(df)} raw items from API")

    # Apply IQR-based price outlier removal
    if apply_outlier_filter and len(df) > 10:
        df = remove_price_outliers_iqr(
            df, group_col="base_type", price_col="price_chaos"
        )

    # Apply stale listing filter
    if apply_stale_filter and len(df) > 10:
        df = remove_stale_listings(df, hours_threshold=48.0, min_tier_score=2)

    print(f"📊 [Training] Final dataset size after filtering: {len(df)} items")

    return df


def collect_training_data_incremental(
    target_bases: List[str],
    items_per_base: int,
    league: str,
    dataset_path: str,
    checkpoint_path: str,
    resume: bool,
    chunk_size: int = 100,
) -> Dict[str, int]:
    client = MarketAPIClient(league=league)
    currency_rates = client.sync_ninja_economy()

    print("🌐 [Meta] Fetching current ladder meta scores...")
    analyzer = LadderAnalyzer(league=league)
    meta_scores = analyzer.fetch_meta_weights(force_refresh=False)

    checkpoint = load_checkpoint(checkpoint_path) if resume else {}
    start_base_idx = int(checkpoint.get("base_index", 0)) if checkpoint else 0
    start_bucket_idx = int(checkpoint.get("bucket_index", 0)) if checkpoint else 0
    start_batch_offset = int(checkpoint.get("batch_offset", 0)) if checkpoint else 0
    checkpoint_base_seen = (
        set(checkpoint.get("base_seen_ids", [])) if checkpoint else set()
    )

    seen_item_ids = read_seen_item_ids(dataset_path)
    flushed_records = 0
    pending_chunk: List[Dict[str, Any]] = []
    bucket_allocations = allocate_items_across_price_buckets(
        items_per_base, bucket_count=len(PRICE_QUERY_BUCKETS)
    )

    for base_idx, base_type in enumerate(target_bases):
        if base_idx < start_base_idx:
            continue

        base_seen_ids: Set[str] = set()
        if resume and base_idx == start_base_idx:
            base_seen_ids = set(checkpoint_base_seen)

        for bucket_idx, (min_price, max_price) in enumerate(PRICE_QUERY_BUCKETS):
            if bucket_allocations[bucket_idx] <= 0:
                continue
            if base_idx == start_base_idx and bucket_idx < start_bucket_idx:
                continue

            query = {
                "query": {
                    "status": {"option": "online"},
                    "type": base_type,
                    "filters": {
                        "trade_filters": {
                            "filters": {
                                "price": build_price_filter_for_query(
                                    min_price=min_price,
                                    max_price=max_price,
                                )
                            }
                        }
                    },
                },
                "sort": {"price": "asc"},
            }
            query_id, result_ids = client.search_items(query)
            if not query_id or not result_ids:
                continue

            sampled_ids = sample_result_ids_stratified(
                result_ids, bucket_allocations[bucket_idx]
            )
            sampled_ids = [
                item_id for item_id in sampled_ids if item_id not in base_seen_ids
            ]
            base_seen_ids.update(sampled_ids)

            effective_offset = 0
            if base_idx == start_base_idx and bucket_idx == start_bucket_idx:
                effective_offset = max(0, start_batch_offset)

            for batch_offset in range(effective_offset, len(sampled_ids), 10):
                batch_ids = sampled_ids[batch_offset : batch_offset + 10]
                details = client.fetch_item_details(batch_ids, query_id)

                for item_json in details:
                    item_id = item_json.get("id")
                    if not isinstance(item_id, str) or not item_id:
                        continue
                    if item_id in seen_item_ids:
                        continue

                    features = parse_trade_item_to_features(
                        item_json,
                        currency_rates,
                        meta_scores=meta_scores,
                    )
                    if not features:
                        continue

                    features["item_id"] = item_id
                    pending_chunk.append(features)
                    seen_item_ids.add(item_id)

                if len(pending_chunk) >= chunk_size:
                    append_jsonl_records(dataset_path, pending_chunk)
                    flushed_records += len(pending_chunk)
                    pending_chunk = []

                save_checkpoint(
                    checkpoint_path,
                    {
                        "league": league,
                        "base_index": base_idx,
                        "bucket_index": bucket_idx,
                        "batch_offset": batch_offset + 10,
                        "base_seen_ids": sorted(base_seen_ids),
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    },
                )

            start_batch_offset = 0

        save_checkpoint(
            checkpoint_path,
            {
                "league": league,
                "base_index": base_idx + 1,
                "bucket_index": 0,
                "batch_offset": 0,
                "base_seen_ids": [],
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
        )

    if pending_chunk:
        append_jsonl_records(dataset_path, pending_chunk)
        flushed_records += len(pending_chunk)

    if os.path.exists(checkpoint_path):
        os.remove(checkpoint_path)

    return {
        "written": flushed_records,
        "seen_total": len(seen_item_ids),
    }


def load_dataset_for_training(
    dataset_path: str,
    apply_outlier_filter: bool = True,
    apply_stale_filter: bool = True,
) -> pd.DataFrame:
    records = load_jsonl_records(dataset_path)
    records = deduplicate_by_item_id(records)
    df = pd.DataFrame(records)

    if df.empty:
        return df

    if apply_outlier_filter and len(df) > 10:
        df = remove_price_outliers_iqr(
            df, group_col="base_type", price_col="price_chaos"
        )
    if apply_stale_filter and len(df) > 10:
        df = remove_stale_listings(df, hours_threshold=48.0, min_tier_score=2)
    return df


def train_xgboost_oracle(
    league: str = "Standard",
    items_per_base: int = 500,
    dataset_path: Optional[str] = None,
) -> None:
    """
    Train the XGBoost price oracle with filtered market data.

    Args:
        league: PoE league to train on
        items_per_base: Number of items to fetch per base type
    """
    print(f"🚀 [Fase 6.1] Iniciando Treino do XGBoost com Dados Reais da Trade API...")
    print(f"   Liga: {league}")

    target_bases = [
        "Imbued Wand",
        "Spine Bow",
        "Titanium Spirit Shield",
        "Vaal Regalia",
        "Hubris Circlet",
        "Sadist Garb",
    ]

    if dataset_path:
        print(f"📂 [Training] Loading dataset from {dataset_path}")
        df = load_dataset_for_training(
            dataset_path=dataset_path,
            apply_outlier_filter=True,
            apply_stale_filter=True,
        )
    else:
        # O GGG Rate Limit aciona demorados limites (timeout 60s) em largas extrações.
        # Puxaremos 500 de cada base x 6 = 3000 itens (respeitando a requisição de 2k-5k itens).
        df = fetch_training_data(
            target_bases,
            items_per_base=items_per_base,
            league=league,
            apply_outlier_filter=True,
            apply_stale_filter=True,
        )

    if len(df) < 50:
        print(
            "❌ Dados insuficientes extraídos (menos que 50). Verifique o GGG Ban IP."
        )
        sys.exit(1)

    distribution_summary = validate_dataset_distribution_or_raise(df)
    print("\n📈 [Distribuição] price_chaos")
    print(f"   nunique: {distribution_summary['nunique']}")
    print(f"   buckets: {distribution_summary['bucket_counts']}")
    print(f"   quantis: {distribution_summary['quantiles']}")

    print(f"\n📊 Extracção Completa! Dados Válidos Encontrados: {len(df)} listagens.")

    # Dataset audit and split strategy
    dataset_audit = audit_dataset(df)
    print("\n🧪 [Auditoria] Dataset")
    print(f"   Registros totais: {dataset_audit['rows']}")
    print(
        f"   Duplicatas exatas (features+target): {dataset_audit['exact_duplicates']}"
    )

    raw_x_train, raw_x_test, y_train, y_test, split_strategy = (
        split_dataset_for_training(df)
    )
    # Remove leakage-prone columns only after split
    leakage_columns = ["listed_at", "base_type", "item_id"]
    X_train = drop_leakage_columns(raw_x_train, leakage_columns)
    X_test = drop_leakage_columns(raw_x_test, leakage_columns)
    validate_training_schema(X_train, X_test, forbidden_columns=leakage_columns)
    overlap_count = calculate_feature_overlap(X_train, X_test)

    print(f"\n🧩 [Split] Estratégia: {split_strategy}")
    print(f"   Treino: {len(raw_x_train)} amostras | Teste: {len(raw_x_test)} amostras")
    print(f"   Overlap train/test (fingerprint de features): {overlap_count}")

    print(f"📋 Features usadas: {', '.join(X_train.columns)}")
    print("🧠 Injetando Matrizes no XGBoost Regressor...")

    y_train_transformed = transform_target_log1p(y_train)
    y_test_transformed = transform_target_log1p(y_test)
    dtrain = xgb.DMatrix(X_train, label=y_train_transformed)
    dtest = xgb.DMatrix(X_test, label=y_test_transformed)

    params = {
        "max_depth": 5,
        "eta": 0.05,
        "objective": "reg:squarederror",
        "eval_metric": "rmse",
    }

    evals = [(dtrain, "train"), (dtest, "eval")]

    # Early Stopping Preemptivo para generalizar melhor
    model = xgb.train(
        params,
        dtrain,
        num_boost_round=150,
        evals=evals,
        early_stopping_rounds=15,
        verbose_eval=50,
    )

    # Avaliação
    preds_log = model.predict(dtest)
    preds = invert_target_log1p(preds_log)
    preds = np.maximum(preds, 0.0)
    baseline_median = float(y_train.median())
    metrics = evaluate_predictions(y_test, preds, baseline_value=baseline_median)

    print("\n🎯 [MÉTRICAS]")
    print(f"   RMSE: {metrics['rmse']:.2f} Chaos")
    print(f"   MAE: {metrics['mae']:.2f} Chaos")
    print(f"   Baseline (mediana treino): {baseline_median:.2f} Chaos")
    print(f"   RMSE Baseline (teste): {metrics['baseline_rmse']:.2f} Chaos")
    print("   RMSE por bucket de preço:")
    for bucket_label, bucket_rmse in metrics["rmse_by_bucket"].items():
        formatted = f"{bucket_rmse:.2f} Chaos" if bucket_rmse is not None else "N/A"
        print(f"      {bucket_label}: {formatted}")

    # Feature importance
    importance = model.get_score(importance_type="gain")
    if importance:
        print("\n📊 Feature Importance (Gain):")
        sorted_importance = sorted(importance.items(), key=lambda x: x[1], reverse=True)
        for feat, score in sorted_importance[:5]:
            print(f"   {feat}: {score:.2f}")

    # Gravando .xgb final
    os.makedirs("data", exist_ok=True)
    model_path = os.path.join("data", "price_oracle.xgb")
    model.set_attr(target_transform="log1p")
    model.save_model(model_path)
    print(
        f"\n✅ [SUCESSO] Cérebro atualizado e injetado com Big Data verdadeiro em {model_path}!"
    )


if __name__ == "__main__":
    import typer

    app = typer.Typer(help="Treino do Oracle com coleta incremental e modo offline.")

    default_bases = [
        "Imbued Wand",
        "Spine Bow",
        "Titanium Spirit Shield",
        "Vaal Regalia",
        "Hubris Circlet",
        "Sadist Garb",
    ]

    @app.command("collect")
    def collect_cmd(
        league: str = typer.Option("Standard", "--league", "-l", help="PoE league"),
        items_per_base: int = typer.Option(
            500, "--items", "-i", help="Items per base type"
        ),
        dataset: Optional[str] = typer.Option(
            None, "--dataset", help="JSONL output path"
        ),
        checkpoint: Optional[str] = typer.Option(
            None, "--checkpoint", help="Checkpoint path"
        ),
        resume: bool = typer.Option(
            False, "--resume", help="Resume collection from checkpoint"
        ),
        chunk_size: int = typer.Option(100, "--chunk-size", help="Flush chunk size"),
    ) -> None:
        dataset_path = dataset or default_dataset_path(league)
        checkpoint_path = checkpoint or default_checkpoint_path(league)
        stats = collect_training_data_incremental(
            target_bases=default_bases,
            items_per_base=items_per_base,
            league=league,
            dataset_path=dataset_path,
            checkpoint_path=checkpoint_path,
            resume=resume,
            chunk_size=chunk_size,
        )
        print(f"✅ [Collect] Dataset: {dataset_path}")
        print(
            f"   Novos registros gravados: {stats['written']} | Total IDs únicos: {stats['seen_total']}"
        )

    @app.command("train")
    def train_cmd(
        league: str = typer.Option("Standard", "--league", "-l", help="PoE league"),
        items_per_base: int = typer.Option(
            500, "--items", "-i", help="Items per base type"
        ),
        dataset: Optional[str] = typer.Option(
            None,
            "--dataset",
            help="JSONL dataset path (offline mode; default path is league-based)",
        ),
    ) -> None:
        dataset_path = dataset or default_dataset_path(league)
        if not os.path.exists(dataset_path):
            print(
                f"❌ Dataset não encontrado para treino offline: {dataset_path}. "
                "Execute o comando collect primeiro."
            )
            raise typer.Exit(code=1)
        train_xgboost_oracle(
            league=league,
            items_per_base=items_per_base,
            dataset_path=dataset_path,
        )

    @app.command("collect-train")
    def collect_train_cmd(
        league: str = typer.Option("Standard", "--league", "-l", help="PoE league"),
        items_per_base: int = typer.Option(
            500, "--items", "-i", help="Items per base type"
        ),
        dataset: Optional[str] = typer.Option(
            None, "--dataset", help="JSONL dataset path"
        ),
        checkpoint: Optional[str] = typer.Option(
            None, "--checkpoint", help="Checkpoint path"
        ),
        resume: bool = typer.Option(
            False, "--resume", help="Resume collection from checkpoint"
        ),
        chunk_size: int = typer.Option(100, "--chunk-size", help="Flush chunk size"),
    ) -> None:
        dataset_path = dataset or default_dataset_path(league)
        checkpoint_path = checkpoint or default_checkpoint_path(league)
        collect_training_data_incremental(
            target_bases=default_bases,
            items_per_base=items_per_base,
            league=league,
            dataset_path=dataset_path,
            checkpoint_path=checkpoint_path,
            resume=resume,
            chunk_size=chunk_size,
        )
        train_xgboost_oracle(
            league=league,
            items_per_base=items_per_base,
            dataset_path=dataset_path,
        )

    if len(sys.argv) > 1 and sys.argv[1].startswith("-"):
        sys.argv.insert(1, "collect-train")

    app()
