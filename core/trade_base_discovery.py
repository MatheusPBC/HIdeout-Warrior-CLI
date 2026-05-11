import sqlite3
from datetime import datetime, timezone
from statistics import median
from typing import Any, Sequence


def build_broad_discovery_query(min_price: int = 1, max_price: int = 500) -> dict[str, Any]:
    return {
        "query": {
            "status": {"option": "online"},
            "filters": {
                "trade_filters": {
                    "filters": {
                        "price": {
                            "min": int(min_price),
                            "max": int(max_price),
                            "option": "chaos",
                        }
                    }
                }
            },
        },
        "sort": {"indexed": "desc"},
    }


def initialize_discovered_base_database(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS discovered_base_types (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            league TEXT NOT NULL,
            base_type TEXT NOT NULL,
            sample_count INTEGER NOT NULL,
            min_price_chaos REAL NOT NULL,
            median_price_chaos REAL NOT NULL,
            max_price_chaos REAL NOT NULL,
            freshness_score REAL NOT NULL,
            discovery_score REAL NOT NULL,
            discovered_at TEXT NOT NULL,
            UNIQUE(league, base_type)
        )
        """
    )
    conn.commit()


def discover_trade_base_types(
    *,
    client: Any,
    conn: sqlite3.Connection,
    league: str,
    run_id: str,
    max_results: int = 100,
    max_fetches: int = 10,
    min_price: int = 1,
    max_price: int = 500,
) -> dict[str, int]:
    initialize_discovered_base_database(conn)
    query_id, result_ids = client.search_items(
        build_broad_discovery_query(min_price=min_price, max_price=max_price)
    )
    if not query_id or not result_ids:
        return {"searched": 1, "fetched": 0, "candidates": 0, "base_types": 0}

    candidates: list[dict[str, Any]] = []
    fetched = 0
    for batch in _batched(result_ids[: max(max_results, 0)], 10):
        if fetched >= max_fetches:
            break
        candidates.extend(_candidate_from_detail(detail) for detail in client.fetch_item_details(batch, query_id))
        fetched += 1

    valid_candidates = [candidate for candidate in candidates if candidate]
    rows = _rank_base_types(valid_candidates, league=league, run_id=run_id)
    _upsert_discovered_bases(conn, rows)
    return {
        "searched": 1,
        "fetched": fetched,
        "candidates": len(valid_candidates),
        "base_types": len(rows),
    }


def get_discovered_base_types(
    conn: sqlite3.Connection,
    *,
    league: str,
    limit: int = 8,
) -> list[str]:
    if limit <= 0 or not _table_exists(conn, "discovered_base_types"):
        return []
    rows = conn.execute(
        """
        SELECT base_type
        FROM discovered_base_types
        WHERE league = ?
          AND TRIM(base_type) != ''
        ORDER BY discovery_score DESC, sample_count DESC, base_type ASC
        LIMIT ?
        """,
        (league, int(limit)),
    ).fetchall()
    return [str(row[0]) for row in rows]


def _candidate_from_detail(detail: dict[str, Any]) -> dict[str, Any]:
    listing = detail.get("listing") or {}
    item = detail.get("item") or {}
    price = listing.get("price") or {}
    base_type = str(item.get("baseType") or item.get("typeLine") or "").strip()
    price_chaos = _price_to_chaos(price)
    indexed = str(listing.get("indexed") or item.get("indexed") or "")
    if not base_type or price_chaos <= 0:
        return {}
    return {"base_type": base_type, "price_chaos": price_chaos, "indexed": indexed}


def _rank_base_types(
    candidates: Sequence[dict[str, Any]],
    *,
    league: str,
    run_id: str,
) -> list[dict[str, Any]]:
    by_base: dict[str, list[float]] = {}
    for candidate in candidates:
        by_base.setdefault(str(candidate["base_type"]), []).append(float(candidate["price_chaos"]))

    discovered_at = _utc_now_iso()
    rows = []
    for base_type, prices in by_base.items():
        sample_count = len(prices)
        price_spread = (max(prices) - min(prices)) / max(median(prices), 1.0)
        freshness_score = 1.0
        discovery_score = sample_count + min(price_spread, 3.0) + freshness_score
        rows.append(
            {
                "run_id": run_id,
                "league": league,
                "base_type": base_type,
                "sample_count": sample_count,
                "min_price_chaos": min(prices),
                "median_price_chaos": float(median(prices)),
                "max_price_chaos": max(prices),
                "freshness_score": freshness_score,
                "discovery_score": discovery_score,
                "discovered_at": discovered_at,
            }
        )
    return sorted(rows, key=lambda row: row["discovery_score"], reverse=True)


def _upsert_discovered_bases(conn: sqlite3.Connection, rows: Sequence[dict[str, Any]]) -> None:
    if not rows:
        return
    with conn:
        conn.executemany(
            """
            INSERT INTO discovered_base_types (
                run_id, league, base_type, sample_count, min_price_chaos,
                median_price_chaos, max_price_chaos, freshness_score,
                discovery_score, discovered_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(league, base_type)
            DO UPDATE SET
                run_id=excluded.run_id,
                sample_count=excluded.sample_count,
                min_price_chaos=excluded.min_price_chaos,
                median_price_chaos=excluded.median_price_chaos,
                max_price_chaos=excluded.max_price_chaos,
                freshness_score=excluded.freshness_score,
                discovery_score=excluded.discovery_score,
                discovered_at=excluded.discovered_at
            """,
            [
                (
                    row["run_id"],
                    row["league"],
                    row["base_type"],
                    row["sample_count"],
                    row["min_price_chaos"],
                    row["median_price_chaos"],
                    row["max_price_chaos"],
                    row["freshness_score"],
                    row["discovery_score"],
                    row["discovered_at"],
                )
                for row in rows
            ],
        )


def _price_to_chaos(price: dict[str, Any]) -> float:
    rates = {"chaos": 1.0, "c": 1.0, "divine": 125.0, "div": 125.0}
    amount = float(price.get("amount") or 0.0)
    currency = str(price.get("currency") or "").lower()
    return round(amount * rates.get(currency, 0.0), 2)


def _batched(values: Sequence[str], size: int) -> list[list[str]]:
    return [list(values[index : index + size]) for index in range(0, len(values), size)]


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
