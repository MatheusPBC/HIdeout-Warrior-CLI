import sqlite3
from typing import Any, cast

from core.trade_base_discovery import (
    build_broad_discovery_query,
    discover_trade_base_types,
    get_discovered_base_types,
    initialize_discovered_base_database,
)


def test_build_broad_discovery_query_has_no_fixed_base_type() -> None:
    query = build_broad_discovery_query(min_price=5, max_price=200)

    assert "type" not in query["query"]
    assert query["query"]["status"] == {"option": "online"}
    price_filter = query["query"]["filters"]["trade_filters"]["filters"]["price"]
    assert price_filter == {"min": 5, "max": 200, "option": "chaos"}
    assert query["sort"] == {"indexed": "desc"}


def test_discover_trade_base_types_ranks_by_volume_and_freshness() -> None:
    conn = sqlite3.connect(":memory:")
    initialize_discovered_base_database(conn)

    class _FakeClient:
        league = "Mirage"

        @staticmethod
        def search_items(_query):
            return "query-1", ["ring-1", "ring-2", "wand-1", "bad-1"]

        @staticmethod
        def fetch_item_details(item_ids, _query_id):
            details: list[dict[str, Any]] = []
            for item_id in item_ids:
                if item_id == "bad-1":
                    details.append({"id": item_id, "listing": {}, "item": {}})
                    continue
                base_type = "Opal Ring" if item_id.startswith("ring") else "Imbued Wand"
                details.append(
                    {
                        "id": item_id,
                        "listing": {
                            "indexed": "2026-05-11T10:00:00Z",
                            "price": {"currency": "chaos", "amount": 25.0},
                        },
                        "item": {"id": item_id, "baseType": base_type, "ilvl": 84},
                    }
                )
            return details

    totals = discover_trade_base_types(
        client=cast(Any, _FakeClient()),
        conn=conn,
        league="Mirage",
        run_id="run-1",
        max_results=4,
        max_fetches=1,
    )
    bases = get_discovered_base_types(conn, league="Mirage", limit=3)

    assert totals == {"searched": 1, "fetched": 1, "candidates": 3, "base_types": 2}
    assert bases == ["Opal Ring", "Imbued Wand"]


def test_get_discovered_base_types_filters_by_league() -> None:
    conn = sqlite3.connect(":memory:")
    initialize_discovered_base_database(conn)
    conn.executemany(
        """
        INSERT INTO discovered_base_types (
            run_id, league, base_type, sample_count, min_price_chaos,
            median_price_chaos, max_price_chaos, freshness_score,
            discovery_score, discovered_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("run-1", "Mirage", "Opal Ring", 3, 10.0, 25.0, 40.0, 1.0, 1.0, "now"),
            ("run-1", "Standard", "Vaal Regalia", 5, 10.0, 25.0, 40.0, 1.0, 2.0, "now"),
        ],
    )

    assert get_discovered_base_types(conn, league="Mirage", limit=10) == ["Opal Ring"]
