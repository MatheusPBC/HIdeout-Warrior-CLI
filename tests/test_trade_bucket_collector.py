import sqlite3
from typing import Any, cast

from scripts.trade_bucket_collector import (
    build_trade_query,
    collect_trade_bucket_events,
    ingest_trade_bucket_rows,
    initialize_trade_bucket_database,
)


def _sample_row() -> dict:
    return {
        "run_id": "run-1",
        "league": "Standard",
        "base_type": "Imbued Wand",
        "bucket_min": 1,
        "bucket_max": 15,
        "query_id": "q-1",
        "item_id": "item-1",
        "indexed": "2026-03-11T10:00:00Z",
        "account_name": "seller-a",
        "price_amount": 12.0,
        "price_currency": "chaos",
        "price_chaos": 12.0,
        "raw_item_json": '{"id":"item-1"}',
        "collected_at": "2026-03-11T10:01:00Z",
        "scan_profile": "default_bucket_scan",
        "query_shape": "online:type=Imbued Wand:price_chaos=1-15:sort=indexed_desc",
        "bucket_label": "1-15",
        "listing_age_seconds": 60.0,
        "search_batch": 1,
        "fetch_batch": 1,
    }


def test_trade_bucket_schema_and_dedupe_are_idempotent() -> None:
    conn = sqlite3.connect(":memory:")
    initialize_trade_bucket_database(conn)

    inserted_1, duplicates_1 = ingest_trade_bucket_rows(conn, [_sample_row()])
    inserted_2, duplicates_2 = ingest_trade_bucket_rows(conn, [_sample_row()])
    count = conn.execute("SELECT COUNT(*) FROM trade_bucket_events").fetchone()[0]

    assert inserted_1 == 1
    assert duplicates_1 == 0
    assert inserted_2 == 0
    assert duplicates_2 == 1
    assert count == 1

    row = conn.execute(
        "SELECT scan_profile, bucket_label, listing_age_seconds, search_batch, fetch_batch FROM trade_bucket_events"
    ).fetchone()
    assert row == ("default_bucket_scan", "1-15", 60.0, 1, 1)


def test_build_trade_query_uses_bucket_and_indexed_desc_sort() -> None:
    query = build_trade_query("Opal Ring", 16, 50)

    assert query["sort"] == {"indexed": "desc"}
    price_filter = query["query"]["filters"]["trade_filters"]["filters"]["price"]
    assert price_filter["min"] == 16
    assert price_filter["max"] == 50
    assert price_filter["option"] == "chaos"


def test_collect_trade_bucket_respects_bucket_and_run_quotas() -> None:
    conn = sqlite3.connect(":memory:")
    initialize_trade_bucket_database(conn)

    class _FakeClient:
        def __init__(self) -> None:
            self.search_calls = 0
            self.fetch_calls = 0

        def search_items(self, _query):
            self.search_calls += 1
            return f"q-{self.search_calls}", [f"item-{idx}" for idx in range(30)]

        def fetch_item_details(self, item_ids, _query_id):
            self.fetch_calls += 1
            return [
                {
                    "id": item_id,
                    "listing": {
                        "indexed": "2026-03-11T10:00:00Z",
                        "account": {"name": "seller"},
                        "price": {"currency": "chaos", "amount": 10.0},
                    },
                    "item": {
                        "id": item_id,
                        "baseType": "Imbued Wand",
                        "ilvl": 84,
                        "explicitMods": [],
                    },
                }
                for item_id in item_ids
            ]

    fake_client = _FakeClient()
    totals = collect_trade_bucket_events(
        client=cast(Any, fake_client),
        conn=conn,
        league="Standard",
        base_types=["Imbued Wand", "Opal Ring"],
        buckets=[(1, 15), (16, 50)],
        run_id="run-x",
        max_items_per_bucket=12,
        max_searches_per_run=2,
        max_fetches_per_run=3,
    )

    inserted_count = conn.execute(
        "SELECT COUNT(*) FROM trade_bucket_events"
    ).fetchone()[0]
    assert totals["searches"] == 2
    assert totals["fetches"] == 3
    assert totals["inserted"] == 12
    assert inserted_count == 12

    row = conn.execute(
        "SELECT scan_profile, query_shape, bucket_label, listing_age_seconds, search_batch, fetch_batch FROM trade_bucket_events ORDER BY id LIMIT 1"
    ).fetchone()
    assert row is not None
    assert row[0] == "default_bucket_scan"
    assert "type=Imbued Wand" in row[1]
    assert row[2] == "1-15"
    assert row[3] >= 0.0
    assert row[4] == 1
    assert row[5] == 1
