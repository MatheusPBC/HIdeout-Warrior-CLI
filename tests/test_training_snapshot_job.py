import sqlite3

import pandas as pd

import json

from scripts.build_training_snapshot import (
    _enrich_bronze_observations,
    _stable_event_key,
    build_bronze_dataframe,
    build_gold_dataframe,
    build_silver_dataframe,
    build_training_snapshot,
)


def _sample_item(item_id: str, base_type: str = "Imbued Wand") -> dict:
    return {
        "id": item_id,
        "baseType": base_type,
        "ilvl": 84,
        "explicitMods": ["+#% increased Spell Damage", "+#% increased Cast Speed"],
        "implicitMods": [],
        "influences": {},
        "corrupted": False,
        "fractured": False,
    }


def test_build_training_snapshot_creates_layers_partitions_and_dedupes(
    tmp_path, monkeypatch
) -> None:
    def _fake_to_parquet(self, path, index=False):
        _ = index
        output_path = path if isinstance(path, str) else str(path)
        with open(output_path, "wb") as handle:
            handle.write(b"PAR1")

    monkeypatch.setattr(pd.DataFrame, "to_parquet", _fake_to_parquet, raising=False)

    db_path = tmp_path / "firehose.db"
    out_dir = tmp_path / "training_snapshots"

    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE stash_events (
            change_id TEXT,
            item_id TEXT,
            league TEXT,
            account_name TEXT,
            indexed TEXT,
            price_amount REAL,
            price_currency TEXT,
            price_chaos REAL,
            raw_item_json TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE trade_bucket_events (
            run_id TEXT,
            league TEXT,
            base_type TEXT,
            bucket_min INTEGER,
            bucket_max INTEGER,
            query_id TEXT,
            item_id TEXT,
            indexed TEXT,
            account_name TEXT,
            price_amount REAL,
            price_currency TEXT,
            price_chaos REAL,
            raw_item_json TEXT,
            collected_at TEXT
        )
        """
    )

    item_a = pd.Series(_sample_item("item-1")).to_json()
    item_b = pd.Series(_sample_item("item-2", base_type="Opal Ring")).to_json()
    rows = [
        (
            "change-1",
            "item-1",
            "Standard",
            "seller-a",
            "2026-03-11T10:00:00Z",
            40.0,
            "chaos",
            40.0,
            item_a,
        ),
        (
            "change-1",
            "item-1",
            "Standard",
            "seller-a",
            "2026-03-11T10:00:00Z",
            40.0,
            "chaos",
            40.0,
            item_a,
        ),
        (
            "change-2",
            "item-1",
            "Standard",
            "seller-a",
            "2026-03-11T10:05:00Z",
            55.0,
            "chaos",
            55.0,
            item_a,
        ),
        (
            "change-3",
            "item-2",
            "Standard",
            "seller-b",
            "2026-03-11T10:10:00Z",
            15.0,
            "chaos",
            15.0,
            item_b,
        ),
        (
            "change-4",
            "item-bad",
            "Standard",
            "seller-c",
            "2026-03-11T10:15:00Z",
            1.0,
            "chaos",
            1.0,
            "{invalid-json}",
        ),
    ]
    conn.executemany(
        "INSERT INTO stash_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows
    )
    conn.execute(
        "INSERT INTO trade_bucket_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "run-1",
            "Standard",
            "Vaal Regalia",
            151,
            500,
            "query-1",
            "item-3",
            "2026-03-11T10:20:00Z",
            "seller-d",
            120.0,
            "chaos",
            120.0,
            pd.Series(_sample_item("item-3", base_type="Vaal Regalia")).to_json(),
            "2026-03-11T10:20:05Z",
        ),
    )
    conn.commit()
    conn.close()

    summary = build_training_snapshot(
        db_path=str(db_path),
        output_dir=str(out_dir),
        snapshot_date="2026-03-11",
    )

    assert summary["invalid_json_skipped"] == 1
    assert summary["bronze_rows"] == 4
    assert summary["silver_rows"] >= 2
    assert summary["gold_rows"] >= 1

    bronze_partition = (
        out_dir / "bronze" / "snapshot_date=2026-03-11" / "league=Standard"
    )
    assert bronze_partition.exists()

    silver_item_partitions = list((out_dir / "silver").rglob("item_family=*"))
    gold_item_partitions = list((out_dir / "gold").rglob("item_family=*"))
    assert silver_item_partitions
    assert gold_item_partitions


def test_snapshot_contract_reconciles_sources_and_carries_freshness(tmp_path) -> None:
    db_path = tmp_path / "firehose.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE stash_events (
            change_id TEXT,
            item_id TEXT,
            league TEXT,
            account_name TEXT,
            stash_name TEXT,
            indexed TEXT,
            price_amount REAL,
            price_currency TEXT,
            price_chaos REAL,
            raw_item_json TEXT,
            collected_at TEXT,
            oauth_source TEXT,
            oauth_scope TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE trade_bucket_events (
            run_id TEXT,
            league TEXT,
            base_type TEXT,
            bucket_min INTEGER,
            bucket_max INTEGER,
            query_id TEXT,
            item_id TEXT,
            indexed TEXT,
            account_name TEXT,
            price_amount REAL,
            price_currency TEXT,
            price_chaos REAL,
            raw_item_json TEXT,
            collected_at TEXT,
            scan_profile TEXT,
            query_shape TEXT,
            bucket_label TEXT,
            listing_age_seconds REAL,
            search_batch INTEGER,
            fetch_batch INTEGER
        )
        """
    )

    raw_item = pd.Series(
        _sample_item("item-shared", base_type="Vaal Regalia")
    ).to_json()
    conn.execute(
        "INSERT INTO stash_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "change-1",
            "item-shared",
            "Standard",
            "seller-a",
            "stash-a",
            "2026-03-11T10:00:00Z",
            50.0,
            "chaos",
            50.0,
            raw_item,
            "2026-03-11T10:05:00Z",
            "client_credentials",
            "service:psapi",
        ),
    )
    conn.execute(
        "INSERT INTO trade_bucket_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "run-1",
            "Standard",
            "Vaal Regalia",
            16,
            50,
            "query-1",
            "item-shared",
            "2026-03-11T10:00:00Z",
            "seller-a",
            52.0,
            "chaos",
            52.0,
            raw_item,
            "2026-03-11T10:02:00Z",
            "default_bucket_scan",
            "online:type=Vaal Regalia:price_chaos=16-50:sort=indexed_desc",
            "16-50",
            120.0,
            1,
            1,
        ),
    )
    conn.commit()
    conn.close()

    bronze_df, stats = build_bronze_dataframe(str(db_path), snapshot_date="2026-03-11")

    assert stats["rows_valid"] == 2
    assert set(bronze_df["source"]) == {"both"}
    assert set(bronze_df["source_count"]) == {2}
    assert set(bronze_df["seen_count"]) == {2}
    assert set(bronze_df["freshness_band"]) == {"fresh"}
    assert bronze_df["listing_age_seconds"].max() == 300.0
    assert bronze_df["listing_age_seconds"].min() == 120.0

    silver_df = build_silver_dataframe(bronze_df)
    gold_df = build_gold_dataframe(silver_df)

    assert {
        "source",
        "source_count",
        "seen_count",
        "listing_age_seconds",
        "freshness_band",
    }.issubset(set(silver_df.columns))
    assert {
        "source",
        "source_count",
        "seen_count",
        "listing_age_seconds",
        "freshness_band",
    }.issubset(set(gold_df.columns))
    assert set(gold_df["source"]) == {"both"}

    # Validate first_seen_at and last_seen_at timestamps
    assert "first_seen_at" in bronze_df.columns
    assert "last_seen_at" in bronze_df.columns
    assert "first_seen_at" in silver_df.columns
    assert "last_seen_at" in silver_df.columns
    assert "first_seen_at" in gold_df.columns
    assert "last_seen_at" in gold_df.columns

    # The shared item has indexed="2026-03-11T10:00:00Z" for both sources,
    # so first_seen_at and last_seen_at will both be "2026-03-11T10:00:00Z"
    # (based on indexed_at, not collected_at)
    first_seen_values = set(bronze_df["first_seen_at"])
    last_seen_values = set(bronze_df["last_seen_at"])
    assert first_seen_values == {"2026-03-11T10:00:00Z"}
    assert last_seen_values == {"2026-03-11T10:00:00Z"}

    # Silver and gold should preserve the timestamps
    assert set(silver_df["first_seen_at"]) == {"2026-03-11T10:00:00Z"}
    assert set(silver_df["last_seen_at"]) == {"2026-03-11T10:00:00Z"}
    assert set(gold_df["first_seen_at"]) == {"2026-03-11T10:00:00Z"}
    assert set(gold_df["last_seen_at"]) == {"2026-03-11T10:00:00Z"}


def test_query_context_preserved_through_pipeline_stash_source(tmp_path) -> None:
    """Validate query_context is built and carried through bronze->silver->gold for stash source."""
    import json

    db_path = tmp_path / "firehose.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE stash_events (
            change_id TEXT,
            item_id TEXT,
            league TEXT,
            account_name TEXT,
            stash_name TEXT,
            indexed TEXT,
            price_amount REAL,
            price_currency TEXT,
            price_chaos REAL,
            raw_item_json TEXT,
            collected_at TEXT,
            oauth_source TEXT,
            oauth_scope TEXT
        )
        """
    )

    raw_item = pd.Series(
        _sample_item("item-stash-only", base_type="Titanium Spirit Shield")
    ).to_json()
    conn.execute(
        "INSERT INTO stash_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "change-stash-1",
            "item-stash-only",
            "Standard",
            "seller-x",
            "My Stash",
            "2026-03-11T10:00:00Z",
            80.0,
            "chaos",
            80.0,
            raw_item,
            "2026-03-11T10:01:00Z",
            "client_credentials",
            "service:psapi",
        ),
    )
    conn.commit()
    conn.close()

    bronze_df, _ = build_bronze_dataframe(str(db_path), snapshot_date="2026-03-11")
    silver_df = build_silver_dataframe(bronze_df)
    gold_df = build_gold_dataframe(silver_df)

    # Validate query_context exists in all layers
    assert "query_context" in bronze_df.columns
    assert "query_context" in silver_df.columns
    assert "query_context" in gold_df.columns

    # Validate query_context structure for stash_events
    bronze_ctx = json.loads(bronze_df.iloc[0]["query_context"])
    assert bronze_ctx["source_table"] == "stash_events"
    assert bronze_ctx["change_id"] == "change-stash-1"
    assert bronze_ctx["stash_name"] == "My Stash"
    assert bronze_ctx["oauth_source"] == "client_credentials"
    assert bronze_ctx["oauth_scope"] == "service:psapi"

    # Silver and gold preserve the query_context
    silver_ctx = json.loads(silver_df.iloc[0]["query_context"])
    gold_ctx = json.loads(gold_df.iloc[0]["query_context"])
    assert silver_ctx == bronze_ctx
    assert gold_ctx == bronze_ctx


def test_first_seen_last_seen_tracks_indexed_differences(tmp_path) -> None:
    """Validate first_seen_at and last_seen_at correctly track different indexed times."""
    db_path = tmp_path / "firehose.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE stash_events (
            change_id TEXT,
            item_id TEXT,
            league TEXT,
            account_name TEXT,
            stash_name TEXT,
            indexed TEXT,
            price_amount REAL,
            price_currency TEXT,
            price_chaos REAL,
            raw_item_json TEXT,
            collected_at TEXT,
            oauth_source TEXT,
            oauth_scope TEXT
        )
        """
    )

    raw_item = pd.Series(
        _sample_item("item-dated", base_type="Large Cluster Jewel")
    ).to_json()

    # First observation: indexed at 10:00
    conn.execute(
        "INSERT INTO stash_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "change-first",
            "item-dated",
            "Standard",
            "seller-a",
            "stash-1",
            "2026-03-11T10:00:00Z",
            50.0,
            "chaos",
            50.0,
            raw_item,
            "2026-03-11T10:00:05Z",
            "",
            "",
        ),
    )
    # Second observation (same item, different indexed time): indexed at 10:30
    conn.execute(
        "INSERT INTO stash_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "change-second",
            "item-dated",
            "Standard",
            "seller-b",
            "stash-2",
            "2026-03-11T10:30:00Z",
            48.0,
            "chaos",
            48.0,
            raw_item,
            "2026-03-11T10:30:05Z",
            "",
            "",
        ),
    )
    conn.commit()
    conn.close()

    bronze_df, _ = build_bronze_dataframe(str(db_path), snapshot_date="2026-03-11")
    silver_df = build_silver_dataframe(bronze_df)
    gold_df = build_gold_dataframe(silver_df)

    # With different indexed times, first_seen should be 10:00 and last_seen should be 10:30
    assert set(bronze_df["first_seen_at"]) == {"2026-03-11T10:00:00Z"}
    assert set(bronze_df["last_seen_at"]) == {"2026-03-11T10:30:00Z"}
    assert set(bronze_df["seen_count"]) == {2}

    # Silver and gold should preserve these timestamps
    assert set(silver_df["first_seen_at"]) == {"2026-03-11T10:00:00Z"}
    assert set(silver_df["last_seen_at"]) == {"2026-03-11T10:30:00Z"}
    assert set(gold_df["first_seen_at"]) == {"2026-03-11T10:00:00Z"}
    assert set(gold_df["last_seen_at"]) == {"2026-03-11T10:30:00Z"}


def test_query_context_preserved_through_pipeline_trade_source(tmp_path) -> None:
    """Validate query_context is built and carried through bronze->silver->gold for trade source."""
    import json

    db_path = tmp_path / "firehose.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE trade_bucket_events (
            run_id TEXT,
            league TEXT,
            base_type TEXT,
            bucket_min INTEGER,
            bucket_max INTEGER,
            query_id TEXT,
            item_id TEXT,
            indexed TEXT,
            account_name TEXT,
            price_amount REAL,
            price_currency TEXT,
            price_chaos REAL,
            raw_item_json TEXT,
            collected_at TEXT,
            scan_profile TEXT,
            query_shape TEXT,
            bucket_label TEXT,
            listing_age_seconds REAL,
            search_batch INTEGER,
            fetch_batch INTEGER
        )
        """
    )

    raw_item = pd.Series(
        _sample_item("item-trade-only", base_type="Opal Ring")
    ).to_json()
    conn.execute(
        "INSERT INTO trade_bucket_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "run-trade-1",
            "Standard",
            "Opal Ring",
            16,
            50,
            "query-trade-1",
            "item-trade-only",
            "2026-03-11T10:00:00Z",
            "seller-y",
            45.0,
            "chaos",
            45.0,
            raw_item,
            "2026-03-11T10:00:30Z",
            "default_bucket_scan",
            "online:type=Opal Ring:price_chaos=16-50:sort=indexed_desc",
            "16-50",
            30.0,
            1,
            1,
        ),
    )
    conn.commit()
    conn.close()

    bronze_df, _ = build_bronze_dataframe(str(db_path), snapshot_date="2026-03-11")
    silver_df = build_silver_dataframe(bronze_df)
    gold_df = build_gold_dataframe(silver_df)

    # Validate query_context exists in all layers
    assert "query_context" in bronze_df.columns
    assert "query_context" in silver_df.columns
    assert "query_context" in gold_df.columns

    # Validate query_context structure for trade_bucket_events
    bronze_ctx = json.loads(bronze_df.iloc[0]["query_context"])
    assert bronze_ctx["source_table"] == "trade_bucket_events"
    assert bronze_ctx["query_id"] == "query-trade-1"
    assert bronze_ctx["base_type"] == "Opal Ring"
    assert bronze_ctx["bucket_min"] == 16
    assert bronze_ctx["bucket_max"] == 50
    assert bronze_ctx["bucket_label"] == "16-50"
    assert bronze_ctx["scan_profile"] == "default_bucket_scan"
    assert (
        bronze_ctx["query_shape"]
        == "online:type=Opal Ring:price_chaos=16-50:sort=indexed_desc"
    )
    assert bronze_ctx["search_batch"] == 1
    assert bronze_ctx["fetch_batch"] == 1

    # Silver and gold preserve the query_context
    silver_ctx = json.loads(silver_df.iloc[0]["query_context"])
    gold_ctx = json.loads(gold_df.iloc[0]["query_context"])
    assert silver_ctx == bronze_ctx
    assert gold_ctx == bronze_ctx


def test_enrich_bronze_observations_sets_price_fix_suspected_on_price_anomaly(
    tmp_path,
) -> None:
    """Validate price_fix_suspected=True when same item has large price variation."""
    db_path = tmp_path / "firehose.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE stash_events (
            change_id TEXT,
            item_id TEXT,
            league TEXT,
            account_name TEXT,
            indexed TEXT,
            price_amount REAL,
            price_currency TEXT,
            price_chaos REAL,
            raw_item_json TEXT
        )
        """
    )

    raw_item = pd.Series(_sample_item("item-price-fix")).to_json()
    # Same item with price 10 chaos first, then 20 chaos (2x difference triggers anomaly)
    rows = [
        (
            "change-1",
            "item-price-fix",
            "Standard",
            "seller-a",
            "2026-03-11T10:00:00Z",
            10.0,
            "chaos",
            10.0,
            raw_item,
        ),
        (
            "change-2",
            "item-price-fix",
            "Standard",
            "seller-b",
            "2026-03-11T10:05:00Z",
            20.0,
            "chaos",
            20.0,
            raw_item,
        ),
    ]
    conn.executemany(
        "INSERT INTO stash_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows
    )
    conn.commit()
    conn.close()

    bronze_df, _ = build_bronze_dataframe(str(db_path), snapshot_date="2026-03-11")

    assert "price_fix_suspected" in bronze_df.columns
    assert bool(bronze_df["price_fix_suspected"].any()), (
        "price_fix_suspected should be True when price varies by >50%"
    )


def test_enrich_bronze_observations_price_fix_false_when_prices_stable(
    tmp_path,
) -> None:
    """Validate price_fix_suspected=False when same item has stable prices."""
    db_path = tmp_path / "firehose.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE stash_events (
            change_id TEXT,
            item_id TEXT,
            league TEXT,
            account_name TEXT,
            indexed TEXT,
            price_amount REAL,
            price_currency TEXT,
            price_chaos REAL,
            raw_item_json TEXT
        )
        """
    )

    raw_item = pd.Series(_sample_item("item-stable")).to_json()
    # Same item with similar prices (ratio < 1.5 should NOT trigger anomaly)
    rows = [
        (
            "change-1",
            "item-stable",
            "Standard",
            "seller-a",
            "2026-03-11T10:00:00Z",
            10.0,
            "chaos",
            10.0,
            raw_item,
        ),
        (
            "change-2",
            "item-stable",
            "Standard",
            "seller-b",
            "2026-03-11T10:05:00Z",
            12.0,
            "chaos",
            12.0,
            raw_item,
        ),
    ]
    conn.executemany(
        "INSERT INTO stash_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows
    )
    conn.commit()
    conn.close()

    bronze_df, _ = build_bronze_dataframe(str(db_path), snapshot_date="2026-03-11")

    assert "price_fix_suspected" in bronze_df.columns
    # With ratio 12/10=1.2 (<1.5), price_fix_suspected should be False
    price_fix_row = bronze_df[bronze_df["item_id"] == "item-stable"]
    assert not bool(price_fix_row["price_fix_suspected"].any()), (
        "price_fix_suspected should be False when price ratio < 1.5x"
    )


def test_build_training_snapshot_returns_complete_bronze_silver_gold_summary(
    tmp_path, monkeypatch
) -> None:
    """Validate build_training_snapshot returns summary with full bronze/silver/gold metrics structure."""

    def _fake_to_parquet(self, path, index=False):
        output_path = path if isinstance(path, str) else str(path)
        with open(output_path, "wb") as handle:
            handle.write(b"PAR1")

    monkeypatch.setattr(pd.DataFrame, "to_parquet", _fake_to_parquet, raising=False)

    db_path = tmp_path / "firehose.db"
    out_dir = tmp_path / "training_snapshots"

    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE stash_events (
            change_id TEXT,
            item_id TEXT,
            league TEXT,
            account_name TEXT,
            indexed TEXT,
            price_amount REAL,
            price_currency TEXT,
            price_chaos REAL,
            raw_item_json TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE trade_bucket_events (
            run_id TEXT,
            league TEXT,
            base_type TEXT,
            bucket_min INTEGER,
            bucket_max INTEGER,
            query_id TEXT,
            item_id TEXT,
            indexed TEXT,
            account_name TEXT,
            price_amount REAL,
            price_currency TEXT,
            price_chaos REAL,
            raw_item_json TEXT,
            collected_at TEXT
        )
        """
    )

    item = pd.Series(_sample_item("item-1")).to_json()
    conn.execute(
        "INSERT INTO stash_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "change-1",
            "item-1",
            "Standard",
            "seller",
            "2026-03-11T10:00:00Z",
            40.0,
            "chaos",
            40.0,
            item,
        ),
    )
    conn.commit()
    conn.close()

    summary = build_training_snapshot(
        db_path=str(db_path),
        output_dir=str(out_dir),
        snapshot_date="2026-03-11",
    )

    # Validate top-level fields
    assert "snapshot_date" in summary
    assert summary["snapshot_date"] == "2026-03-11"

    # Validate bronze structure
    assert "bronze" in summary
    bronze = summary["bronze"]
    assert "rows" in bronze
    assert "rows_read" in bronze
    assert "rows_valid" in bronze
    assert "rows_deduped" in bronze
    assert "partitions" in bronze
    assert "source_distribution" in bronze
    assert "freshness_distribution" in bronze
    assert "dedup_rate" in bronze

    # Validate silver structure
    assert "silver" in summary
    silver = summary["silver"]
    assert "rows" in silver
    assert "rows_input" in silver
    assert "rows_output" in silver
    assert "normalization_failures" in silver
    assert "partitions" in silver

    # Validate gold structure
    assert "gold" in summary
    gold = summary["gold"]
    assert "rows" in gold
    assert "rows_input" in gold
    assert "rows_output" in gold
    assert "feature_extraction_failures" in gold
    assert "partitions" in gold

    # Validate legacy top-level fields still present
    assert "bronze_rows" in summary
    assert "silver_rows" in summary
    assert "gold_rows" in summary
    assert "invalid_json_skipped" in summary


def test_stable_event_key_is_deterministic() -> None:
    """Validate _stable_event_key returns same hash for identical inputs."""
    row = {
        "source_table": "stash_events",
        "account_name": "seller",
        "change_id": "change-1",
        "query_id": "",
        "item_id": "item-1",
        "base_type": "",
        "indexed": "2026-03-11T10:00:00Z",
        "price_chaos": 40.0,
    }
    normalized_json = json.dumps({"id": "item-1"}, sort_keys=True)

    key1 = _stable_event_key(row, normalized_json)
    key2 = _stable_event_key(row, normalized_json)

    assert key1 == key2
    assert len(key1) == 40  # SHA1 hex length
