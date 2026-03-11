import sqlite3

import pandas as pd

from scripts.build_training_snapshot import build_training_snapshot


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
