import sqlite3

from scripts.firehose_miner import (
    ingest_stash_page,
    initialize_database,
    is_useful_item,
    parse_price_note,
    update_checkpoint,
)


def test_parse_price_note_supports_known_currencies() -> None:
    assert parse_price_note("~b/o 12 chaos") == (12.0, "chaos")
    assert parse_price_note("~price 2 divine") == (2.0, "divine")
    assert parse_price_note("~b/o 3 exa") == (3.0, "exalted")
    assert parse_price_note("~price 1 alch") == (1.0, "alchemy")
    assert parse_price_note("price 12 chaos") == (None, None)


def test_is_useful_item_filters_rarity_and_price() -> None:
    rare_item = {"frameType": 2, "note": "~price 10 chaos"}
    unique_item = {"frameType": 3, "note": "~b/o 1 divine"}
    magic_item = {"frameType": 1, "note": "~price 10 chaos"}
    no_price_item = {"frameType": 2, "note": "~price 0 chaos"}

    assert is_useful_item(rare_item)[0] is True
    assert is_useful_item(unique_item)[0] is True
    assert is_useful_item(magic_item)[0] is False
    assert is_useful_item(no_price_item)[0] is False


def test_ingest_is_idempotent_by_change_and_item_id() -> None:
    conn = sqlite3.connect(":memory:")
    initialize_database(conn)
    payload = {
        "stashes": [
            {
                "stash": "s1",
                "league": "Standard",
                "accountName": "seller",
                "items": [
                    {
                        "id": "item-1",
                        "frameType": 2,
                        "note": "~price 5 chaos",
                        "baseType": "Imbued Wand",
                        "name": "",
                        "ilvl": 84,
                        "indexed": "2026-03-11T10:00:00Z",
                    }
                ],
            }
        ]
    }

    inserted_1, duplicates_1 = ingest_stash_page(conn, payload, change_id="change-a")
    inserted_2, duplicates_2 = ingest_stash_page(conn, payload, change_id="change-a")
    rows = conn.execute("SELECT COUNT(*) FROM stash_events").fetchone()[0]

    assert inserted_1 == 1
    assert duplicates_1 == 0
    assert inserted_2 == 0
    assert duplicates_2 == 1
    assert rows == 1


def test_checkpoint_updates_after_success() -> None:
    conn = sqlite3.connect(":memory:")
    initialize_database(conn)

    payload = {
        "stashes": [
            {
                "stash": "s1",
                "league": "Standard",
                "accountName": "seller",
                "items": [
                    {
                        "id": "item-2",
                        "frameType": 3,
                        "note": "~b/o 1 divine",
                        "baseType": "Vaal Regalia",
                        "name": "",
                        "ilvl": 86,
                        "indexed": "2026-03-11T11:00:00Z",
                    }
                ],
            }
        ]
    }

    inserted, duplicates = ingest_stash_page(conn, payload, change_id="change-b")
    update_checkpoint(
        conn,
        next_change_id="next-123",
        pages_delta=1,
        ingested_delta=inserted,
        duplicates_delta=duplicates,
    )

    checkpoint = conn.execute(
        "SELECT next_change_id, pages_processed, events_ingested, duplicates_skipped FROM miner_checkpoint WHERE id = 1"
    ).fetchone()

    assert checkpoint == ("next-123", 1, 1, 0)
