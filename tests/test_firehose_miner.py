import sqlite3
from typing import Any, cast

import pytest
import typer

from scripts.firehose_miner import (
    fetch_stash_page,
    run,
    ingest_stash_page,
    initialize_database,
    is_useful_item,
    parse_price_note,
    update_checkpoint,
)
from core.poe_oauth import OAuthAccessToken


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


def test_ingest_persists_collection_and_oauth_metadata() -> None:
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
                        "id": "item-meta-1",
                        "frameType": 2,
                        "note": "~price 9 chaos",
                        "baseType": "Imbued Wand",
                        "name": "",
                        "ilvl": 84,
                        "indexed": "2026-03-11T10:00:00Z",
                    }
                ],
            }
        ]
    }

    ingest_stash_page(
        conn,
        payload,
        change_id="change-meta",
        collected_at="2026-03-11T10:01:00Z",
        oauth_source="client_credentials",
        oauth_scope="service:psapi",
    )

    row = conn.execute(
        "SELECT collected_at, oauth_source, oauth_scope FROM stash_events WHERE item_id = ?",
        ("item-meta-1",),
    ).fetchone()

    assert row == (
        "2026-03-11T10:01:00Z",
        "client_credentials",
        "service:psapi",
    )


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


def test_run_emits_operational_metric(tmp_path, monkeypatch) -> None:
    payload = {
        "next_change_id": "next-1",
        "stashes": [
            {
                "stash": "s1",
                "league": "Standard",
                "accountName": "seller",
                "items": [
                    {
                        "id": "item-3",
                        "frameType": 2,
                        "note": "~price 7 chaos",
                        "baseType": "Imbued Wand",
                        "name": "",
                        "ilvl": 84,
                        "indexed": "2026-03-11T10:00:00Z",
                    }
                ],
            }
        ],
    }
    captured = {}

    monkeypatch.setattr(
        "scripts.firehose_miner.fetch_stash_page",
        lambda *_args, **_kwargs: payload,
    )

    def _capture_metric(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr("scripts.firehose_miner.append_metric_event", _capture_metric)

    run(
        db_path=str(tmp_path / "firehose.db"),
        start_change_id="boot",
        max_pages=1,
        sleep_seconds=0.0,
    )

    assert captured["component"] == "firehose_miner.run"
    assert captured["status"] == "ok"
    assert captured["payload"]["pages_processed"] == 1
    assert "throughput_items_per_sec" in captured["payload"]


def test_run_resolves_oauth_via_client_credentials(tmp_path, monkeypatch) -> None:
    payload = {
        "next_change_id": "next-1",
        "stashes": [
            {
                "stash": "s1",
                "league": "Standard",
                "accountName": "seller",
                "items": [],
            }
        ],
    }
    captured = {}

    def _fake_resolve(**kwargs):
        captured.update(kwargs)
        return OAuthAccessToken(
            access_token="generated-token",
            scope="service:psapi",
            source="client_credentials",
        )

    def _fake_fetch(session, *_args, **_kwargs):
        assert session.headers["Authorization"] == "Bearer generated-token"
        return payload

    monkeypatch.setattr(
        "scripts.firehose_miner.resolve_service_oauth_token", _fake_resolve
    )
    monkeypatch.setattr("scripts.firehose_miner.fetch_stash_page", _fake_fetch)

    run(
        db_path=str(tmp_path / "firehose.db"),
        start_change_id="boot",
        max_pages=1,
        sleep_seconds=0.0,
        oauth_client_id="client-id",
        oauth_client_secret="client-secret",
    )

    assert captured["client_id"] == "client-id"
    assert captured["client_secret"] == "client-secret"
    assert captured["scope"] == "service:psapi"


def test_fetch_stash_page_raises_permission_error_on_oauth_forbidden() -> None:
    class _Resp:
        status_code = 403

        @staticmethod
        def json():
            return {
                "error": {
                    "code": 6,
                    "message": "Forbidden; You must use an OAuth client to access this endpoint",
                }
            }

        @staticmethod
        def raise_for_status():
            raise AssertionError("raise_for_status should not be called")

    class _Session:
        @staticmethod
        def get(*_args, **_kwargs):
            return _Resp()

    with pytest.raises(PermissionError):
        fetch_stash_page(cast(Any, _Session()), next_change_id=None, max_retries=1)


def test_fetch_stash_page_raises_permission_error_on_unauthorized() -> None:
    class _Resp:
        status_code = 401

        @staticmethod
        def json():
            return {
                "error": {
                    "code": 8,
                    "message": "Unauthorized",
                }
            }

        @staticmethod
        def raise_for_status():
            raise AssertionError("raise_for_status should not be called")

    class _Session:
        @staticmethod
        def get(*_args, **_kwargs):
            return _Resp()

    with pytest.raises(PermissionError):
        fetch_stash_page(cast(Any, _Session()), next_change_id=None, max_retries=1)


def test_run_exits_cleanly_on_oauth_permission_error(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        "scripts.firehose_miner.fetch_stash_page",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            PermissionError("oauth required")
        ),
    )

    with pytest.raises(typer.Exit):
        run(
            db_path=str(tmp_path / "firehose.db"),
            start_change_id="boot",
            max_pages=1,
            sleep_seconds=0.0,
        )
