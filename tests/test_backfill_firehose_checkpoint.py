"""Testes para scripts/backfill_firehose_checkpoint.py - sync SQLite → Supabase."""

import sqlite3
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from scripts.backfill_firehose_checkpoint import main


class _FakeQueryBuilder:
    def __init__(self, result: Any = None) -> None:
        self._result = result

    def execute(self):
        return self._result or {"status": "ok"}


class _FakeTableCheckpoint:
    def __init__(self, result: Any) -> None:
        self._result = result

    def select(self, *_args):
        return self

    def eq(self, *_args):
        return self

    def maybe_single(self):
        return self

    def execute(self):
        return self._result


class _FakeSupabaseClient:
    def __init__(self, checkpoint_result: Any = None) -> None:
        self._checkpoint_result = checkpoint_result

    def table(self, _name: str):
        return _FakeTableCheckpoint(self._checkpoint_result)


def test_backfill_skips_when_sqlite_has_no_checkpoint(
    tmp_path, monkeypatch, capsys
) -> None:
    """Quando SQLite não tem checkpoint, script deve sair com 0 (nada a fazer)."""
    db_path = tmp_path / "empty.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "CREATE TABLE miner_checkpoint (id INTEGER PRIMARY KEY, next_change_id TEXT, pages_processed INTEGER, events_ingested INTEGER, duplicates_skipped INTEGER)"
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr("sys.argv", ["backfill", "--db-path", str(db_path)])
    monkeypatch.setattr(
        "scripts.backfill_firehose_checkpoint.load_cloud_config",
        lambda: MagicMock(
            backend="supabase",
            is_configured=True,
            project_url="https://demo.supabase.co",
            service_role_key="key",
        ),
    )

    exit_code = main()

    assert exit_code == 0
    captured = capsys.readouterr()
    assert "Nenhum checkpoint encontrado" in captured.out


def test_backfill_skips_when_supabase_has_newer_checkpoint(
    tmp_path, monkeypatch, capsys
) -> None:
    """Quando Supabase já tem checkpoint mais recente, não deve fazer overwrite."""
    db_path = tmp_path / "firehose.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "CREATE TABLE miner_checkpoint (id INTEGER PRIMARY KEY, next_change_id TEXT, pages_processed INTEGER, events_ingested INTEGER, duplicates_skipped INTEGER)"
    )
    conn.execute(
        "INSERT INTO miner_checkpoint (id, next_change_id, pages_processed, events_ingested, duplicates_skipped) VALUES (1, 'old-change', 5, 100, 10)"
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr("sys.argv", ["backfill", "--db-path", str(db_path)])
    monkeypatch.setattr(
        "scripts.backfill_firehose_checkpoint.load_cloud_config",
        lambda: MagicMock(
            backend="supabase",
            is_configured=True,
            project_url="https://demo.supabase.co",
            service_role_key="key",
        ),
    )

    fake_cloud_cp = MagicMock()
    fake_cloud_cp.data = {
        "next_change_id": "newer-change",
        "pages_processed": 10,  # maior que SQLite
        "events_ingested": 200,
        "duplicates_skipped": 20,
    }
    monkeypatch.setattr(
        "scripts.backfill_firehose_checkpoint.load_checkpoint_from_supabase",
        lambda config: {
            "next_change_id": "newer-change",
            "pages_processed": 10,
            "events_ingested": 200,
            "duplicates_skipped": 20,
        },
    )

    exit_code = main()

    assert exit_code == 0
    captured = capsys.readouterr()
    assert "skipping backfill" in captured.out.lower()


def test_backfill_syncs_sqlite_to_supabase_when_cloud_is_behind(
    tmp_path, monkeypatch, capsys
) -> None:
    """Quando SQLite tem checkpoint mais recente, deve sincronizar para Supabase."""
    db_path = tmp_path / "firehose.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "CREATE TABLE miner_checkpoint (id INTEGER PRIMARY KEY, next_change_id TEXT, pages_processed INTEGER, events_ingested INTEGER, duplicates_skipped INTEGER)"
    )
    conn.execute(
        "INSERT INTO miner_checkpoint (id, next_change_id, pages_processed, events_ingested, duplicates_skipped) VALUES (1, 'local-change', 10, 200, 20)"
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr("sys.argv", ["backfill", "--db-path", str(db_path)])
    monkeypatch.setattr(
        "scripts.backfill_firehose_checkpoint.load_cloud_config",
        lambda: MagicMock(
            backend="supabase",
            is_configured=True,
            project_url="https://demo.supabase.co",
            service_role_key="key",
        ),
    )

    # Cloud com checkpoint menor
    monkeypatch.setattr(
        "scripts.backfill_firehose_checkpoint.load_checkpoint_from_supabase",
        lambda config: {
            "next_change_id": "old-change",
            "pages_processed": 5,
            "events_ingested": 100,
            "duplicates_skipped": 10,
        },
    )

    sync_calls = []

    def _track_sync(**kwargs):
        sync_calls.append(kwargs)
        return True

    monkeypatch.setattr(
        "scripts.backfill_firehose_checkpoint.sync_firehose_checkpoint_to_supabase",
        _track_sync,
    )

    exit_code = main()

    assert exit_code == 0
    assert len(sync_calls) == 1
    assert sync_calls[0]["next_change_id"] == "local-change"
    assert sync_calls[0]["pages_processed"] == 10
    captured = capsys.readouterr()
    assert "sincronizado" in captured.out.lower()


def test_backfill_exits_early_when_supabase_not_configured(
    tmp_path, monkeypatch, capsys
) -> None:
    """Quando Supabase não está configurado, deve sair com 0 sem modificar nada."""
    db_path = tmp_path / "firehose.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "CREATE TABLE miner_checkpoint (id INTEGER PRIMARY KEY, next_change_id TEXT, pages_processed INTEGER, events_ingested INTEGER, duplicates_skipped INTEGER)"
    )
    conn.execute(
        "INSERT INTO miner_checkpoint (id, next_change_id, pages_processed, events_ingested, duplicates_skipped) VALUES (1, 'local', 1, 10, 0)"
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr("sys.argv", ["backfill", "--db-path", str(db_path)])
    monkeypatch.setattr(
        "scripts.backfill_firehose_checkpoint.load_cloud_config",
        lambda: MagicMock(backend="local", is_configured=False),
    )

    exit_code = main()

    assert exit_code == 0
    captured = capsys.readouterr()
    assert "não configurado" in captured.out


def test_backfill_fails_when_sqlite_db_missing(monkeypatch, capsys) -> None:
    """Quando banco SQLite não existe, deve retornar 1."""
    monkeypatch.setattr(
        "sys.argv", ["backfill", "--db-path", "/nonexistent/path/firehose.db"]
    )
    monkeypatch.setattr(
        "scripts.backfill_firehose_checkpoint.load_cloud_config",
        lambda: MagicMock(
            backend="supabase",
            is_configured=True,
            project_url="https://demo.supabase.co",
            service_role_key="key",
        ),
    )
    monkeypatch.setattr(
        "scripts.backfill_firehose_checkpoint.Path",
        lambda x: Path("/nonexistent/path/firehose.db"),
    )

    exit_code = main()

    assert exit_code == 1
    captured = capsys.readouterr()
    assert "não encontrado" in captured.out


def test_backfill_reports_failure_when_sync_returns_false(
    tmp_path, monkeypatch, capsys
) -> None:
    """Quando sync_firehose_checkpoint_to_supabase retorna False, deve retornar 1."""
    db_path = tmp_path / "firehose.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "CREATE TABLE miner_checkpoint (id INTEGER PRIMARY KEY, next_change_id TEXT, pages_processed INTEGER, events_ingested INTEGER, duplicates_skipped INTEGER)"
    )
    conn.execute(
        "INSERT INTO miner_checkpoint (id, next_change_id, pages_processed, events_ingested, duplicates_skipped) VALUES (1, 'local-fail', 5, 50, 5)"
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr("sys.argv", ["backfill", "--db-path", str(db_path)])
    monkeypatch.setattr(
        "scripts.backfill_firehose_checkpoint.load_cloud_config",
        lambda: MagicMock(
            backend="supabase",
            is_configured=True,
            project_url="https://demo.supabase.co",
            service_role_key="key",
        ),
    )
    monkeypatch.setattr(
        "scripts.backfill_firehose_checkpoint.load_checkpoint_from_supabase",
        lambda config: None,
    )
    monkeypatch.setattr(
        "scripts.backfill_firehose_checkpoint.sync_firehose_checkpoint_to_supabase",
        lambda **kwargs: False,
    )

    exit_code = main()

    assert exit_code == 1
    captured = capsys.readouterr()
    assert "Falha ao sincronizar" in captured.out
