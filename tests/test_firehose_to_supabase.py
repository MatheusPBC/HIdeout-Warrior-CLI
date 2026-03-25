"""Testes para scripts/firehose_to_supabase.py - upload de NDJSON landing para Supabase."""

import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from scripts.firehose_to_supabase import (
    _ndjson_records,
    _upload_ndjson,
    process_firehose_raw,
)


class _FakeQueryBuilder:
    def __init__(self, result: Any = None) -> None:
        self._result = result

    def execute(self):
        return self._result or {"status": "ok"}


class _FakeBucket:
    def __init__(self, sink: list[tuple[str, str, dict]]) -> None:
        self.sink = sink

    def upload(self, *, path: str, file: Any, file_options: dict) -> dict:
        content = file.read()
        if isinstance(content, bytes):
            content = content.decode("utf-8")
        self.sink.append((path, content, file_options))
        return {"path": path}


class _FakeStorage:
    def __init__(self, sink: list[tuple[str, str, dict]]) -> None:
        self.sink = sink

    def from_(self, _bucket: str) -> _FakeBucket:
        return _FakeBucket(self.sink)


class _FakeManifestTable:
    def __init__(self, sink: list[tuple[Any, Any]]) -> None:
        self.sink = sink

    def upsert(
        self, payload: dict, on_conflict: str | None = None
    ) -> _FakeQueryBuilder:
        self.sink.append((payload, on_conflict))
        return _FakeQueryBuilder()


class _FakeSupabaseClient:
    def __init__(self) -> None:
        self.uploads: list[tuple[str, str, dict]] = []
        self.manifest_ops: list[tuple[Any, Any]] = []
        self.storage = _FakeStorage(self.uploads)

    def table(self, _name: str) -> Any:
        return _FakeManifestTable(self.manifest_ops)


def test_ndjson_records_parses_valid_lines(tmp_path: Path) -> None:
    file_path = tmp_path / "test.ndjson"
    file_path.write_text(
        '{"change_id":"c1","items_count":10}\n{"change_id":"c2","items_count":5}\n',
        encoding="utf-8",
    )

    records = _ndjson_records(file_path)
    assert len(records) == 2
    assert records[0]["change_id"] == "c1"
    assert records[1]["change_id"] == "c2"


def test_ndjson_records_skips_empty_lines(tmp_path: Path) -> None:
    file_path = tmp_path / "test.ndjson"
    file_path.write_text(
        '{"change_id":"c1"}\n\n\n{"change_id":"c2"}\n',
        encoding="utf-8",
    )

    records = _ndjson_records(file_path)
    assert len(records) == 2


def test_ndjson_records_warns_on_invalid_json(tmp_path: Path, caplog) -> None:
    file_path = tmp_path / "test.ndjson"
    file_path.write_text('{"valid":true}\nnot-json\n{"also":false}\n', encoding="utf-8")

    records = _ndjson_records(file_path)
    assert len(records) == 2
    assert "linha inválida JSON" in caplog.text


def test_upload_ndjson_returns_metadata(tmp_path) -> None:
    """_upload_ndjson deve retornar object_path, size e sha256."""
    client = _FakeSupabaseClient()
    config = MagicMock()
    config.firehose_raw_bucket = "firehose-raw"

    # arquivo precisa estar dentro de FIREHOSE_RAW_DIR para relative_to funcionar
    landing_dir = tmp_path / "data" / "firehose_raw" / "2026-03-25"
    landing_dir.mkdir(parents=True)
    file_path = landing_dir / "page-test.ndjson"
    file_path.write_text('{"test":true}\n', encoding="utf-8")

    import scripts.firehose_to_supabase as fts

    original_dir = fts.FIREHOSE_RAW_DIR
    fts.FIREHOSE_RAW_DIR = tmp_path / "data" / "firehose_raw"
    try:
        result = _upload_ndjson(file_path, config, client)
    finally:
        fts.FIREHOSE_RAW_DIR = original_dir

    assert result is not None
    assert "object_path" in result
    assert result["file_size_bytes"] > 0
    assert len(result["content_sha256"]) == 64  # sha256 hex
    assert len(client.uploads) == 1


def test_upload_ndjson_returns_none_for_missing_file() -> None:
    client = _FakeSupabaseClient()
    config = MagicMock()
    config.firehose_raw_bucket = "firehose-raw"

    result = _upload_ndjson(Path("/nonexistent/file.ndjson"), config, client)
    assert result is None


def test_process_firehose_raw_uploaded_and_deleted_on_success(
    tmp_path, monkeypatch
) -> None:
    """Arquivo deve ser removido após upload bem-sucedido."""
    landing_dir = tmp_path / "firehose_raw" / "2026-03-25"
    landing_dir.mkdir(parents=True)
    ndjson_file = landing_dir / "page-001.ndjson"
    ndjson_file.write_text(
        '{"change_id":"page-001","items_count":10,"collected_at":"2026-03-25T10:00:00Z","next_change_id":"next-001","stashes":[],"raw_payload":{}}\n',
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "scripts.firehose_to_supabase.FIREHOSE_RAW_DIR", landing_dir.parent
    )
    monkeypatch.setattr(
        "scripts.firehose_to_supabase._create_supabase_client",
        lambda config: _FakeSupabaseClient(),
    )

    from core.cloud_config import SupabaseCloudConfig

    config = SupabaseCloudConfig(
        backend="supabase",
        project_url="https://demo.supabase.co",
        service_role_key="key",
        firehose_raw_bucket="firehose-raw",
    )

    stats = process_firehose_raw(config, keep_files=False)

    assert stats["uploaded"] == 1
    assert stats["deleted"] == 1
    assert not ndjson_file.exists()


def test_process_firehose_raw_keeps_file_when_keep_true(tmp_path, monkeypatch) -> None:
    """Arquivo deve ser mantido quando keep=True."""
    landing_dir = tmp_path / "firehose_raw" / "2026-03-25"
    landing_dir.mkdir(parents=True)
    ndjson_file = landing_dir / "page-002.ndjson"
    ndjson_file.write_text(
        '{"change_id":"page-002","items_count":5,"collected_at":"2026-03-25T10:00:00Z","next_change_id":"next-002","stashes":[],"raw_payload":{}}\n',
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "scripts.firehose_to_supabase.FIREHOSE_RAW_DIR", landing_dir.parent
    )
    monkeypatch.setattr(
        "scripts.firehose_to_supabase._create_supabase_client",
        lambda config: _FakeSupabaseClient(),
    )

    from core.cloud_config import SupabaseCloudConfig

    config = SupabaseCloudConfig(
        backend="supabase",
        project_url="https://demo.supabase.co",
        service_role_key="key",
        firehose_raw_bucket="firehose-raw",
    )

    stats = process_firehose_raw(config, keep_files=True)

    assert stats["uploaded"] == 1
    assert stats["deleted"] == 0
    assert ndjson_file.exists()


def test_process_firehose_raw_manifest_uses_object_path_idempotency(
    tmp_path, monkeypatch
) -> None:
    """Manifest upsert deve usar object_path para suporte a upsert idempotente."""
    landing_dir = tmp_path / "firehose_raw" / "2026-03-25"
    landing_dir.mkdir(parents=True)
    ndjson_file = landing_dir / "page-003.ndjson"
    ndjson_file.write_text(
        '{"change_id":"page-003","items_count":3,"collected_at":"2026-03-25T10:00:00Z","next_change_id":"next-003","stashes":[],"raw_payload":{}}\n',
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "scripts.firehose_to_supabase.FIREHOSE_RAW_DIR", landing_dir.parent
    )
    fake_client = _FakeSupabaseClient()
    monkeypatch.setattr(
        "scripts.firehose_to_supabase._create_supabase_client",
        lambda config: fake_client,
    )

    from core.cloud_config import SupabaseCloudConfig

    config = SupabaseCloudConfig(
        backend="supabase",
        project_url="https://demo.supabase.co",
        service_role_key="key",
        firehose_raw_bucket="firehose-raw",
    )

    process_firehose_raw(config, keep_files=True)

    assert len(fake_client.manifest_ops) == 1
    payload, on_conflict = fake_client.manifest_ops[0]
    assert on_conflict == "object_path"
    assert payload["status"] == "uploaded"


def test_process_firehose_raw_deletes_empty_file(tmp_path, monkeypatch) -> None:
    """Arquivo vazio ou só com linhas inválidas deve ser deletado."""
    landing_dir = tmp_path / "firehose_raw" / "2026-03-25"
    landing_dir.mkdir(parents=True)
    ndjson_file = landing_dir / "empty.ndjson"
    ndjson_file.write_text("", encoding="utf-8")

    monkeypatch.setattr(
        "scripts.firehose_to_supabase.FIREHOSE_RAW_DIR", landing_dir.parent
    )
    monkeypatch.setattr(
        "scripts.firehose_to_supabase._create_supabase_client",
        lambda config: _FakeSupabaseClient(),
    )

    from core.cloud_config import SupabaseCloudConfig

    config = SupabaseCloudConfig(
        backend="supabase",
        project_url="https://demo.supabase.co",
        service_role_key="key",
        firehose_raw_bucket="firehose-raw",
    )

    stats = process_firehose_raw(config, keep_files=False)

    assert stats["deleted"] == 1
    assert not ndjson_file.exists()


def test_process_firehose_raw_handles_upload_failure_gracefully(
    tmp_path, monkeypatch
) -> None:
    """Falha no upload deve ser contada e não deve impedir processamento de outros."""

    class _FailingClient:
        def storage(self):
            raise RuntimeError("storage unavailable")

        def table(self, name):
            return _FakeManifestTable([])

    landing_dir = tmp_path / "firehose_raw" / "2026-03-25"
    landing_dir.mkdir(parents=True)
    ndjson_file = landing_dir / "fail.ndjson"
    ndjson_file.write_text(
        '{"change_id":"fail","items_count":1,"collected_at":"2026-03-25T10:00:00Z","next_change_id":"n","stashes":[],"raw_payload":{}}\n',
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "scripts.firehose_to_supabase.FIREHOSE_RAW_DIR", landing_dir.parent
    )
    monkeypatch.setattr(
        "scripts.firehose_to_supabase._create_supabase_client",
        lambda config: _FailingClient(),
    )

    from core.cloud_config import SupabaseCloudConfig

    config = SupabaseCloudConfig(
        backend="supabase",
        project_url="https://demo.supabase.co",
        service_role_key="key",
        firehose_raw_bucket="firehose-raw",
    )

    stats = process_firehose_raw(config, keep_files=False)

    assert stats["failed"] == 1


def test_process_firehose_raw_skips_old_files_by_max_age(tmp_path, monkeypatch) -> None:
    """Arquivos mais antigos que max_age_days devem ser pulados."""
    from datetime import datetime, timezone, timedelta

    landing_dir = tmp_path / "firehose_raw" / "2026-01-01"
    landing_dir.mkdir(parents=True)
    ndjson_file = landing_dir / "old.ndjson"
    ndjson_file.write_text(
        '{"change_id":"old","items_count":1,"collected_at":"2026-01-01T00:00:00Z","next_change_id":"n","stashes":[],"raw_payload":{}}\n',
        encoding="utf-8",
    )
    # make file appear old
    old_mtime = (datetime.now(timezone.utc) - timedelta(days=10)).timestamp()
    ndjson_file.touch()
    import os

    os.utime(ndjson_file, (old_mtime, old_mtime))

    monkeypatch.setattr(
        "scripts.firehose_to_supabase.FIREHOSE_RAW_DIR", landing_dir.parent
    )
    monkeypatch.setattr(
        "scripts.firehose_to_supabase._create_supabase_client",
        lambda config: _FakeSupabaseClient(),
    )

    from core.cloud_config import SupabaseCloudConfig

    config = SupabaseCloudConfig(
        backend="supabase",
        project_url="https://demo.supabase.co",
        service_role_key="key",
        firehose_raw_bucket="firehose-raw",
    )

    stats = process_firehose_raw(config, keep_files=True, max_age_days=7)

    assert stats["skipped"] == 1
    assert stats["uploaded"] == 0
