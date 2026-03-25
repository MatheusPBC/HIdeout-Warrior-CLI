from pathlib import Path
from typing import Any

from core.cloud_config import SupabaseCloudConfig
from core.supabase_cloud import (
    load_checkpoint_from_supabase,
    sync_directory_to_supabase,
    sync_file_to_supabase,
    upsert_firehose_raw_manifest,
)


class _FakeTableOp:
    def __init__(self, rows) -> None:
        self.rows = rows

    def execute(self):
        return {"status": "ok"}


class _FakeTable:
    def __init__(self, sink: list[tuple[str, object, str | None]]) -> None:
        self.sink = sink

    def upsert(self, payload, on_conflict=None):
        self.sink.append(("upsert", payload, on_conflict))
        return _FakeTableOp(self.sink)


class _FakeBucket:
    def __init__(self, sink: list[tuple[str, str]]) -> None:
        self.sink = sink

    def upload(self, *, path, file, file_options):
        self.sink.append((path, file.read().decode("utf-8")))
        return {"path": path, "file_options": file_options}


class _FakeStorage:
    def __init__(self, sink: list[tuple[str, str]]) -> None:
        self.sink = sink

    def from_(self, _bucket_name):
        return _FakeBucket(self.sink)


class _FakeSupabaseClient:
    def __init__(self) -> None:
        self.uploads: list[tuple[str, str]] = []
        self.rows: list[tuple[str, object, str | None]] = []
        self.storage = _FakeStorage(self.uploads)

    def table(self, _table_name):
        return _FakeTable(self.rows)


def _config() -> SupabaseCloudConfig:
    return SupabaseCloudConfig(
        backend="supabase",
        project_url="https://demo.supabase.co",
        service_role_key="service-role-key",
        storage_bucket="hw-data",
        storage_prefix="dev",
    )


def test_sync_file_to_supabase_uploads_and_upserts_metadata(tmp_path: Path) -> None:
    file_path = tmp_path / "registry.json"
    file_path.write_text('{"hello":"world"}', encoding="utf-8")
    client = _FakeSupabaseClient()

    result = sync_file_to_supabase(
        file_path,
        artifact_type="model_registry",
        metadata={"kind": "registry"},
        config=_config(),
        client=client,
    )

    assert result is not None
    assert client.uploads[0][0] == "dev/model_registry/registry.json"
    assert client.uploads[0][1] == '{"hello":"world"}'
    assert client.rows[0][0] == "upsert"
    assert client.rows[0][2] == "artifact_key"


def test_sync_directory_to_supabase_preserves_relative_paths(tmp_path: Path) -> None:
    root = tmp_path / "snapshots"
    (root / "gold" / "snapshot_date=2026-03-25").mkdir(parents=True)
    (root / "gold" / "snapshot_date=2026-03-25" / "part-000.parquet").write_text(
        "parquet-data",
        encoding="utf-8",
    )
    (root / "silver").mkdir(parents=True)
    (root / "silver" / "part-000.parquet").write_text(
        "silver-data",
        encoding="utf-8",
    )
    client = _FakeSupabaseClient()

    results = sync_directory_to_supabase(
        root,
        artifact_type="training_snapshots",
        metadata={"kind": "snapshot"},
        config=_config(),
        client=client,
    )

    assert len(results) == 2
    uploaded_paths = {path for path, _content in client.uploads}
    assert (
        "dev/training_snapshots/gold/snapshot_date=2026-03-25/part-000.parquet"
        in uploaded_paths
    )
    assert "dev/training_snapshots/silver/part-000.parquet" in uploaded_paths


# ─── Firehose Checkpoint & Manifest ───────────────────────────────────────────


class _FakeQueryBuilder:
    def __init__(self, mock_result: Any) -> None:
        self._result = mock_result

    def select(self, *_args):
        return self

    def eq(self, *_args):
        return self

    def maybe_single(self):
        return self

    def execute(self):
        return self._result


class _FakeSupabaseForFirehose:
    def __init__(self, checkpoint_result: Any = None) -> None:
        self._checkpoint_result = checkpoint_result
        self.manifest_ops: list[Any] = []

    def table(self, name: str):
        if "checkpoint" in name:
            return _FakeTableForCheckpoint(self._checkpoint_result)
        elif "manifest" in name:
            return _FakeTableForManifest(self.manifest_ops)
        return _FakeTableForGeneric()


class _FakeTableForCheckpoint:
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


class _FakeTableForManifest:
    def __init__(self, sink: list[Any]) -> None:
        self.sink = sink

    def upsert(self, payload, on_conflict=None):
        self.sink.append(("upsert", payload, on_conflict))
        return _FakeQueryBuilder({"status": "ok"})


class _FakeTableForGeneric:
    def __init__(self) -> None:
        pass

    def select(self, *_args):
        return self

    def upsert(self, *_args, **_kwargs):
        return _FakeQueryBuilder({"status": "ok"})


def test_load_checkpoint_from_supabase_returns_row() -> None:
    """load_checkpoint_from_supabase deve retornar dict com campos quando encontrado."""
    fake_result = MagicMock()
    fake_result.data = {
        "next_change_id": "cloud-123",
        "pages_processed": 42,
        "events_ingested": 1000,
        "duplicates_skipped": 50,
    }
    client = _FakeSupabaseForFirehose(checkpoint_result=fake_result)

    result = load_checkpoint_from_supabase(
        config=_config(),
        client=client,
    )

    assert result is not None
    assert result["next_change_id"] == "cloud-123"
    assert result["pages_processed"] == 42
    assert result["events_ingested"] == 1000
    assert result["duplicates_skipped"] == 50


def test_load_checkpoint_from_supabase_returns_none_when_empty() -> None:
    """load_checkpoint_from_supabase deve retornar None quando não há checkpoint."""
    fake_result = MagicMock()
    fake_result.data = None
    client = _FakeSupabaseForFirehose(checkpoint_result=fake_result)

    result = load_checkpoint_from_supabase(
        config=_config(),
        client=client,
    )

    assert result is None


def test_load_checkpoint_from_supabase_returns_none_on_error() -> None:
    """load_checkpoint_from_supabase deve retornar None quando há exceção."""

    class _FakeTableThatErrors:
        def select(self, *_args):
            raise RuntimeError("connection error")

        def eq(self, *_args):
            return self

        def maybe_single(self):
            return self

        def execute(self):
            raise RuntimeError("connection error")

    class _BadClient:
        def table(self, name):
            return _FakeTableThatErrors()

    result = load_checkpoint_from_supabase(
        config=_config(),
        client=_BadClient(),
    )

    assert result is None


def test_load_checkpoint_from_supabase_returns_none_when_not_configured() -> None:
    """load_checkpoint_from_supabase deve retornar None quando config não está configurada."""
    from core.cloud_config import SupabaseCloudConfig

    result = load_checkpoint_from_supabase(
        config=SupabaseCloudConfig(backend="local"),
    )
    assert result is None


def test_upsert_firehose_raw_manifest_uses_object_path_as_conflict_target() -> None:
    """upsert_firehose_raw_manifest deve usar object_path para idempotência."""
    client = _FakeSupabaseForFirehose()

    success = upsert_firehose_raw_manifest(
        run_id="2026-03-25",
        object_path="2026-03-25/page-001.ndjson",
        rows_count=150,
        page_start_change_id="page-001",
        page_end_change_id="page-001",
        file_size_bytes=4096,
        content_sha256="abc123",
        status="uploaded",
        error_message=None,
        config=_config(),
        client=client,
    )

    assert success is True
    assert len(client.manifest_ops) == 1
    op_type, payload, on_conflict = client.manifest_ops[0]
    assert op_type == "upsert"
    assert on_conflict == "object_path"
    assert payload["object_path"] == "2026-03-25/page-001.ndjson"
    assert payload["status"] == "uploaded"


def test_upsert_firehose_raw_manifest_handles_failure_status() -> None:
    """upsert_firehose_raw_manifest deve registrar status failed com error_message."""
    client = _FakeSupabaseForFirehose()

    success = upsert_firehose_raw_manifest(
        run_id="2026-03-25",
        object_path="2026-03-25/page-fail.ndjson",
        rows_count=0,
        page_start_change_id="page-fail",
        page_end_change_id="page-fail",
        file_size_bytes=0,
        content_sha256="",
        status="failed",
        error_message="upload timeout",
        config=_config(),
        client=client,
    )

    assert success is True
    _op, payload, _conflict = client.manifest_ops[0]
    assert payload["status"] == "failed"
    assert payload["error_message"] == "upload timeout"


def test_upsert_firehose_raw_manifest_returns_false_when_not_configured() -> None:
    """upsert_firehose_raw_manifest deve retornar False quando config não está configurada."""
    from core.cloud_config import SupabaseCloudConfig

    success = upsert_firehose_raw_manifest(
        run_id="2026-03-25",
        object_path="2026-03-25/page-001.ndjson",
        rows_count=100,
        page_start_change_id="page-001",
        page_end_change_id="page-001",
        file_size_bytes=2048,
        content_sha256="def456",
        status="uploaded",
        error_message=None,
        config=SupabaseCloudConfig(backend="local"),
    )
    assert success is False


# ─── Artifact Integrity Helpers (Bloco B - Fase 2) ───────────────────────────────


from core.supabase_cloud import (
    ArtifactIntegrityInfo,
    get_artifact_checksum_info,
    validate_local_file_checksum,
    verify_artifact_integrity,
)


class _FakeSupabaseForIntegrity:
    """Fake client that returns different results for catalog queries."""

    def __init__(self, catalog_result: Any = None) -> None:
        self._catalog_result = catalog_result
        self.queries: list[str] = []

    def table(self, name: str):
        self.queries.append(name)
        if "artifact_catalog" in name:
            return _FakeTableForCatalog(self._catalog_result)
        return _FakeTableForGeneric()


class _FakeTableForCatalog:
    def __init__(self, result: Any) -> None:
        self._result = result

    def select(self, *args):
        return self

    def eq(self, *args):
        return self

    def maybe_single(self):
        return self

    def execute(self):
        return self._result


def test_get_artifact_checksum_info_returns_info_for_artifact_with_sha256() -> None:
    """get_artifact_checksum_info returns ArtifactIntegrityInfo when artifact has checksum."""
    fake_result = MagicMock()
    fake_result.data = {
        "artifact_key": "hw-data:dev/models/registry.json",
        "object_path": "dev/models/registry.json",
        "content_sha256": "abc123def456",
    }
    client = _FakeSupabaseForIntegrity(catalog_result=fake_result)

    info = get_artifact_checksum_info(
        "hw-data:dev/models/registry.json",
        config=_config(),
        client=client,
    )

    assert info is not None
    assert info.artifact_key == "hw-data:dev/models/registry.json"
    assert info.stored_sha256 == "abc123def456"
    assert info.checksum_validated is True
    assert info.is_legacy is False


def test_get_artifact_checksum_info_marks_legacy_when_no_sha256() -> None:
    """get_artifact_checksum_info marks artifact as legacy when content_sha256 is null."""
    fake_result = MagicMock()
    fake_result.data = {
        "artifact_key": "hw-data:dev/legacy/file.txt",
        "object_path": "dev/legacy/file.txt",
        "content_sha256": None,
    }
    client = _FakeSupabaseForIntegrity(catalog_result=fake_result)

    info = get_artifact_checksum_info(
        "hw-data:dev/legacy/file.txt",
        config=_config(),
        client=client,
    )

    assert info is not None
    assert info.is_legacy is True
    assert info.checksum_validated is False
    assert info.stored_sha256 is None


def test_get_artifact_checksum_info_returns_none_when_not_found() -> None:
    """get_artifact_checksum_info returns None when artifact not in catalog."""
    fake_result = MagicMock()
    fake_result.data = None
    client = _FakeSupabaseForIntegrity(catalog_result=fake_result)

    info = get_artifact_checksum_info(
        "nonexistent-artifact",
        config=_config(),
        client=client,
    )

    assert info is None


def test_validate_local_file_checksum_match(tmp_path: Path) -> None:
    """validate_local_file_checksum returns True when SHA256 matches."""
    test_file = tmp_path / "test.txt"
    test_file.write_bytes(b"hello world")
    expected_sha = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"

    is_valid, actual = validate_local_file_checksum(test_file, expected_sha)

    assert is_valid is True
    assert actual == expected_sha


def test_validate_local_file_checksum_mismatch(tmp_path: Path) -> None:
    """validate_local_file_checksum returns False when SHA256 doesn't match."""
    test_file = tmp_path / "test.txt"
    test_file.write_bytes(b"hello world")
    wrong_sha = "deadbeef" * 8

    is_valid, actual = validate_local_file_checksum(test_file, wrong_sha)

    assert is_valid is False
    assert actual is not None
    assert actual != wrong_sha


def test_validate_local_file_checksum_missing_file(tmp_path: Path) -> None:
    """validate_local_file_checksum returns False for missing file."""
    missing = tmp_path / "does_not_exist.txt"

    is_valid, actual = validate_local_file_checksum(missing, "anysha")

    assert is_valid is False
    assert actual is None


def test_verify_artifact_integrity_legacy_artifact(tmp_path: Path) -> None:
    """verify_artifact_integrity marks legacy artifacts as valid (can't validate)."""
    fake_result = MagicMock()
    fake_result.data = {
        "artifact_key": "hw-data:dev/legacy/file.txt",
        "object_path": "dev/legacy/file.txt",
        "content_sha256": None,
    }
    client = _FakeSupabaseForIntegrity(catalog_result=fake_result)
    test_file = tmp_path / "legacy.txt"
    test_file.write_bytes(b"legacy content")

    result = verify_artifact_integrity(
        "hw-data:dev/legacy/file.txt",
        test_file,
        config=_config(),
        client=client,
    )

    assert result["is_valid"] is True
    assert result["is_legacy"] is True
    assert "Legacy artifact" in (result["error"] or "")


def test_verify_artifact_integrity_with_matching_checksum(tmp_path: Path) -> None:
    """verify_artifact_integrity returns is_valid=True when checksums match."""
    content = b"hello world"
    sha = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
    fake_result = MagicMock()
    fake_result.data = {
        "artifact_key": "hw-data:dev/snapshots/hello.txt",
        "object_path": "dev/snapshots/hello.txt",
        "content_sha256": sha,
    }
    client = _FakeSupabaseForIntegrity(catalog_result=fake_result)
    test_file = tmp_path / "hello.txt"
    test_file.write_bytes(content)

    result = verify_artifact_integrity(
        "hw-data:dev/snapshots/hello.txt",
        test_file,
        config=_config(),
        client=client,
    )

    assert result["is_valid"] is True
    assert result["is_legacy"] is False
    assert result["stored_sha256"] == sha
    assert result["actual_sha256"] == sha


def test_verify_artifact_integrity_missing_local_file() -> None:
    """verify_artifact_integrity returns error when local file doesn't exist."""
    fake_result = MagicMock()
    fake_result.data = {
        "artifact_key": "hw-data:dev/models/file.txt",
        "object_path": "dev/models/file.txt",
        "content_sha256": "abc123",
    }
    client = _FakeSupabaseForIntegrity(catalog_result=fake_result)

    result = verify_artifact_integrity(
        "hw-data:dev/models/file.txt",
        Path("/nonexistent/file.txt"),
        config=_config(),
        client=client,
    )

    assert result["is_valid"] is False
    assert "Local file does not exist" in (result["error"] or "")


# needed for fake
from unittest.mock import MagicMock
