"""Tests for core/cloud_download.py - Download utilities with integrity metadata."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from core.cloud_download import (
    DownloadResult,
    _compute_file_sha256,
    download_directory_from_supabase,
    download_file_from_supabase,
)
from core.cloud_config import SupabaseCloudConfig


class _FakeStorage:
    def __init__(self, files: dict[str, bytes] | None = None) -> None:
        self._files = files or {}

    def from_(self, bucket: str):
        return _FakeBucket(self._files)


class _FakeBucket:
    def __init__(self, files: dict[str, bytes]) -> None:
        self._files = files

    def download(self, path: str) -> bytes:
        if path not in self._files:
            raise FileNotFoundError(f"File not found: {path}")
        return self._files[path]

    def list(self, path: str = "", options: dict | None = None):
        prefix = path.rstrip("/") + "/" if path else ""
        return [
            {"name": name.replace(prefix, "")}
            for name in self._files
            if name.startswith(prefix)
        ]


class _FakeSupabaseClient:
    def __init__(self, storage: _FakeStorage) -> None:
        self.storage = storage


def _config() -> SupabaseCloudConfig:
    return SupabaseCloudConfig(
        backend="supabase",
        project_url="https://demo.supabase.co",
        service_role_key="service-role-key",
        storage_bucket="hw-data",
        storage_prefix="dev",
    )


# ─── DownloadResult ────────────────────────────────────────────────────────────


def test_download_result_is_legacy_true_when_no_checksum() -> None:
    """Legacy artifacts have no expected_sha256 and checksum_validated=False."""
    result = DownloadResult(
        local_path=Path("/tmp/test.txt"),
        success=True,
        checksum_validated=False,
        expected_sha256=None,
        actual_sha256="abc123",
        error_message=None,
    )
    assert result.is_legacy is True


def test_download_result_is_legacy_false_when_has_expected_checksum() -> None:
    """Artifacts with expected_sha256 are not legacy even if not yet validated."""
    result = DownloadResult(
        local_path=Path("/tmp/test.txt"),
        success=True,
        checksum_validated=False,
        expected_sha256="abc123",
        actual_sha256=None,
        error_message=None,
    )
    assert result.is_legacy is False


def test_download_result_is_legacy_false_when_validated() -> None:
    """Validated artifacts are not legacy."""
    result = DownloadResult(
        local_path=Path("/tmp/test.txt"),
        success=True,
        checksum_validated=True,
        expected_sha256="abc123",
        actual_sha256="abc123",
        error_message=None,
    )
    assert result.is_legacy is False


# ─── _compute_file_sha256 ──────────────────────────────────────────────────────


def test_compute_file_sha256_returns_hex_digest(tmp_path: Path) -> None:
    """_compute_file_sha256 returns SHA256 hex string."""
    test_file = tmp_path / "test.txt"
    test_file.write_bytes(b"hello world")

    sha = _compute_file_sha256(test_file)

    # SHA256 of "hello world"
    expected = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
    assert sha == expected


def test_compute_file_sha256_returns_none_for_missing_file(tmp_path: Path) -> None:
    """_compute_file_sha256 returns None for non-existent file."""
    missing = tmp_path / "does_not_exist.txt"
    assert _compute_file_sha256(missing) is None


# ─── download_file_from_supabase ───────────────────────────────────────────────


def test_download_file_from_supabase_success_without_checksum(tmp_path: Path) -> None:
    """Download succeeds and returns checksum_validated=False when no expected_sha256."""
    content = b"test content"
    storage = _FakeStorage({"dev/snapshots/file.txt": content})
    client = _FakeSupabaseClient(storage)

    with patch("core.cloud_download._create_supabase_client", return_value=client):
        result = download_file_from_supabase(
            artifact_type="snapshots",
            relative_path="file.txt",
            output_path=tmp_path / "output.txt",
            config=_config(),
            expected_sha256=None,
            validate_checksum=False,
        )

    assert result.success is True
    assert result.checksum_validated is False
    assert result.expected_sha256 is None
    assert result.actual_sha256 is not None
    assert (tmp_path / "output.txt").read_bytes() == content


def test_download_file_from_supabase_validates_checksum_match(tmp_path: Path) -> None:
    """Download succeeds with checksum_validated=True when SHA256 matches."""
    content = b"hello world"
    # Pre-computed SHA256 of "hello world"
    expected_sha = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
    storage = _FakeStorage({"dev/snapshots/hello.txt": content})
    client = _FakeSupabaseClient(storage)

    with patch("core.cloud_download._create_supabase_client", return_value=client):
        result = download_file_from_supabase(
            artifact_type="snapshots",
            relative_path="hello.txt",
            output_path=tmp_path / "hello.txt",
            config=_config(),
            expected_sha256=expected_sha,
            validate_checksum=True,
        )

    assert result.success is True
    assert result.checksum_validated is True
    assert result.expected_sha256 == expected_sha
    assert result.actual_sha256 == expected_sha


def test_download_file_from_supabase_mismatch_still_succeeds_strategy2(
    tmp_path: Path,
) -> None:
    """Checksum mismatch doesn't break download (strategy 2)."""
    content = b"hello world"
    wrong_sha = "deadbeef" * 8  # Wrong SHA
    storage = _FakeStorage({"dev/snapshots/hello.txt": content})
    client = _FakeSupabaseClient(storage)

    with patch("core.cloud_download._create_supabase_client", return_value=client):
        result = download_file_from_supabase(
            artifact_type="snapshots",
            relative_path="hello.txt",
            output_path=tmp_path / "hello.txt",
            config=_config(),
            expected_sha256=wrong_sha,
            validate_checksum=True,
        )

    # Download succeeded but validation failed (strategy 2: don't break downloads)
    assert result.success is True
    assert result.checksum_validated is False
    assert result.error_message == "Checksum mismatch"


def test_download_file_from_supabase_not_configured(tmp_path: Path) -> None:
    """Returns failure result when Supabase not configured."""
    result = download_file_from_supabase(
        artifact_type="snapshots",
        relative_path="file.txt",
        output_path=tmp_path / "output.txt",
        config=SupabaseCloudConfig(backend="local"),
    )

    assert result.success is False
    assert result.error_message == "Supabase not configured"


def test_download_file_from_supabase_handles_exception(tmp_path: Path) -> None:
    """Returns failure result when download raises exception."""
    # This test verifies exception handling - the mock raises an exception
    # and the function should return a failed DownloadResult
    with patch("core.cloud_download._create_supabase_client") as mock_create:
        mock_create.side_effect = RuntimeError("Connection refused")

        result = download_file_from_supabase(
            artifact_type="snapshots",
            relative_path="file.txt",
            output_path=tmp_path / "output.txt",
            config=_config(),
        )

    assert result.success is False
    assert result.error_message is not None


# ─── download_directory_from_supabase ─────────────────────────────────────────


def test_download_directory_from_supabase_returns_list(tmp_path: Path) -> None:
    """download_directory_from_supabase returns list of DownloadResult."""
    files = {
        "dev/snapshots/batch/": None,  # Folder marker
        "dev/snapshots/batch/file1.txt": b"content1",
        "dev/snapshots/batch/file2.txt": b"content2",
    }
    storage = _FakeStorage(files)
    client = _FakeSupabaseClient(storage)

    with patch("core.cloud_download._create_supabase_client", return_value=client):
        results = download_directory_from_supabase(
            artifact_type="snapshots",
            prefix="batch",
            output_dir=tmp_path / "batch",
            config=_config(),
        )

    assert len(results) == 2
    assert all(r.success for r in results)
    assert all(
        r.checksum_validated is False for r in results
    )  # Legacy - no expected checksum


def test_download_directory_from_supabase_empty_when_not_configured() -> None:
    """Returns empty list when not configured."""
    results = download_directory_from_supabase(
        artifact_type="snapshots",
        prefix="batch",
        output_dir=Path("/tmp/out"),
        config=SupabaseCloudConfig(backend="local"),
    )
    assert results == []
