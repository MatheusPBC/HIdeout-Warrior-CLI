"""Tests for scripts/bootstrap_supabase.py - Bootstrap and verification utilities."""

from unittest.mock import MagicMock, patch

import pytest

from core.cloud_config import SupabaseCloudConfig
from scripts.bootstrap_supabase import (
    ExitCode,
    bootstrap,
    check_bucket_exists,
    check_db_connectivity,
    check_storage_connectivity,
    print_report,
    verify_schema,
)


def _config() -> SupabaseCloudConfig:
    return SupabaseCloudConfig(
        backend="supabase",
        project_url="https://demo.supabase.co",
        service_role_key="service-role-key",
        storage_bucket="hw-data",
        storage_prefix="dev",
    )


class _FakeBucket:
    def __init__(self, name: str) -> None:
        self.name = name


class _FakeStorage:
    def __init__(self, buckets: list[str]) -> None:
        self._bucket_names = buckets

    def list_buckets(self):
        return [_FakeBucket(name) for name in self._bucket_names]


class _FakeSupabaseClient:
    def __init__(
        self, buckets: list[str] | None = None, tables_exist: bool = True
    ) -> None:
        self.storage = _FakeStorage(buckets or ["hw-data", "firehose-raw"])
        self._tables_exist = tables_exist

    def table(self, name: str):
        return _FakeTable(self._tables_exist)


class _FakeTable:
    def __init__(self, exists: bool) -> None:
        self._exists = exists

    def select(self, *args, **kwargs):
        return self

    def limit(self, *args, **kwargs):
        return self

    def execute(self):
        if not self._exists:
            raise RuntimeError(f"Table not found")
        return MagicMock(data=[])


# ─── Bucket Checks ─────────────────────────────────────────────────────────────


def test_check_bucket_exists_returns_true_when_present() -> None:
    """check_bucket_exists returns True when bucket is in the list."""
    client = _FakeSupabaseClient(buckets=["hw-data", "firehose-raw"])

    assert check_bucket_exists(client, "hw-data") is True
    assert check_bucket_exists(client, "firehose-raw") is True


def test_check_bucket_exists_returns_false_when_missing() -> None:
    """check_bucket_exists returns False when bucket not found."""
    client = _FakeSupabaseClient(buckets=["other-bucket"])

    assert check_bucket_exists(client, "hw-data") is False


def test_check_bucket_exists_handles_exception() -> None:
    """check_bucket_exists returns False on exception."""

    class _ErrorClient:
        storage = None

        class _ErrorStorage:
            def list_buckets(self):
                raise RuntimeError("Connection error")

    assert check_bucket_exists(_ErrorClient(), "hw-data") is False


# ─── Schema Verification ───────────────────────────────────────────────────────


def test_verify_schema_returns_true_for_existing_tables() -> None:
    """verify_schema returns True for each existing table."""
    client = _FakeSupabaseClient(tables_exist=True)
    config = SupabaseCloudConfig(
        backend="supabase",
        project_url="https://demo.supabase.co",
        service_role_key="service-role-key",
        storage_bucket="hw-data",
        storage_prefix="dev",
        firehose_raw_manifest_table="firehose_raw_manifest",
        active_models_table="active_models",
        snapshot_runs_table="snapshot_runs",
        firehose_checkpoint_table="firehose_checkpoints",
    )

    results = verify_schema(client, config)

    assert all(results.values())


def test_verify_schema_returns_false_for_missing_tables() -> None:
    """verify_schema returns False for missing tables."""
    client = _FakeSupabaseClient(tables_exist=False)
    config = SupabaseCloudConfig(
        backend="supabase",
        project_url="https://demo.supabase.co",
        service_role_key="service-role-key",
        storage_bucket="hw-data",
        storage_prefix="dev",
        firehose_raw_manifest_table="firehose_raw_manifest",
        active_models_table="active_models",
        snapshot_runs_table="snapshot_runs",
        firehose_checkpoint_table="firehose_checkpoints",
    )

    results = verify_schema(client, config)

    assert results["firehose_raw_manifest"] is False


# ─── Connectivity Checks ───────────────────────────────────────────────────────


def test_check_storage_connectivity_returns_true_on_success() -> None:
    """check_storage_connectivity returns True when storage is accessible."""
    client = _FakeSupabaseClient(buckets=["hw-data"])
    config = _config()

    result = check_storage_connectivity(client, config)

    assert result is True


def test_check_storage_connectivity_returns_false_on_error() -> None:
    """check_storage_connectivity returns False when storage throws."""

    class _BadClient:
        class _BadStorage:
            def list_buckets(self):
                raise RuntimeError("Access denied")

        storage = _BadStorage()

    result = check_storage_connectivity(_BadClient(), _config())
    assert result is False


def test_check_db_connectivity_returns_true_on_success() -> None:
    """check_db_connectivity returns True when DB is accessible."""
    client = _FakeSupabaseClient(tables_exist=True)
    result = check_db_connectivity(client)
    assert result is True


def test_check_db_connectivity_returns_false_on_error() -> None:
    """check_db_connectivity returns False when DB throws."""

    class _BadClient:
        def table(self, name):
            raise RuntimeError("Connection refused")

    assert check_db_connectivity(_BadClient()) is False


# ─── Bootstrap Logic ───────────────────────────────────────────────────────────


def test_bootstrap_returns_configured_false_when_not_configured() -> None:
    """bootstrap returns configured=False when Supabase not configured."""
    result = bootstrap(
        create_buckets=False,
        dry_run=True,
        config=SupabaseCloudConfig(backend="local"),
    )

    assert result.get("configured") is False


def test_bootstrap_returns_connectivity_results() -> None:
    """bootstrap returns storage and db connectivity results."""
    with patch("scripts.bootstrap_supabase._create_client") as mock_create:
        mock_create.return_value = _FakeSupabaseClient(
            buckets=["hw-data", "firehose-raw"]
        )

        result = bootstrap(
            create_buckets=False,
            dry_run=True,
            config=_config(),
        )

    assert result.get("configured") is True
    assert "storage_connectivity" in result
    assert "db_connectivity" in result


def test_bootstrap_checks_buckets() -> None:
    """bootstrap verifies expected buckets exist."""
    with patch("scripts.bootstrap_supabase._create_client") as mock_create:
        mock_create.return_value = _FakeSupabaseClient(buckets=["hw-data"])

        result = bootstrap(
            create_buckets=False,
            dry_run=True,
            config=_config(),
        )

    assert "buckets" in result
    assert result["buckets"]["hw-data"]["exists"] is True
    assert result["buckets"]["firehose-raw"]["exists"] is False


# ─── Report Printing ───────────────────────────────────────────────────────────


def test_print_report_returns_true_when_all_ok() -> None:
    """print_report returns True when all checks pass."""
    results = {
        "configured": True,
        "storage_connectivity": True,
        "db_connectivity": True,
        "buckets": {
            "hw-data": {"exists": True},
            "firehose-raw": {"exists": True},
        },
        "schema": {
            "artifact_catalog": True,
            "active_models": True,
        },
    }

    # Just verify it doesn't raise - actual output is on console
    all_ok = print_report(results)
    assert all_ok is True


def test_print_report_returns_false_when_some_fail() -> None:
    """print_report returns False when any check fails."""
    results = {
        "configured": True,
        "storage_connectivity": True,
        "db_connectivity": False,  # Failed
        "buckets": {},
        "schema": {},
    }

    all_ok = print_report(results)
    assert all_ok is False


def test_print_report_returns_false_when_not_configured() -> None:
    """print_report returns False when not configured."""
    results = {"configured": False}

    all_ok = print_report(results)
    assert all_ok is False
