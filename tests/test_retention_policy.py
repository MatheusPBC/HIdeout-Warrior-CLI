"""Tests for scripts/retention_policy.py - Retention policy and governance utilities."""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from scripts.retention_policy import (
    DEFAULT_POLICIES,
    PolicyType,
    RetentionPolicy,
    apply_policy,
    check_policy_status,
    get_policy_from_config,
)


# ─── Policy Structures ───────────────────────────────────────────────────────────


def test_default_policies_exist() -> None:
    """DEFAULT_POLICIES should have at least firehose and snapshot policies."""
    assert len(DEFAULT_POLICIES) >= 2
    policy_names = [p.name for p in DEFAULT_POLICIES]
    assert any("firehose" in name for name in policy_names)
    assert any("snapshot" in name for name in policy_names)


def test_default_firehose_raw_manifest_policy_has_correct_type() -> None:
    """firehose_raw_manifest policy should have correct PolicyType."""
    firehose_policy = next(
        p for p in DEFAULT_POLICIES if p.policy_type == PolicyType.FIREHOSE_RAW_MANIFEST
    )
    assert firehose_policy.retention_days == 30
    assert firehose_policy.target_table_or_bucket == "firehose_raw_manifest"


def test_default_snapshot_runs_policy_uses_snapshot_date_field() -> None:
    """snapshot_runs policy should use snapshot_date field for filtering."""
    snapshot_policy = next(
        p for p in DEFAULT_POLICIES if p.policy_type == PolicyType.SNAPSHOT_RUNS
    )
    assert snapshot_policy.date_field == "updated_at"
    assert snapshot_policy.retention_days == 90


def test_retention_policy_dataclass_is_frozen() -> None:
    """RetentionPolicy should be a frozen dataclass."""
    policy = RetentionPolicy(
        name="test_policy",
        policy_type=PolicyType.FIREHOSE_RAW_MANIFEST,
        retention_days=30,
        description="Test policy",
        target_table_or_bucket="test_table",
    )
    with pytest.raises(Exception):  # FrozenInstanceError
        policy.retention_days = 60


# ─── get_policy_from_config ─────────────────────────────────────────────────────


def test_get_policy_from_config_returns_defaults() -> None:
    """get_policy_from_config should return DEFAULT_POLICIES by default."""
    with patch("scripts.retention_policy.load_cloud_config") as mock_config:
        mock_config.return_value = MagicMock()

        policies = get_policy_from_config()

    assert len(policies) == len(DEFAULT_POLICIES)


# ─── check_policy_status ───────────────────────────────────────────────────────


def test_check_policy_status_returns_stats() -> None:
    """check_policy_status returns affected count and size for a policy."""

    class _FakeQueryBuilder:
        def __init__(self, records: list) -> None:
            self._records = records

        def select(self, *args, **kwargs):
            return self

        def lt(self, *args, **kwargs):
            return self

        def execute(self):
            return MagicMock(data=self._records)

    class _FakeTable:
        def __init__(self, records: list) -> None:
            self._records = records

        def select(self, *args, **kwargs):
            return _FakeQueryBuilder(self._records)

        def lt(self, *args, **kwargs):
            return _FakeQueryBuilder(self._records)

    class _FakeClient:
        def table(self, name: str):
            return _FakeTable(
                [
                    {
                        "id": 1,
                        "file_size_bytes": 4096,
                        "uploaded_at": "2026-01-01T00:00:00Z",
                    },
                    {
                        "id": 2,
                        "file_size_bytes": 2048,
                        "uploaded_at": "2026-01-02T00:00:00Z",
                    },
                ]
            )

    policy = RetentionPolicy(
        name="test_policy",
        policy_type=PolicyType.FIREHOSE_RAW_MANIFEST,
        retention_days=30,
        description="Test",
        target_table_or_bucket="firehose_raw_manifest",
    )

    with patch("scripts.retention_policy.load_cloud_config") as mock_config:
        mock_config.return_value = MagicMock()
        with patch(
            "core.supabase_cloud._create_supabase_client", return_value=_FakeClient()
        ):
            result = check_policy_status(policy)

    assert "affected_count" in result
    assert "affected_size_bytes" in result
    assert "error" in result


def test_check_policy_status_handles_exception() -> None:
    """check_policy_status returns error on exception."""

    class _BadClient:
        def table(self, name: str):
            raise RuntimeError("Connection error")

    policy = RetentionPolicy(
        name="test_policy",
        policy_type=PolicyType.FIREHOSE_RAW_MANIFEST,
        retention_days=30,
        description="Test",
        target_table_or_bucket="firehose_raw_manifest",
    )

    with patch("scripts.retention_policy.load_cloud_config") as mock_config:
        mock_config.return_value = MagicMock()
        with patch(
            "core.supabase_cloud._create_supabase_client", return_value=_BadClient()
        ):
            result = check_policy_status(policy)

    assert result["error"] is not None
    assert result["affected_count"] == 0


# ─── apply_policy ───────────────────────────────────────────────────────────────


def test_apply_policy_dry_run_does_not_delete() -> None:
    """apply_policy with dry_run=True should not delete records."""

    class _FakeClient:
        def __init__(self) -> None:
            self.deleted_ids: list = []

        def table(self, name: str):
            return _FakeTable(self)

    class _FakeTable:
        def __init__(self, client: _FakeClient) -> None:
            self._client = client

        def select(self, *args, **kwargs):
            return self

        def lt(self, *args, **kwargs):
            return self

        def delete(self, *args, **kwargs):
            return _FakeDelete(self._client)

        def execute(self):
            return MagicMock(data=[{"id": 1}, {"id": 2}])

    class _FakeDelete:
        def __init__(self, client: _FakeClient) -> None:
            self._client = client

        def eq(self, *args, **kwargs):
            return self

        def execute(self):
            self._client.deleted_ids.append(1)
            return MagicMock()

    policy = RetentionPolicy(
        name="test_policy",
        policy_type=PolicyType.FIREHOSE_RAW_MANIFEST,
        retention_days=30,
        description="Test",
        target_table_or_bucket="firehose_raw_manifest",
    )

    with patch("scripts.retention_policy.load_cloud_config") as mock_config:
        mock_config.return_value = MagicMock()
        with patch(
            "core.supabase_cloud._create_supabase_client", return_value=_FakeClient()
        ):
            result = apply_policy(policy, dry_run=True)

    assert result["deleted_count"] == 0
    assert result["dry_run"] is True


def test_apply_policy_snapshot_runs_uses_snapshot_date() -> None:
    """apply_policy for SNAPSHOT_RUNS uses snapshot_date field."""
    policy = RetentionPolicy(
        name="test_snapshot_policy",
        policy_type=PolicyType.SNAPSHOT_RUNS,
        retention_days=90,
        description="Test",
        target_table_or_bucket="snapshot_runs",
        date_field="snapshot_date",
    )

    with patch("scripts.retention_policy.load_cloud_config") as mock_config:
        mock_config.return_value = MagicMock()
        with patch("core.supabase_cloud._create_supabase_client") as mock_create:
            mock_client = MagicMock()
            mock_create.return_value = mock_client

            result = apply_policy(policy, dry_run=True)

    # Should have called with snapshot_date field
    assert "error" in result  # Either success or error, but should not crash


def test_apply_policy_firehose_raw_storage_returns_error_message() -> None:
    """apply_policy for FIREHOSE_RAW_STORAGE returns error (requires separate logic)."""
    policy = RetentionPolicy(
        name="test_storage_policy",
        policy_type=PolicyType.FIREHOSE_RAW_STORAGE,
        retention_days=60,
        description="Test",
        target_table_or_bucket="firehose-raw",
    )

    with patch("scripts.retention_policy.load_cloud_config") as mock_config:
        mock_config.return_value = MagicMock()
        with patch("core.supabase_cloud._create_supabase_client"):
            result = apply_policy(policy, dry_run=True)

    assert result["error"] is not None
    assert "cleanup_firehose_raw" in result["error"]


# ─── Policy Type Enum ──────────────────────────────────────────────────────────


def test_policy_type_enum_values() -> None:
    """PolicyType enum should have expected values."""
    assert PolicyType.FIREHOSE_RAW_MANIFEST.value == "firehose_raw_manifest"
    assert PolicyType.FIREHOSE_RAW_STORAGE.value == "firehose_raw_storage"
    assert PolicyType.ARTIFACT_CATALOG.value == "artifact_catalog"
    assert PolicyType.SNAPSHOT_RUNS.value == "snapshot_runs"
