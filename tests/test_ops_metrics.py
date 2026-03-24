import json
import re

import pytest

from core.ops_metrics import (
    _sanitize_for_filename,
    emit_snapshot_metrics,
)


class TestSanitizeForFilename:
    def test_returns_value_when_matches_pattern(self) -> None:
        pattern = re.compile(r"^[a-zA-Z0-9_\-]{1,32}$")
        result = _sanitize_for_filename("valid-value_123", pattern, "fallback")
        assert result == "valid-value_123"

    def test_returns_fallback_when_value_is_empty(self) -> None:
        pattern = re.compile(r"^[a-zA-Z0-9_\-]{1,32}$")
        result = _sanitize_for_filename("", pattern, "fallback")
        assert result == "fallback"

    def test_returns_fallback_when_value_contains_invalid_chars(self) -> None:
        pattern = re.compile(r"^[a-zA-Z0-9_\-]{1,32}$")
        result = _sanitize_for_filename("invalid@value!", pattern, "fallback")
        assert result == "fallback"

    def test_returns_fallback_when_value_too_long(self) -> None:
        pattern = re.compile(r"^[a-zA-Z0-9_\-]{1,32}$")
        long_value = "a" * 50
        result = _sanitize_for_filename(long_value, pattern, "fallback")
        assert result == "fallback"


class TestEmitSnapshotMetrics:
    def test_emit_snapshot_metrics_creates_valid_json_file(self, tmp_path) -> None:
        snapshot_summary = {
            "snapshot_date": "2026-03-11",
            "bronze": {"rows": 100, "rows_read": 120},
            "silver": {"rows": 95},
            "gold": {"rows": 90},
        }

        result_path = emit_snapshot_metrics(
            snapshot_summary=snapshot_summary,
            metrics_dir=tmp_path,
            run_id="test-run-001",
        )

        assert result_path.exists()
        payload = json.loads(result_path.read_text(encoding="utf-8"))
        assert payload["snapshot_date"] == "2026-03-11"
        assert payload["run_id"] == "test-run-001"
        assert payload["bronze"]["rows"] == 100
        assert payload["silver"]["rows"] == 95
        assert payload["gold"]["rows"] == 90

    def test_emit_snapshot_metrics_uses_fallback_for_invalid_snapshot_date(
        self, tmp_path
    ) -> None:
        snapshot_summary = {
            "snapshot_date": "invalid@date!",
            "bronze": {"rows": 100},
            "silver": {"rows": 95},
            "gold": {"rows": 90},
        }

        result_path = emit_snapshot_metrics(
            snapshot_summary=snapshot_summary,
            metrics_dir=tmp_path,
            run_id="test-run",
        )

        payload = json.loads(result_path.read_text(encoding="utf-8"))
        # Should use fallback date (ISO date pattern)
        assert payload["snapshot_date"] != "invalid@date!"
        assert len(payload["snapshot_date"]) == 10  # YYYY-MM-DD format

    def test_emit_snapshot_metrics_raises_on_non_dict_summary(self, tmp_path) -> None:
        with pytest.raises(ValueError, match="snapshot_summary deve ser um dict"):
            emit_snapshot_metrics(
                snapshot_summary="not a dict",
                metrics_dir=tmp_path,
            )

    def test_emit_snapshot_metrics_raises_on_non_dict_in_list(self, tmp_path) -> None:
        with pytest.raises(ValueError, match="snapshot_summary deve ser um dict"):
            emit_snapshot_metrics(
                snapshot_summary=[1, 2, 3],
                metrics_dir=tmp_path,
            )

    def test_emit_snapshot_metrics_uses_run_id_when_provided(self, tmp_path) -> None:
        snapshot_summary = {
            "snapshot_date": "2026-03-11",
            "bronze": {"rows": 50},
            "silver": {"rows": 45},
            "gold": {"rows": 40},
        }

        result_path = emit_snapshot_metrics(
            snapshot_summary=snapshot_summary,
            metrics_dir=tmp_path,
            run_id="custom-run-id",
        )

        payload = json.loads(result_path.read_text(encoding="utf-8"))
        assert payload["run_id"] == "custom-run-id"

    def test_emit_snapshot_metrics_file_naming_convention(self, tmp_path) -> None:
        snapshot_summary = {
            "snapshot_date": "2026-03-11",
            "bronze": {"rows": 100},
            "silver": {"rows": 95},
            "gold": {"rows": 90},
        }

        result_path = emit_snapshot_metrics(
            snapshot_summary=snapshot_summary,
            metrics_dir=tmp_path,
        )

        assert result_path.name.startswith("snapshot_")
        assert result_path.name.endswith(".json")
