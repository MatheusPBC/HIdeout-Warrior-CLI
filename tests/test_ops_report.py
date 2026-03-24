import json
from pathlib import Path

from scripts.ops_report import _load_snapshot_metrics, build


def test_ops_report_build_consolidates_metrics_and_registry(tmp_path: Path) -> None:
    metrics_dir = tmp_path / "ops_metrics"
    metrics_dir.mkdir(parents=True, exist_ok=True)
    (metrics_dir / "2026-03-10.jsonl").write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "ts_utc": "2026-03-10T10:00:00Z",
                        "component": "ops_cycle.daily_run",
                        "run_id": "run-1",
                        "duration_ms": 100,
                        "status": "ok",
                        "error_count": 0,
                        "payload": {},
                    }
                ),
                "{invalid-json-line}",
                json.dumps(
                    {
                        "ts_utc": "2026-03-10T12:00:00Z",
                        "component": "ops_cycle.daily_run",
                        "run_id": "run-2",
                        "duration_ms": 300,
                        "status": "error",
                        "error_count": 1,
                        "payload": {},
                    }
                ),
                json.dumps(
                    {
                        "ts_utc": "2026-03-10T13:00:00Z",
                        "component": "firehose_miner.run",
                        "run_id": "run-3",
                        "duration_ms": 50,
                        "status": "ok",
                        "error_count": 0,
                        "payload": {},
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (metrics_dir / "2026-03-11.jsonl").write_text(
        json.dumps(
            {
                "ts_utc": "2026-03-11T09:00:00Z",
                "component": "ops_cycle.daily_run",
                "run_id": "run-4",
                "duration_ms": 200,
                "status": "ok",
                "error_count": 0,
                "payload": {},
            }
        )
        + "\n",
        encoding="utf-8",
    )

    registry_path = tmp_path / "model_registry" / "registry.json"
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    registry_path.write_text(
        json.dumps(
            {
                "families": {
                    "generic": {"active_version": "run-4", "versions": []},
                    "wand_caster": {"active_version": None, "versions": []},
                }
            }
        ),
        encoding="utf-8",
    )

    output_path = tmp_path / "ops_reports" / "report.json"
    build(
        metrics_dir=str(metrics_dir),
        registry_path=str(registry_path),
        output_path=str(output_path),
    )

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    components = payload["metrics"]["components"]

    assert "ops_cycle.daily_run" in components
    assert components["ops_cycle.daily_run"]["runs"] == 3
    assert components["ops_cycle.daily_run"]["ok"] == 2
    assert components["ops_cycle.daily_run"]["error"] == 1
    assert components["ops_cycle.daily_run"]["error_rate"] == 1 / 3
    assert components["ops_cycle.daily_run"]["duration_ms"]["avg"] == 200
    assert components["ops_cycle.daily_run"]["duration_ms"]["p50"] == 200
    assert components["ops_cycle.daily_run"]["duration_ms"]["p95"] == 300
    assert components["ops_cycle.daily_run"]["last_ts_utc"] == "2026-03-11T09:00:00Z"

    assert payload["registry"]["active_by_family"]["generic"] == "run-4"
    assert payload["registry"]["active_by_family"]["wand_caster"] is None


def test_load_snapshot_metrics_reads_bronze_silver_gold_structure(
    tmp_path: Path,
) -> None:
    """Validate _load_snapshot_metrics parses snapshot JSON with bronze/silver/gold layers."""
    metrics_dir = tmp_path / "ops_metrics"
    metrics_dir.mkdir(parents=True, exist_ok=True)

    snapshot_payload = {
        "snapshot_date": "2026-03-11",
        "run_id": "snapshot_2026-03-11",
        "ts_utc": "2026-03-11T12:00:00Z",
        "bronze": {
            "rows": 100,
            "rows_read": 120,
            "rows_valid": 110,
            "rows_deduped": 10,
            "partitions": 5,
            "source_distribution": {"stash": 60, "trade": 40},
            "freshness_distribution": {"fresh": 80, "active": 20},
            "dedup_rate": 0.083,
        },
        "silver": {
            "rows": 95,
            "rows_input": 110,
            "rows_output": 95,
            "normalization_failures": 15,
            "partitions": 8,
        },
        "gold": {
            "rows": 90,
            "rows_input": 95,
            "rows_output": 90,
            "feature_extraction_failures": 5,
            "partitions": 10,
        },
    }
    (metrics_dir / "snapshot_2026-03-11.json").write_text(
        json.dumps(snapshot_payload), encoding="utf-8"
    )

    result = _load_snapshot_metrics(metrics_dir)

    assert result["snapshot_date"] == "2026-03-11"
    assert result["run_id"] == "snapshot_2026-03-11"
    assert result["bronze"]["rows"] == 100
    assert result["silver"]["rows"] == 95
    assert result["gold"]["rows"] == 90
    assert result["bronze"]["source_distribution"]["stash"] == 60
    assert result["bronze"]["dedup_rate"] == 0.083
    assert result["silver"]["normalization_failures"] == 15
    assert result["gold"]["feature_extraction_failures"] == 5
    assert "source_file" in result


def test_load_snapshot_metrics_returns_empty_when_no_snapshot_files(
    tmp_path: Path,
) -> None:
    """Validate _load_snapshot_metrics returns empty dict when no snapshot files exist."""
    metrics_dir = tmp_path / "ops_metrics"
    metrics_dir.mkdir(parents=True, exist_ok=True)

    result = _load_snapshot_metrics(metrics_dir)

    assert result == {}


def test_ops_report_build_includes_snapshot_metrics_in_output(tmp_path: Path) -> None:
    """Validate build command includes snapshot metrics in the report output."""
    metrics_dir = tmp_path / "ops_metrics"
    metrics_dir.mkdir(parents=True, exist_ok=True)

    # Create a minimal jsonl to avoid empty metrics error
    (metrics_dir / "2026-03-11.jsonl").write_text(
        json.dumps(
            {
                "ts_utc": "2026-03-11T09:00:00Z",
                "component": "test_component",
                "run_id": "run-1",
                "duration_ms": 100,
                "status": "ok",
                "error_count": 0,
                "payload": {},
            }
        )
        + "\n",
        encoding="utf-8",
    )

    # Create snapshot metrics file
    snapshot_payload = {
        "snapshot_date": "2026-03-11",
        "run_id": "snapshot_2026-03-11",
        "ts_utc": "2026-03-11T12:00:00Z",
        "bronze": {"rows": 50},
        "silver": {"rows": 45},
        "gold": {"rows": 40},
    }
    (metrics_dir / "snapshot_2026-03-11.json").write_text(
        json.dumps(snapshot_payload), encoding="utf-8"
    )

    registry_path = tmp_path / "model_registry" / "registry.json"
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    registry_path.write_text(json.dumps({"families": {}}), encoding="utf-8")

    output_path = tmp_path / "ops_reports" / "report.json"
    build(
        metrics_dir=str(metrics_dir),
        registry_path=str(registry_path),
        output_path=str(output_path),
    )

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    snapshot = payload["metrics"]["snapshot"]

    assert snapshot["snapshot_date"] == "2026-03-11"
    assert snapshot["bronze"]["rows"] == 50
    assert snapshot["silver"]["rows"] == 45
    assert snapshot["gold"]["rows"] == 40
