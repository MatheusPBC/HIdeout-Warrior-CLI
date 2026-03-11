import json
from pathlib import Path

from scripts.ops_report import build


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
