import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict


DEFAULT_OPS_METRICS_DIR = Path("data/ops_metrics")


def _utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def append_metric_event(
    *,
    component: str,
    run_id: str,
    duration_ms: float,
    status: str,
    error_count: int,
    payload: Dict[str, Any],
    metrics_dir: Path = DEFAULT_OPS_METRICS_DIR,
) -> Path:
    metrics_dir.mkdir(parents=True, exist_ok=True)
    target_path = metrics_dir / f"{datetime.now(timezone.utc):%Y-%m-%d}.jsonl"

    event = {
        "ts_utc": _utc_now_iso(),
        "component": component,
        "run_id": run_id,
        "duration_ms": int(max(duration_ms, 0.0)),
        "status": status,
        "error_count": int(max(error_count, 0)),
        "payload": payload,
    }
    line = json.dumps(event, ensure_ascii=True, separators=(",", ":")) + "\n"

    fd = os.open(str(target_path), os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o644)
    try:
        os.write(fd, line.encode("utf-8"))
    finally:
        os.close(fd)
    return target_path
