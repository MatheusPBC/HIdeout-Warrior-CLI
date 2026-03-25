import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from core.supabase_cloud import sync_file_to_supabase


DEFAULT_OPS_METRICS_DIR = Path("data/ops_metrics")

# Sanitização máxima: só alfanumérico + hifens + underscores, até 32 chars.
_SNAPSHOT_DATE_RE = re.compile(r"^[a-zA-Z0-9_\-]{1,32}$")
_RUN_ID_RE = re.compile(r"^[a-zA-Z0-9_\-]{1,64}$")


def _utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _sanitize_for_filename(value: str, pattern: re.Pattern, fallback: str) -> str:
    """Retorna valor sanitizado para uso em nome de arquivo, ou fallback se inválido."""
    if value and pattern.match(value):
        return value
    return fallback


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
    if not component or not status:
        raise ValueError("component e status são obrigatórios")
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


def emit_snapshot_metrics(
    snapshot_summary: Dict[str, Any],
    metrics_dir: Path = DEFAULT_OPS_METRICS_DIR,
    run_id: Optional[str] = None,
) -> Path:
    """
    Persiste métricas de execução de snapshot em JSON.

    Args:
        snapshot_summary: dict com estrutura de bronze/silver/gold do build_training_snapshot
        metrics_dir: diretório base para ops_metrics
        run_id: identificador da execução (default: timestamp UTC)

    Returns:
        Path para o arquivo JSON criado
    """
    if not isinstance(snapshot_summary, dict):
        raise ValueError("snapshot_summary deve ser um dict")

    metrics_dir.mkdir(parents=True, exist_ok=True)

    raw_date = str(snapshot_summary.get("snapshot_date", ""))
    safe_date = _sanitize_for_filename(raw_date, _SNAPSHOT_DATE_RE, _utc_now_iso()[:10])

    safe_run_id = _sanitize_for_filename(
        run_id or _utc_now_iso(), _RUN_ID_RE, _utc_now_iso()
    )

    payload = {
        "ts_utc": _utc_now_iso(),
        "run_id": safe_run_id,
        "snapshot_date": safe_date,
        "bronze": snapshot_summary.get("bronze", {}),
        "silver": snapshot_summary.get("silver", {}),
        "gold": snapshot_summary.get("gold", {}),
    }

    content = json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True)

    # Escrita atômica: temp file + rename
    tmp_fd, tmp_path = tempfile.mkstemp(dir=str(metrics_dir), suffix=".json")
    try:
        os.write(tmp_fd, content.encode("utf-8"))
        os.close(tmp_fd)
        os.replace(tmp_path, metrics_dir / f"snapshot_{safe_date}.json")
    except Exception:
        try:
            os.close(tmp_fd)
        except OSError:
            pass
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise

    target_file = metrics_dir / f"snapshot_{safe_date}.json"
    try:
        sync_file_to_supabase(
            target_file,
            artifact_type="snapshot_metrics",
            metadata={"snapshot_date": safe_date, "run_id": safe_run_id},
        )
    except Exception:
        pass
    return target_file
