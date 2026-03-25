from __future__ import annotations

import hashlib
import json
import mimetypes
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional

from core.cloud_config import SupabaseCloudConfig, load_cloud_config


def _file_sha256(file_path: Path) -> str:
    digest = hashlib.sha256()
    with file_path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8192), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _content_type_for(file_path: Path) -> str:
    guessed, _ = mimetypes.guess_type(str(file_path))
    return guessed or "application/octet-stream"


def _build_object_key(
    config: SupabaseCloudConfig,
    artifact_type: str,
    relative_path: str,
) -> str:
    normalized_relative_path = str(relative_path).replace("\\", "/").lstrip("/")
    normalized_type = str(artifact_type).strip().replace(" ", "_")
    return "/".join(
        part
        for part in (
            config.storage_prefix.strip("/"),
            normalized_type,
            normalized_relative_path,
        )
        if part
    )


def _create_supabase_client(config: SupabaseCloudConfig) -> Any:
    if not config.is_configured:
        raise RuntimeError("Supabase cloud backend não está configurado")
    from supabase import create_client

    return create_client(config.project_url, config.service_role_key)


@dataclass(frozen=True)
class CloudArtifactResult:
    artifact_key: str
    bucket_name: str
    object_path: str
    sha256: str
    size_bytes: int


def sync_file_to_supabase(
    file_path: Path,
    *,
    artifact_type: str,
    relative_path: Optional[str] = None,
    metadata: Optional[dict[str, Any]] = None,
    config: Optional[SupabaseCloudConfig] = None,
    client: Any = None,
) -> Optional[CloudArtifactResult]:
    effective_config = config or load_cloud_config()
    target_path = Path(file_path)
    if (
        not effective_config.is_configured
        or not target_path.exists()
        or not target_path.is_file()
    ):
        return None

    supabase = client or _create_supabase_client(effective_config)
    object_path = _build_object_key(
        effective_config,
        artifact_type,
        relative_path or target_path.name,
    )
    with target_path.open("rb") as stream:
        supabase.storage.from_(effective_config.storage_bucket).upload(
            path=object_path,
            file=stream,
            file_options={
                "cache-control": "3600",
                "upsert": "true",
                "content-type": _content_type_for(target_path),
            },
        )

    sha256 = _file_sha256(target_path)
    size_bytes = target_path.stat().st_size
    artifact_key = f"{effective_config.storage_bucket}:{object_path}"
    payload = {
        "artifact_key": artifact_key,
        "artifact_type": artifact_type,
        "bucket_name": effective_config.storage_bucket,
        "object_path": object_path,
        "local_path": str(target_path),
        "content_sha256": sha256,
        "size_bytes": size_bytes,
        "metadata": metadata or {},
    }
    supabase.table(effective_config.artifact_catalog_table).upsert(
        payload,
        on_conflict="artifact_key",
    ).execute()
    return CloudArtifactResult(
        artifact_key=artifact_key,
        bucket_name=effective_config.storage_bucket,
        object_path=object_path,
        sha256=sha256,
        size_bytes=size_bytes,
    )


def sync_directory_to_supabase(
    directory_path: Path,
    *,
    artifact_type: str,
    metadata: Optional[dict[str, Any]] = None,
    config: Optional[SupabaseCloudConfig] = None,
    client: Any = None,
) -> list[CloudArtifactResult]:
    target_dir = Path(directory_path)
    if not target_dir.exists() or not target_dir.is_dir():
        return []
    results: list[CloudArtifactResult] = []
    for file_path in sorted(path for path in target_dir.rglob("*") if path.is_file()):
        relative_path = str(file_path.relative_to(target_dir))
        result = sync_file_to_supabase(
            file_path,
            artifact_type=artifact_type,
            relative_path=relative_path,
            metadata=metadata,
            config=config,
            client=client,
        )
        if result is not None:
            results.append(result)
    return results


def sync_registry_state_to_supabase(
    registry: dict[str, Any],
    *,
    config: Optional[SupabaseCloudConfig] = None,
    client: Any = None,
) -> int:
    effective_config = config or load_cloud_config()
    if not effective_config.is_configured:
        return 0
    supabase = client or _create_supabase_client(effective_config)
    families = registry.get("families", {}) if isinstance(registry, dict) else {}
    rows: list[dict[str, Any]] = []
    if not isinstance(families, dict):
        return 0
    for family, payload in families.items():
        if not isinstance(payload, dict):
            continue
        active_run_id = payload.get("active_version")
        versions = payload.get("versions", [])
        active_version: dict[str, Any] | None = None
        if active_run_id and isinstance(versions, list):
            for version in versions:
                if isinstance(version, dict) and str(version.get("run_id", "")) == str(
                    active_run_id
                ):
                    active_version = version
                    break
        rows.append(
            {
                "family": str(family),
                "active_run_id": str(active_run_id or ""),
                "model_path": str((active_version or {}).get("model_path", "")),
                "model_sha256": str((active_version or {}).get("model_sha256", "")),
                "metrics": (active_version or {}).get("metrics", {}),
                "payload": payload,
            }
        )
    if not rows:
        return 0
    supabase.table(effective_config.active_models_table).upsert(
        rows,
        on_conflict="family",
    ).execute()
    return len(rows)


def sync_snapshot_summary_to_supabase(
    snapshot_summary: dict[str, Any],
    *,
    config: Optional[SupabaseCloudConfig] = None,
    client: Any = None,
) -> bool:
    effective_config = config or load_cloud_config()
    if not effective_config.is_configured:
        return False
    supabase = client or _create_supabase_client(effective_config)
    snapshot_date = str(snapshot_summary.get("snapshot_date") or "")
    run_id = f"snapshot_{snapshot_date}" if snapshot_date else "snapshot_unknown"
    payload = {
        "run_id": run_id,
        "snapshot_date": snapshot_date,
        "bronze_rows": int(snapshot_summary.get("bronze_rows") or 0),
        "silver_rows": int(snapshot_summary.get("silver_rows") or 0),
        "gold_rows": int(snapshot_summary.get("gold_rows") or 0),
        "summary": snapshot_summary,
    }
    supabase.table(effective_config.snapshot_runs_table).upsert(
        payload,
        on_conflict="run_id",
    ).execute()
    return True


def sync_firehose_checkpoint_to_supabase(
    *,
    next_change_id: str,
    pages_processed: int,
    events_ingested: int,
    duplicates_skipped: int,
    config: Optional[SupabaseCloudConfig] = None,
    client: Any = None,
) -> bool:
    effective_config = config or load_cloud_config()
    if not effective_config.is_configured:
        return False
    supabase = client or _create_supabase_client(effective_config)
    payload = {
        "checkpoint_name": "default",
        "next_change_id": str(next_change_id),
        "pages_processed": int(pages_processed),
        "events_ingested": int(events_ingested),
        "duplicates_skipped": int(duplicates_skipped),
    }
    supabase.table(effective_config.firehose_checkpoint_table).upsert(
        payload,
        on_conflict="checkpoint_name",
    ).execute()
    return True


def load_checkpoint_from_supabase(
    *,
    config: Optional[SupabaseCloudConfig] = None,
    client: Any = None,
) -> Optional[dict[str, Any]]:
    """Lê checkpoint atual do Supabase. Retorna dict com campos ou None se indisponível."""
    effective_config = config or load_cloud_config()
    if not effective_config.is_configured:
        return None
    try:
        supabase = client or _create_supabase_client(effective_config)
        result = (
            supabase.table(effective_config.firehose_checkpoint_table)
            .select("*")
            .eq("checkpoint_name", "default")
            .maybe_single()
            .execute()
        )
        if result and result.data:
            row = result.data
            return {
                "next_change_id": str(row.get("next_change_id") or ""),
                "pages_processed": int(row.get("pages_processed") or 0),
                "events_ingested": int(row.get("events_ingested") or 0),
                "duplicates_skipped": int(row.get("duplicates_skipped") or 0),
            }
        return None
    except Exception:
        return None


def upsert_firehose_raw_manifest(
    *,
    run_id: str,
    object_path: str,
    rows_count: int,
    page_start_change_id: str,
    page_end_change_id: str,
    file_size_bytes: int,
    content_sha256: str,
    status: str,
    error_message: str | None,
    config: Optional[SupabaseCloudConfig] = None,
    client: Any = None,
) -> bool:
    """Upsert um registro no firehose_raw_manifest.

    Usa object_path como chave de idempotência (ON CONFLICT DO UPDATE).
    """
    effective_config = config or load_cloud_config()
    if not effective_config.is_configured:
        return False
    supabase = client or _create_supabase_client(effective_config)
    payload = {
        "run_id": str(run_id),
        "object_path": str(object_path),
        "rows_count": int(rows_count),
        "page_start_change_id": str(page_start_change_id),
        "page_end_change_id": str(page_end_change_id),
        "file_size_bytes": int(file_size_bytes),
        "content_sha256": str(content_sha256),
        "status": str(status),
        "error_message": str(error_message) if error_message else None,
    }
    try:
        supabase.table(effective_config.firehose_raw_manifest_table).upsert(
            payload,
            on_conflict="object_path",
        ).execute()
        return True
    except Exception:
        return False


def download_file_from_supabase(
    remote_path: str,
    local_destination: Path,
    *,
    artifact_type: str,
    config: Optional[SupabaseCloudConfig] = None,
    client: Any = None,
) -> bool:
    """Baixa um arquivo do Supabase Storage para destino local.

    Returns:
        True se download bem-sucedido, False caso contrário.
    """
    effective_config = config or load_cloud_config()
    if not effective_config.is_configured:
        return False

    supabase = client or _create_supabase_client(effective_config)
    object_key = _build_object_key(effective_config, artifact_type, remote_path)

    try:
        data = supabase.storage.from_(effective_config.storage_bucket).download(
            path=object_key
        )
        local_destination.parent.mkdir(parents=True, exist_ok=True)
        with local_destination.open("wb") as f:
            f.write(data)
        return True
    except Exception:
        return False


def list_artifacts_from_supabase(
    artifact_type: str,
    *,
    config: Optional[SupabaseCloudConfig] = None,
    client: Any = None,
) -> list[dict[str, Any]]:
    """Lista artefatos de um tipo específico no Supabase Storage.

    Returns:
        Lista de dicts com artifact_key, object_path, metadata.
    """
    effective_config = config or load_cloud_config()
    if not effective_config.is_configured:
        return []

    supabase = client or _create_supabase_client(effective_config)
    prefix = f"{effective_config.storage_prefix.strip('/')}/{artifact_type.replace(' ', '_')}/"

    try:
        result = (
            supabase.table(effective_config.artifact_catalog_table)
            .select("*")
            .like("object_path", f"{prefix}%")
            .execute()
        )
        return result.data or []
    except Exception:
        return []


def sync_ops_metrics_to_supabase(
    metrics_file_path: Path,
    *,
    config: Optional[SupabaseCloudConfig] = None,
    client: Any = None,
) -> bool:
    """Faz upload de arquivo de métricas ops para Supabase Storage.

    Returns:
        True se upload bem-sucedido, False caso contrário.
    """
    effective_config = config or load_cloud_config()
    if not effective_config.is_configured:
        return False
    if not metrics_file_path.exists():
        return False

    try:
        sync_file_to_supabase(
            metrics_file_path,
            artifact_type="ops_metrics",
            metadata={"file_name": metrics_file_path.name},
            config=config,
            client=client,
        )
        return True
    except Exception:
        return False
