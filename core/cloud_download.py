"""Download utilities for Supabase Storage artifacts.

Supports cloud-first runtime: when running in lambda/remote environment,
artifacts (snapshots, models) can be downloaded from Supabase before processing.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from core.cloud_config import SupabaseCloudConfig, load_cloud_config

logger = logging.getLogger(__name__)


def _create_supabase_client(config: SupabaseCloudConfig):
    if not config.is_configured:
        raise RuntimeError("Supabase cloud backend não está configurado")
    from supabase import create_client

    return create_client(config.project_url, config.service_role_key)


def download_file_from_supabase(
    artifact_type: str,
    relative_path: str,
    output_path: Path,
    *,
    config: Optional[SupabaseCloudConfig] = None,
) -> bool:
    """Download a single file from Supabase Storage.

    Args:
        artifact_type: artifact category (e.g., 'training_snapshots/gold')
        relative_path: path within the artifact type bucket
        output_path: local destination path
        config: optional cloud config override

    Returns:
        True if download succeeded, False otherwise
    """
    effective_config = config or load_cloud_config()
    if not effective_config.is_configured:
        logger.warning("Supabase não configurado, pulando download")
        return False

    try:
        supabase = _create_supabase_client(effective_config)
        object_path = "/".join(
            part
            for part in (
                effective_config.storage_prefix.strip("/"),
                artifact_type,
                relative_path,
            )
            if part
        )

        output_path.parent.mkdir(parents=True, exist_ok=True)
        data = supabase.storage.from_(effective_config.storage_bucket).download(
            object_path
        )

        with output_path.open("wb") as f:
            f.write(data)

        logger.info(
            "Downloaded %s -> %s (%d bytes)",
            object_path,
            output_path,
            output_path.stat().st_size,
        )
        return True
    except Exception as exc:
        logger.warning("Falha ao baixar %s/%s: %s", artifact_type, relative_path, exc)
        return False


def download_directory_from_supabase(
    artifact_type: str,
    prefix: str,
    output_dir: Path,
    *,
    config: Optional[SupabaseCloudConfig] = None,
) -> list[Path]:
    """Download all files under a prefix from Supabase Storage.

    Args:
        artifact_type: artifact category
        prefix: path prefix within artifact type
        output_dir: local destination directory
        config: optional cloud config override

    Returns:
        List of successfully downloaded file paths
    """
    effective_config = config or load_cloud_config()
    if not effective_config.is_configured:
        logger.warning("Supabase não configurado, pulando download")
        return []

    downloaded: list[Path] = []
    try:
        supabase = _create_supabase_client(effective_config)
        base_path = "/".join(
            part
            for part in (
                effective_config.storage_prefix.strip("/"),
                artifact_type,
                prefix,
            )
            if part
        )

        result = supabase.storage.from_(effective_config.storage_bucket).list(
            path=base_path,
            options={"limit": 1000, "search": ""},
        )

        if not result:
            logger.info("Nenhum arquivo encontrado em %s", base_path)
            return []

        output_dir.mkdir(parents=True, exist_ok=True)

        for item in result:
            if not item.name:
                continue
            remote_path = f"{base_path}/{item.name}"
            local_path = output_dir / item.name

            try:
                data = supabase.storage.from_(effective_config.storage_bucket).download(
                    remote_path
                )
                with local_path.open("wb") as f:
                    f.write(data)
                downloaded.append(local_path)
                logger.debug("Downloaded %s -> %s", remote_path, local_path)
            except Exception as exc:
                logger.warning("Falha ao baixar %s: %s", remote_path, exc)
                continue

        logger.info(
            "Download directory %s -> %s: %d files",
            base_path,
            output_dir,
            len(downloaded),
        )
    except Exception as exc:
        logger.warning(
            "Falha ao listar diretório %s/%s: %s", artifact_type, prefix, exc
        )

    return downloaded


def ensure_latest_gold_snapshot(
    snapshot_date: str,
    output_dir: Path,
    *,
    config: Optional[SupabaseCloudConfig] = None,
) -> Optional[Path]:
    """Ensure the latest gold snapshot is available locally.

    Downloads from Supabase if not present or if local is older.

    Args:
        snapshot_date: date string (YYYY-MM-DD)
        output_dir: local output directory
        config: optional cloud config override

    Returns:
        Path to gold parquet file or None if unavailable
    """
    effective_config = config or load_cloud_config()
    if not effective_config.is_configured:
        return None

    gold_dir = output_dir / "gold"
    expected_pattern = f"snapshot_date={snapshot_date}"

    local_files: list[Path] = []
    if gold_dir.exists():
        local_files = list(gold_dir.rglob("*.parquet"))

    for f in local_files:
        if expected_pattern in str(f):
            logger.info("Gold snapshot já existe localmente: %s", f)
            return f

    logger.info(
        "Gold snapshot não encontrado localmente para %s, tentando download",
        snapshot_date,
    )

    downloaded = download_directory_from_supabase(
        artifact_type="training_snapshots",
        prefix=f"gold/{expected_pattern}",
        output_dir=gold_dir,
        config=effective_config,
    )

    parquet_files = [p for p in downloaded if p.suffix == ".parquet"]
    if parquet_files:
        return parquet_files[0]

    return None
