"""Download utilities for Supabase Storage artifacts.

Supports cloud-first runtime: when running in lambda/remote environment,
artifacts (snapshots, models) can be downloaded from Supabase before processing.

Artifact Integrity (Bloco B - Fase 2):
- Downloads return DownloadResult with validation metadata
- Legacy artifacts (no checksum in catalog) are marked checksum_validated=False
- New artifacts can request validation against expected checksum
- Validation failures are logged but don't break downloads (strategy 2)
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from core.cloud_config import SupabaseCloudConfig, load_cloud_config

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DownloadResult:
    """Result of a cloud download with integrity metadata."""

    local_path: Path
    success: bool
    checksum_validated: (
        bool  # True if SHA256 matched expected, False if legacy/unvalidated
    )
    expected_sha256: Optional[str]  # None for legacy artifacts
    actual_sha256: Optional[str]  # None if download failed before checksum compute
    error_message: Optional[str]

    @property
    def is_legacy(self) -> bool:
        """True if this artifact has no checksum validation (legacy)."""
        return not self.checksum_validated and self.expected_sha256 is None


def _compute_file_sha256(file_path: Path) -> Optional[str]:
    """Compute SHA256 of a file, returns None on error."""
    try:
        digest = hashlib.sha256()
        with file_path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(8192), b""):
                digest.update(chunk)
        return digest.hexdigest()
    except Exception as exc:
        logger.warning("Falha ao calcular SHA256 de %s: %s", file_path, exc)
        return None


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
    expected_sha256: Optional[str] = None,
    validate_checksum: bool = True,
) -> DownloadResult:
    """Download a single file from Supabase Storage with optional integrity check.

    Args:
        artifact_type: artifact category (e.g., 'training_snapshots/gold')
        relative_path: path within the artifact type bucket
        output_path: local destination path
        config: optional cloud config override
        expected_sha256: if provided, validate download against this SHA256
        validate_checksum: if True (default), compute and compare SHA256 when expected_sha256 provided

    Returns:
        DownloadResult with integrity metadata
    """
    effective_config = config or load_cloud_config()
    if not effective_config.is_configured:
        logger.warning("Supabase não configurado, pulando download")
        return DownloadResult(
            local_path=output_path,
            success=False,
            checksum_validated=False,
            expected_sha256=expected_sha256,
            actual_sha256=None,
            error_message="Supabase not configured",
        )

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

        # Compute actual checksum after successful download
        actual_sha256 = _compute_file_sha256(output_path)

        # Determine validation status
        if expected_sha256 and validate_checksum:
            checksum_validated = actual_sha256 == expected_sha256
            if not checksum_validated:
                logger.warning(
                    "Checksum mismatch for %s: expected=%s actual=%s",
                    object_path,
                    expected_sha256,
                    actual_sha256,
                )
                return DownloadResult(
                    local_path=output_path,
                    success=True,  # Download succeeded, validation failed
                    checksum_validated=False,
                    expected_sha256=expected_sha256,
                    actual_sha256=actual_sha256,
                    error_message="Checksum mismatch",
                )
        else:
            # Legacy artifact or validation disabled
            checksum_validated = False  # Legacy artifacts don't have checksum

        logger.info(
            "Downloaded %s -> %s (%d bytes) validated=%s",
            object_path,
            output_path,
            output_path.stat().st_size,
            checksum_validated,
        )
        return DownloadResult(
            local_path=output_path,
            success=True,
            checksum_validated=checksum_validated,
            expected_sha256=expected_sha256 if expected_sha256 else None,
            actual_sha256=actual_sha256,
            error_message=None,
        )
    except Exception as exc:
        logger.warning("Falha ao baixar %s/%s: %s", artifact_type, relative_path, exc)
        return DownloadResult(
            local_path=output_path,
            success=False,
            checksum_validated=False,
            expected_sha256=expected_sha256,
            actual_sha256=None,
            error_message=str(exc)[:500],
        )


def download_directory_from_supabase(
    artifact_type: str,
    prefix: str,
    output_dir: Path,
    *,
    config: Optional[SupabaseCloudConfig] = None,
    validate_checksum: bool = False,
) -> list[DownloadResult]:
    """Download all files under a prefix from Supabase Storage.

    Args:
        artifact_type: artifact category
        prefix: path prefix within artifact type
        output_dir: local destination directory
        config: optional cloud config override
        validate_checksum: if True, compute checksums for each file (for legacy tracking)

    Returns:
        List of DownloadResult for each attempted file
    """
    effective_config = config or load_cloud_config()
    if not effective_config.is_configured:
        logger.warning("Supabase não configurado, pulando download")
        return []

    results: list[DownloadResult] = []
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
            if not item.get("name"):
                continue
            remote_path = f"{base_path}/{item['name']}"
            local_path = output_dir / item["name"]

            try:
                data = supabase.storage.from_(effective_config.storage_bucket).download(
                    remote_path
                )
                with local_path.open("wb") as f:
                    f.write(data)

                # Compute checksum if requested (for integrity tracking)
                actual_sha256 = None
                if validate_checksum:
                    actual_sha256 = _compute_file_sha256(local_path)

                results.append(
                    DownloadResult(
                        local_path=local_path,
                        success=True,
                        checksum_validated=False,  # Legacy - no expected checksum to compare
                        expected_sha256=None,
                        actual_sha256=actual_sha256,
                        error_message=None,
                    )
                )
                logger.debug("Downloaded %s -> %s", remote_path, local_path)
            except Exception as exc:
                logger.warning("Falha ao baixar %s: %s", remote_path, exc)
                results.append(
                    DownloadResult(
                        local_path=local_path,
                        success=False,
                        checksum_validated=False,
                        expected_sha256=None,
                        actual_sha256=None,
                        error_message=str(exc)[:500],
                    )
                )
                continue

        logger.info(
            "Download directory %s -> %s: %d files, %d succeeded",
            base_path,
            output_dir,
            len(results),
            sum(1 for r in results if r.success),
        )
    except Exception as exc:
        logger.warning(
            "Falha ao listar diretório %s/%s: %s", artifact_type, prefix, exc
        )

    return results


def ensure_latest_gold_snapshot(
    snapshot_date: str,
    output_dir: Path,
    *,
    config: Optional[SupabaseCloudConfig] = None,
) -> tuple[Optional[Path], list[DownloadResult]]:
    """Ensure the latest gold snapshot is available locally.

    Downloads from Supabase if not present or if local is older.

    Args:
        snapshot_date: date string (YYYY-MM-DD)
        output_dir: local output directory
        config: optional cloud config override

    Returns:
        Tuple of (Path to gold parquet file or None if unavailable, list of download results)
    """
    effective_config = config or load_cloud_config()
    if not effective_config.is_configured:
        return None, []

    gold_dir = output_dir / "gold"
    expected_pattern = f"snapshot_date={snapshot_date}"

    local_files: list[Path] = []
    if gold_dir.exists():
        local_files = list(gold_dir.rglob("*.parquet"))

    for f in local_files:
        if expected_pattern in str(f):
            logger.info("Gold snapshot já existe localmente: %s", f)
            return f, []

    logger.info(
        "Gold snapshot não encontrado localmente para %s, tentando download",
        snapshot_date,
    )

    download_results = download_directory_from_supabase(
        artifact_type="training_snapshots",
        prefix=f"gold/{expected_pattern}",
        output_dir=gold_dir,
        config=effective_config,
    )

    parquet_files = [
        r.local_path
        for r in download_results
        if r.local_path.suffix == ".parquet" and r.success
    ]
    if parquet_files:
        return parquet_files[0], download_results

    return None, download_results
