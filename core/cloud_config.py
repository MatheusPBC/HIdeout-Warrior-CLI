from __future__ import annotations

import os
from dataclasses import dataclass


def _clean_optional_str(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


@dataclass(frozen=True)
class SupabaseCloudConfig:
    # Cloud-first: default é supabase se SUPABASE_URL existir, senão local
    backend: str = "supabase"
    project_url: str | None = None
    service_role_key: str | None = None
    storage_bucket: str = "hideout-warrior-data"
    storage_prefix: str = "hideout-warrior"
    artifact_catalog_table: str = "artifact_catalog"
    active_models_table: str = "active_models"
    snapshot_runs_table: str = "snapshot_runs"
    firehose_checkpoint_table: str = "firehose_checkpoints"
    firehose_raw_bucket: str = "firehose-raw"
    firehose_raw_manifest_table: str = "firehose_raw_manifest"

    @property
    def enabled(self) -> bool:
        return self.backend == "supabase"

    @property
    def is_configured(self) -> bool:
        return bool(self.enabled and self.project_url and self.service_role_key)


def load_cloud_config() -> SupabaseCloudConfig:
    # Lógica cloud-first: se backend explícito, usar; senão se SUPABASE_URL existe, usar supabase; senão local
    explicit_backend = _clean_optional_str(
        os.getenv("HW_CLOUD_BACKEND")
    ) or _clean_optional_str(os.getenv("HIDEOUT_STORAGE_BACKEND"))
    if explicit_backend is not None:
        resolved_backend = explicit_backend.lower()
    elif _clean_optional_str(os.getenv("SUPABASE_URL")) is not None:
        resolved_backend = "supabase"
    else:
        resolved_backend = "local"

    return SupabaseCloudConfig(
        backend=resolved_backend,
        project_url=_clean_optional_str(os.getenv("SUPABASE_URL")),
        service_role_key=(
            _clean_optional_str(os.getenv("SUPABASE_SERVICE_ROLE_KEY"))
            or _clean_optional_str(os.getenv("SUPABASE_KEY"))
        ),
        storage_bucket=(
            _clean_optional_str(os.getenv("SUPABASE_STORAGE_BUCKET"))
            or "hideout-warrior-data"
        ),
        storage_prefix=(
            _clean_optional_str(os.getenv("SUPABASE_STORAGE_PREFIX"))
            or "hideout-warrior"
        ),
        artifact_catalog_table=(
            _clean_optional_str(os.getenv("SUPABASE_ARTIFACT_CATALOG_TABLE"))
            or "artifact_catalog"
        ),
        active_models_table=(
            _clean_optional_str(os.getenv("SUPABASE_ACTIVE_MODELS_TABLE"))
            or "active_models"
        ),
        snapshot_runs_table=(
            _clean_optional_str(os.getenv("SUPABASE_SNAPSHOT_RUNS_TABLE"))
            or "snapshot_runs"
        ),
        firehose_checkpoint_table=(
            _clean_optional_str(os.getenv("SUPABASE_FIREHOSE_CHECKPOINT_TABLE"))
            or "firehose_checkpoints"
        ),
        firehose_raw_bucket=(
            _clean_optional_str(os.getenv("SUPABASE_FIREHOSE_RAW_BUCKET"))
            or "firehose-raw"
        ),
        firehose_raw_manifest_table=(
            _clean_optional_str(os.getenv("SUPABASE_FIREHOSE_RAW_MANIFEST_TABLE"))
            or "firehose_raw_manifest"
        ),
    )
