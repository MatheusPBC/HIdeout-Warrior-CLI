from __future__ import annotations

from pathlib import Path

import typer
from rich import print

from core.cloud_config import load_cloud_config
from core.supabase_cloud import sync_directory_to_supabase, sync_file_to_supabase

app = typer.Typer(help="Sync local artifacts to Supabase Storage")


@app.command()
def artifacts(
    snapshots_dir: str = typer.Option(
        "data/training_snapshots",
        "--snapshots-dir",
        help="Local training snapshots root",
    ),
    model_metadata_dir: str = typer.Option(
        "data/model_metadata",
        "--model-metadata-dir",
        help="Local model metadata dir",
    ),
    registry_path: str = typer.Option(
        "data/model_registry/registry.json",
        "--registry-path",
        help="Local model registry file",
    ),
    metrics_dir: str = typer.Option(
        "data/ops_metrics",
        "--metrics-dir",
        help="Local ops metrics dir",
    ),
    reports_dir: str = typer.Option(
        "data/ops_reports",
        "--reports-dir",
        help="Local ops reports dir",
    ),
) -> None:
    config = load_cloud_config()
    if not config.is_configured:
        raise typer.BadParameter(
            "Supabase não configurado. Defina HW_CLOUD_BACKEND=supabase, SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY."
        )

    snapshots_uploaded = sync_directory_to_supabase(
        Path(snapshots_dir),
        artifact_type="training_snapshots",
        metadata={"root": snapshots_dir},
        config=config,
    )
    metadata_uploaded = sync_directory_to_supabase(
        Path(model_metadata_dir),
        artifact_type="model_metadata",
        metadata={"root": model_metadata_dir},
        config=config,
    )
    metrics_uploaded = sync_directory_to_supabase(
        Path(metrics_dir),
        artifact_type="ops_metrics",
        metadata={"root": metrics_dir},
        config=config,
    )
    reports_uploaded = sync_directory_to_supabase(
        Path(reports_dir),
        artifact_type="ops_reports",
        metadata={"root": reports_dir},
        config=config,
    )
    registry_uploaded = sync_file_to_supabase(
        Path(registry_path),
        artifact_type="model_registry",
        metadata={"path": registry_path},
        config=config,
    )

    print(
        "[bold cyan]Supabase sync concluído[/bold cyan] "
        f"snapshots={len(snapshots_uploaded)} metadata={len(metadata_uploaded)} "
        f"metrics={len(metrics_uploaded)} reports={len(reports_uploaded)} "
        f"registry={1 if registry_uploaded else 0}"
    )


if __name__ == "__main__":
    app()
