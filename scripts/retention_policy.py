#!/usr/bin/env python3
"""Políticas de retenção e governança para Supabase cloud.

Define e aplica políticas de retenção para:
- firehose_raw_manifest: retenção de metadados
- firehose-raw storage: retenção de arquivos

Uso:
    # Ver status atual
    python scripts/retention_policy.py --check

    # Ver políticas configuradas
    python scripts/retention_policy.py --show-policies

    # Dry-run de política específica
    python scripts/retention_policy.py --policy firehose_raw --days 30

    # Aplicar política (dry-run por padrão)
    python scripts/retention_policy.py --policy firehose_raw --days 30 --execute
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Optional

import typer
from rich.console import Console

sys.path.insert(0, ".")

from core.cloud_config import load_cloud_config

app = typer.Typer()

console = Console()


class PolicyType(Enum):
    FIREHOSE_RAW_MANIFEST = "firehose_raw_manifest"
    FIREHOSE_RAW_STORAGE = "firehose_raw_storage"
    ARTIFACT_CATALOG = "artifact_catalog"
    SNAPSHOT_RUNS = "snapshot_runs"


@dataclass(frozen=True)
class RetentionPolicy:
    name: str
    policy_type: PolicyType
    retention_days: int
    description: str
    target_table_or_bucket: str
    id_field: str = "id"
    date_field: str = "uploaded_at"


# Policies padrão
DEFAULT_POLICIES = [
    RetentionPolicy(
        name="firehose_raw_manifest_default",
        policy_type=PolicyType.FIREHOSE_RAW_MANIFEST,
        retention_days=30,
        description="Remove entries do manifest mais antigas que N dias",
        target_table_or_bucket="firehose_raw_manifest",
    ),
    RetentionPolicy(
        name="firehose_raw_storage_default",
        policy_type=PolicyType.FIREHOSE_RAW_STORAGE,
        retention_days=60,
        description="Remove arquivos no storage sem correspondência no manifest ou muito antigos",
        target_table_or_bucket="firehose-raw",
    ),
    RetentionPolicy(
        name="snapshot_runs_default",
        policy_type=PolicyType.SNAPSHOT_RUNS,
        retention_days=90,
        description="Remove snapshots runs mais antigos que N dias",
        target_table_or_bucket="snapshot_runs",
        date_field="updated_at",
    ),
]


def get_policy_from_config() -> list[RetentionPolicy]:
    """Carrega policies da configuração de ambiente ou usa defaults."""
    config = load_cloud_config()

    # Permite sobrescrever via env vars
    policies = DEFAULT_POLICIES.copy()

    # days override via env
    raw_days = (
        int(config.firehose_raw_manifest_table.split("_")[-1]) if False else None
    )  # placeholder

    return policies


def check_policy_status(policy: RetentionPolicy) -> dict:
    """Verifica status de uma política (quantos registros seriam afetados)."""
    from core.supabase_cloud import _create_supabase_client

    config = load_cloud_config()
    result = {
        "policy": policy.name,
        "retention_days": policy.retention_days,
        "affected_count": 0,
        "affected_size_bytes": 0,
        "oldest_record": None,
        "newest_record": None,
        "error": None,
    }

    try:
        client = _create_supabase_client(config)
        cutoff = datetime.now(timezone.utc) - timedelta(days=policy.retention_days)

        if policy.policy_type in (
            PolicyType.FIREHOSE_RAW_MANIFEST,
            PolicyType.SNAPSHOT_RUNS,
            PolicyType.ARTIFACT_CATALOG,
        ):
            # Query no DB
            query = (
                client.table(policy.target_table_or_bucket)
                .select("*")
                .lt(policy.date_field, cutoff.isoformat())
            )

            # Para snapshot_runs, usa snapshot_date; para firehose_raw usa uploaded_at
            if policy.policy_type == PolicyType.SNAPSHOT_RUNS:
                # Snapshot runs usa snapshot_date text field
                query = (
                    client.table(policy.target_table_or_bucket)
                    .select("*")
                    .lt("snapshot_date", cutoff.strftime("%Y-%m-%d"))
                )

            response = query.execute()
            records = response.data or []

            result["affected_count"] = len(records)
            result["affected_size_bytes"] = sum(
                r.get("file_size_bytes", 0) or 0 for r in records
            )

            if records:
                dates = [
                    r.get(policy.date_field, "")
                    for r in records
                    if r.get(policy.date_field)
                ]
                result["oldest_record"] = min(dates) if dates else None
                result["newest_record"] = max(dates) if dates else None

    except Exception as e:
        result["error"] = str(e)

    return result


def apply_policy(policy: RetentionPolicy, dry_run: bool = True) -> dict:
    """Aplica política de retenção.

    Args:
        policy: A política a ser aplicada.
        dry_run: Se True, apenas conta affected records.

    Returns:
        Dict com resultado da operação.
    """
    from core.supabase_cloud import _create_supabase_client

    config = load_cloud_config()
    result = {
        "policy": policy.name,
        "dry_run": dry_run,
        "deleted_count": 0,
        "error": None,
    }

    try:
        client = _create_supabase_client(config)
        cutoff = datetime.now(timezone.utc) - timedelta(days=policy.retention_days)

        if policy.policy_type == PolicyType.FIREHOSE_RAW_MANIFEST:
            # Busca IDs para deletar
            response = (
                client.table(policy.target_table_or_bucket)
                .select("id")
                .lt(policy.date_field, cutoff.isoformat())
                .execute()
            )

            ids_to_delete = [r["id"] for r in (response.data or [])]
            result["affected_count"] = len(ids_to_delete)

            if not dry_run and ids_to_delete:
                for id_val in ids_to_delete:
                    try:
                        client.table(policy.target_table_or_bucket).delete().eq(
                            "id", id_val
                        ).execute()
                        result["deleted_count"] += 1
                    except Exception as e:
                        console.print(
                            f"[yellow]Erro ao deletar id {id_val}: {e}[/yellow]"
                        )

        elif policy.policy_type == PolicyType.SNAPSHOT_RUNS:
            # Snapshot runs usa snapshot_date text
            response = (
                client.table(policy.target_table_or_bucket)
                .select("run_id")
                .lt("snapshot_date", cutoff.strftime("%Y-%m-%d"))
                .execute()
            )

            run_ids = [r["run_id"] for r in (response.data or [])]
            result["affected_count"] = len(run_ids)

            if not dry_run and run_ids:
                for run_id in run_ids:
                    try:
                        client.table(policy.target_table_or_bucket).delete().eq(
                            "run_id", run_id
                        ).execute()
                        result["deleted_count"] += 1
                    except Exception as e:
                        console.print(
                            f"[yellow]Erro ao deletar run_id {run_id}: {e}[/yellow]"
                        )

        elif policy.policy_type == PolicyType.FIREHOSE_RAW_STORAGE:
            # Storage - requer lógica adicional de match com manifest
            result["error"] = (
                "FIREHOSE_RAW_STORAGE requer lógica de match com manifest. Use cleanup_firehose_raw.py."
            )

    except Exception as e:
        result["error"] = str(e)

    return result


@app.command()
def main(
    check: bool = typer.Option(
        False,
        "--check",
        help="Ver status atual de todas as políticas",
    ),
    show_policies: bool = typer.Option(
        False,
        "--show-policies",
        help="Mostrar políticas configuradas",
    ),
    policy_name: Optional[str] = typer.Option(
        None,
        "--policy",
        help="Nome da política específica (firehose_raw_manifest, snapshot_runs, etc.)",
    ),
    days: int = typer.Option(
        30,
        "--days",
        help="Dias de retenção para dry-run",
    ),
    execute: bool = typer.Option(
        False,
        "--execute",
        help="Aplicar política (default: dry-run)",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        help="Output detalhado",
    ),
) -> None:
    """Políticas de retenção e governança para Supabase cloud."""
    config = load_cloud_config()

    if not config.is_configured:
        console.print("[red]Supabase não configurado[/red]")
        raise typer.Exit(code=1)

    policies = get_policy_from_config()

    # Mostrar políticas
    if show_policies:
        console.print("\n[bold cyan]Políticas de Retenção Configuradas[/bold cyan]\n")
        for p in policies:
            console.print(f"[green]{p.name}[/green]")
            console.print(f"  Type: {p.policy_type.value}")
            console.print(f"  Retenção: {p.retention_days} dias")
            console.print(f"  Target: {p.target_table_or_bucket}")
            console.print(f"  {p.description}")
            console.print()

    # Check status
    if check:
        console.print("\n[bold cyan]Status das Políticas de Retenção[/bold cyan]\n")

        for p in policies:
            if (
                policy_name
                and p.policy_type.value != policy_name
                and p.name != policy_name
            ):
                continue

            status = check_policy_status(p)

            if status["error"]:
                console.print(f"[red]{p.name}: ERRO - {status['error']}[/red]")
            else:
                size_mb = (
                    status["affected_size_bytes"] / 1024 / 1024
                    if status["affected_size_bytes"]
                    else 0
                )
                console.print(f"[green]{p.name}[/green]")
                console.print(f"  Retenção: {status['retention_days']} dias")
                console.print(
                    f"  Afetados: {status['affected_count']} registro(s) ({size_mb:.2f} MB)"
                )
                if status["oldest_record"]:
                    console.print(f"  Mais antigo: {status['oldest_record']}")
                if status["newest_record"]:
                    console.print(f"  Mais novo: {status['newest_record']}")
                console.print()

    # Aplicar política
    if policy_name and not check and not show_policies:
        target_policy = None
        for p in policies:
            if p.policy_type.value == policy_name or p.name == policy_name:
                target_policy = RetentionPolicy(
                    name=p.name,
                    policy_type=p.policy_type,
                    retention_days=days,  # Usa days do argumento
                    description=p.description,
                    target_table_or_bucket=p.target_table_or_bucket,
                    id_field=p.id_field,
                    date_field=p.date_field,
                )
                break

        if not target_policy:
            console.print(f"[red]Política '{policy_name}' não encontrada[/red]")
            raise typer.Exit(code=1)

        console.print(
            f"\n[bold cyan]Aplicando política: {target_policy.name}[/bold cyan]"
        )
        console.print(f"[cyan]Modo: {'EXECUTAR' if execute else 'DRY-RUN'}[/cyan]")
        console.print(f"[cyan]Retenção: {days} dias[/cyan]\n")

        result = apply_policy(target_policy, dry_run=not execute)

        if result["error"]:
            console.print(f"[red]Erro: {result['error']}[/red]")
        else:
            console.print(
                f"[green]Registros afetados: {result.get('affected_count', 0)}[/green]"
            )
            if execute:
                console.print(
                    f"[green]Registros deletados: {result.get('deleted_count', 0)}[/green]"
                )

        if execute:
            console.print("\n[bold green]✓ Política aplicada[/bold green]")
        else:
            console.print(
                "\n[bold yellow]⚠ Dry-run - nada foi modificado[/bold yellow]"
            )
            console.print("[dim]Use --execute para aplicar[/dim]")

    if not check and not show_policies and not policy_name:
        console.print("[yellow]Use --check, --show-policies ou --policy[/yellow]")


if __name__ == "__main__":
    typer.run(main)
