#!/usr/bin/env python3
"""Cleanup/governança básica para firehose_raw_manifest e storage.

Funcionalidades:
- Lista arquivos órfãos no storage sem manifest
- Remove entries do manifest por idade
- Remove arquivos órfãos do storage
- Dry-run por padrão (não destrói nada)

Uso:
    # Dry-run (default)
    python scripts/cleanup_firehose_raw.py

    # Executar cleanup de fato
    python scripts/cleanup_firehose_raw.py --execute

    # Apenas listar órfãos
    python scripts/cleanup_firehose_raw.py --list-orphans

    # Especificar retenção
    python scripts/cleanup_firehose_raw.py --days 30 --execute
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

sys.path.insert(0, ".")

from core.cloud_config import load_cloud_config

app = typer.Typer()

console = Console()


def _create_client():
    from supabase import create_client

    config = load_cloud_config()
    if not config.is_configured:
        raise RuntimeError("Supabase não configurado")
    return create_client(config.project_url, config.service_role_key), config


def get_manifest_entries(
    status: Optional[str] = None,
    older_than_days: Optional[int] = None,
    limit: int = 1000,
) -> list[dict]:
    """Busca entries do manifest com filtros."""
    client, config = _create_client()

    query = client.table(config.firehose_raw_manifest_table).select("*")

    if status:
        query = query.eq("status", status)

    if older_than_days is not None:
        cutoff = datetime.now(timezone.utc) - timedelta(days=older_than_days)
        query = query.lt("uploaded_at", cutoff.isoformat())

    query = query.order("uploaded_at").limit(limit)
    result = query.execute()

    return result.data or []


def get_storage_files(prefix: str = "") -> list[str]:
    """Lista arquivos no firehose-raw bucket."""
    client, config = _create_client()

    try:
        files = client.storage.from_(config.firehose_raw_bucket).list(path=prefix)
        # Flatten any folders
        all_paths = []
        for item in files:
            if isinstance(item, dict):
                if item.get("id"):  # It's a file
                    all_paths.append(item.get("name", ""))
                elif item.get("name"):  # Could be folder, recurse
                    sub_prefix = f"{prefix}/{item['name']}" if prefix else item["name"]
                    all_paths.extend(get_storage_files(sub_prefix))
            elif hasattr(item, "name"):
                all_paths.append(item.name)
        return [p for p in all_paths if p]  # Filter empty
    except Exception as e:
        console.print(f"[red]Erro ao listar storage: {e}[/red]")
        return []


def find_orphaned_storage_files(
    manifest_paths: set[str], storage_paths: list[str]
) -> list[str]:
    """Encontra arquivos no storage sem correspondência no manifest.

    Considera apenas arquivos .ndjson.
    """
    manifest_ndjson = {p for p in manifest_paths if p.endswith(".ndjson")}
    storage_ndjson = {p for p in storage_paths if p.endswith(".ndjson")}

    # Extract base paths (remove any folder prefix logic if needed)
    # For simplicity, compare full paths
    orphaned = []
    for sp in storage_ndjson:
        # Check if this exact path or with any prefix is in manifest
        if sp not in manifest_ndjson:
            orphaned.append(sp)

    return orphaned


def delete_storage_file(object_path: str) -> bool:
    """Remove arquivo do storage."""
    client, config = _create_client()
    try:
        client.storage.from_(config.firehose_raw_bucket).remove(object_path)
        return True
    except Exception as e:
        console.print(f"[red]Erro ao remover {object_path}: {e}[/red]")
        return False


def delete_manifest_entries(ids: list[int]) -> int:
    """Remove entries do manifest pelo ID. Retorna quantos foram removidos."""
    if not ids:
        return 0

    client, config = _create_client()
    deleted = 0
    # Supabase delete em batches
    for id_val in ids:
        try:
            client.table(config.firehose_raw_manifest_table).delete().eq(
                "id", id_val
            ).execute()
            deleted += 1
        except Exception as e:
            console.print(f"[yellow]Erro ao deletar id {id_val}: {e}[/yellow]")
    return deleted


@app.command()
def main(
    execute: bool = typer.Option(
        False,
        "--execute",
        help="Executar cleanup (default: dry-run)",
    ),
    list_orphans: bool = typer.Option(
        False,
        "--list-orphans",
        help="Apenas listar órfãos do storage",
    ),
    days: int = typer.Option(
        30,
        "--days",
        help="Idade em dias para cleanup de manifest (default: 30)",
    ),
    status: Optional[str] = typer.Option(
        None,
        "--status",
        help="Filtrar por status no manifest (pending, uploaded, failed)",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        help="Output detalhado",
    ),
) -> None:
    """Cleanup/governança para firehose_raw_manifest e storage.

    Opera em modo dry-run por padrão. Use --execute para aplicar.
    """
    config = load_cloud_config()

    if not config.is_configured:
        console.print("[red]Supabase não configurado[/red]")
        raise typer.Exit(code=1)

    console.print(f"\n[bold cyan]Firehose Raw Cleanup[/bold cyan]")
    console.print(f"[cyan]Modo: {'EXECUTAR' if execute else 'DRY-RUN'}[/cyan]\n")

    # 1. Buscar entries do manifest
    console.print(
        f"[cyan]Buscando entries do manifest (status={status or 'any'}, >{days} dias)...[/cyan]"
    )
    manifest_entries = get_manifest_entries(status=status, older_than_days=days)

    if verbose:
        console.print(f"[cyan]{len(manifest_entries)} entries encontradas[/cyan]")

    # 2. Analisar entries
    total_size = 0
    entries_by_status = {}
    old_entry_ids = []

    for entry in manifest_entries:
        status_val = entry.get("status", "unknown")
        entries_by_status[status_val] = entries_by_status.get(status_val, 0) + 1
        total_size += entry.get("file_size_bytes", 0) or 0

        # Collect IDs para possível exclusão
        if execute:
            old_entry_ids.append(entry["id"])

    # 3. Mostrar tabela de entries antigas
    if manifest_entries:
        table = Table(
            title=f"Manifest entries >{days} dias ({len(manifest_entries)} total)"
        )
        table.add_column("ID", style="cyan")
        table.add_column("run_id", style="white")
        table.add_column("object_path", style="white")
        table.add_column("status", style="yellow")
        table.add_column("size_bytes", style="dim")
        table.add_column("uploaded_at", style="dim")

        for entry in manifest_entries[:50]:  # Limita a 50 na tabela
            table.add_row(
                str(entry.get("id", "")),
                str(entry.get("run_id", ""))[:20],
                str(entry.get("object_path", ""))[:40],
                str(entry.get("status", "")),
                str(entry.get("file_size_bytes", 0) or 0),
                str(entry.get("uploaded_at", ""))[:19],
            )

        console.print(table)

        if len(manifest_entries) > 50:
            console.print(f"[dim]... e mais {len(manifest_entries) - 50} entries[/dim]")

    # 4. Resumo de sizes
    console.print(f"\n[cyan]Resumo:[/cyan]")
    console.print(f"  Total entries: {len(manifest_entries)}")
    for s, c in entries_by_status.items():
        console.print(f"    {s}: {c}")
    console.print(f"  Size total: {total_size / 1024 / 1024:.2f} MB")

    # 5. Executar cleanup do manifest se --execute
    if execute and old_entry_ids:
        console.print(
            f"\n[yellow]Removendo {len(old_entry_ids)} entries do manifest...[/yellow]"
        )
        deleted = delete_manifest_entries(old_entry_ids)
        console.print(f"[green]Removidas {deleted} entries do manifest[/green]")

    # 6. Listar órfãos do storage se --list-orphans ou --execute
    if list_orphans or execute:
        console.print(f"\n[cyan]Buscando arquivos órfãos no storage...[/cyan]")

        # Pega todos os paths do manifest
        all_manifest_paths = set()
        all_entries = get_manifest_entries(limit=10000)
        for entry in all_entries:
            all_manifest_paths.add(entry.get("object_path", ""))

        # Lista arquivos no storage
        storage_files = get_storage_files()

        # Encontra órfãos
        orphaned = find_orphaned_storage_files(all_manifest_paths, storage_files)

        if orphaned:
            console.print(
                f"[yellow]{len(orphaned)} arquivo(s) órfão(s) encontrado(s)[/yellow]"
            )
            if verbose:
                for i, o in enumerate(orphaned[:20]):
                    console.print(f"  {o}")
                if len(orphaned) > 20:
                    console.print(f"  ... e mais {len(orphaned) - 20}")

            if execute:
                console.print(
                    f"[yellow]Removendo {len(orphaned)} arquivos órfãos...[/yellow]"
                )
                removed = 0
                for obj_path in orphaned:
                    if delete_storage_file(obj_path):
                        removed += 1
                console.print(f"[green]Removidos {removed} arquivos órfãos[/green]")
        else:
            console.print("[green]Nenhum arquivo órfão encontrado[/green]")

    # 7. Resumo final
    console.print()
    if execute:
        console.print("[bold green]✓ Cleanup executado[/bold green]")
    else:
        console.print("[bold yellow]⚠ Dry-run - nada foi modificado[/bold yellow]")
        console.print("[dim]Use --execute para aplicar[/dim]")


if __name__ == "__main__":
    typer.run(main)
