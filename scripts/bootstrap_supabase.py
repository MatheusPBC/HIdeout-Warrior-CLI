#!/usr/bin/env python3
"""Bootstrap e verificação de infraestrutura Supabase cloud.

Verifica (e cria se necessário):
- Buckets de storage (hideout-warrior-data, firehose-raw)
- Schema das tabelas (artifact_catalog, active_models, etc.)
- Configuração básica de RLS/policies
- Conectividade básica

Uso:
    python scripts/bootstrap_supabase.py [--create] [--dry-run]
"""

from __future__ import annotations

import sys
from enum import Enum
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

sys.path.insert(0, ".")

from core.cloud_config import SupabaseCloudConfig, load_cloud_config

app = typer.Typer()


class ExitCode(Enum):
    OK = 0
    NOT_CONFIGURED = 1
    BUCKET_MISSING = 2
    SCHEMA_INVALID = 3
    CHECK_FAILED = 4


console = Console()


def _create_client(config: SupabaseCloudConfig):
    if not config.is_configured:
        raise RuntimeError("Supabase não configurado")
    from supabase import create_client

    return create_client(config.project_url, config.service_role_key)


def check_bucket_exists(client, bucket_name: str) -> bool:
    """Verifica se bucket existe."""
    try:
        result = client.storage.list_buckets()
        bucket_names = [b.name for b in result]
        return bucket_name in bucket_names
    except Exception as e:
        console.print(f"[red]Erro ao listar buckets: {e}[/red]")
        return False


def create_bucket(client, bucket_name: str, public: bool = False) -> bool:
    """Cria bucket se não existir."""
    try:
        client.storage.create_bucket(bucket_name, options={"public": public})
        console.print(f"[green]Bucket '{bucket_name}' criado[/green]")
        return True
    except Exception as e:
        console.print(f"[yellow]Bucket '{bucket_name}' já existe ou erro: {e}[/yellow]")
        return False


def verify_schema(client, config: SupabaseCloudConfig) -> dict[str, bool]:
    """Verifica se as tabelas esperadas existem."""
    expected_tables = [
        config.artifact_catalog_table,
        config.active_models_table,
        config.snapshot_runs_table,
        config.firehose_checkpoint_table,
        config.firehose_raw_manifest_table,
    ]

    results = {}
    for table in expected_tables:
        try:
            client.table(table).select("*").limit(1).execute()
            results[table] = True
        except Exception as e:
            console.print(f"[red]Tabela '{table}' não encontrada: {e}[/red]")
            results[table] = False

    return results


def run_schema_sql(client, config: SupabaseCloudConfig) -> bool:
    """Executa schema.sql básico para garantir tabelas.

    Nota: Em produção cloud, o schema deve ser aplicado via migrations.
    Este método é apenas para verificação/leitura do estado atual.
    """
    schema_path = "supabase/schema.sql"
    try:
        with open(schema_path) as f:
            sql_content = f.read()
        console.print(
            f"[cyan]Schema SQL encontrado em {schema_path} - aplicar manualmente em produção[/cyan]"
        )
        return True
    except FileNotFoundError:
        console.print(f"[yellow]Schema SQL não encontrado em {schema_path}[/yellow]")
        return False


def check_storage_connectivity(client, config: SupabaseCloudConfig) -> bool:
    """Testa conectividade básica com storage."""
    try:
        result = client.storage.list_buckets()
        console.print(
            f"[green]Storage conectivo - {len(result)} bucket(s) encontrado(s)[/green]"
        )
        return True
    except Exception as e:
        console.print(f"[red]Erro de conectividade com storage: {e}[/red]")
        return False


def check_db_connectivity(client) -> bool:
    """Testa conectividade básica com DB."""
    try:
        client.table("artifact_catalog").select("*").limit(1).execute()
        console.print("[green]DB conectivo[/green]")
        return True
    except Exception as e:
        console.print(f"[red]Erro de conectividade com DB: {e}[/red]")
        return False


def bootstrap(
    create_buckets: bool = False,
    dry_run: bool = False,
    config: Optional[SupabaseCloudConfig] = None,
) -> dict[str, bool]:
    """Executa bootstrap/check completo.

    Args:
        create_buckets: Se True, tenta criar buckets ausentes.
        dry_run: Se True, apenas verifica sem criar.
        config: Configuração do Supabase.

    Returns:
        Dict com resultados das verificações.
    """
    cfg = config or load_cloud_config()

    if not cfg.is_configured:
        console.print(
            "[red]Supabase não configurado. Defina SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY.[/red]"
        )
        return {"configured": False}

    results = {"configured": True}

    try:
        client = _create_client(cfg)
    except Exception as e:
        console.print(f"[red]Falha ao criar cliente Supabase: {e}[/red]")
        return {"configured": False, "client_error": str(e)}

    # 1. Conectividade
    results["storage_connectivity"] = check_storage_connectivity(client, cfg)
    results["db_connectivity"] = check_db_connectivity(client)

    if not results["storage_connectivity"]:
        return results

    # 2. Buckets
    buckets_to_check = [cfg.storage_bucket, cfg.firehose_raw_bucket]
    results["buckets"] = {}

    for bucket in buckets_to_check:
        exists = check_bucket_exists(client, bucket)
        results["buckets"][bucket] = {"exists": exists}

        if not exists and create_buckets and not dry_run:
            create_bucket(client, bucket)
            # Verifica novamente
            results["buckets"][bucket]["exists"] = check_bucket_exists(client, bucket)
        elif not exists and not dry_run:
            console.print(
                f"[yellow]Bucket '{bucket}' não existe (use --create para criar)[/yellow]"
            )

    # 3. Schema
    results["schema"] = verify_schema(client, cfg)

    # 4. Schema SQL info
    results["schema_sql_available"] = run_schema_sql(client, cfg)

    return results


def print_report(results: dict[str, bool]) -> bool:
    """Imprime relatório formatado e retorna True se tudo ok."""
    table = Table(title="Supabase Bootstrap Report")
    table.add_column("Check", style="cyan")
    table.add_column("Status", style="white")

    all_ok = True

    def add_row(key: str, value: any, is_ok: bool = True):
        nonlocal all_ok
        status = "[green]✓[/green]" if value else "[red]✗[/red]"
        if not value:
            all_ok = False
        table.add_row(key, status)

    if not results.get("configured", False):
        console.print("[red]Supabase não configurado[/red]")
        return False

    add_row("Storage Conectividade", results.get("storage_connectivity", False))
    add_row("DB Conectividade", results.get("db_connectivity", False))

    buckets = results.get("buckets", {})
    for bucket_name, bucket_result in buckets.items():
        add_row(f"Bucket: {bucket_name}", bucket_result.get("exists", False))

    schema = results.get("schema", {})
    for table_name, exists in schema.items():
        add_row(f"Tabela: {table_name}", exists)

    console.print(table)
    return all_ok


@app.command()
def main(
    create: bool = typer.Option(
        False,
        "--create",
        help="Criar buckets ausentes",
    ),
    dry_run: bool = typer.Option(
        True,
        "--dry-run/--no-dry-run",
        help="Apenas verificar, não modificar (default: True)",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        help="Output detalhado",
    ),
) -> None:
    """Bootstrap e verificação de infraestrutura Supabase cloud."""
    config = load_cloud_config()

    if verbose:
        console.print(f"[cyan]Project URL: {config.project_url}[/cyan]")
        console.print(f"[cyan]Storage Bucket: {config.storage_bucket}[/cyan]")
        console.print(f"[cyan]Firehose Raw Bucket: {config.firehose_raw_bucket}[/cyan]")

    results = bootstrap(
        create_buckets=create,
        dry_run=dry_run,
        config=config,
    )

    all_ok = print_report(results)

    if all_ok:
        console.print("\n[bold green]✓ Bootstrap OK[/bold green]")
        raise typer.Exit(code=ExitCode.OK.value)
    else:
        console.print("\n[bold red]✗ Bootstrap com problemas[/bold red]")
        raise typer.Exit(code=ExitCode.CHECK_FAILED.value)


if __name__ == "__main__":
    typer.run(main)
