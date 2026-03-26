#!/usr/bin/env python3
"""Health check simples para ambiente Supabase cloud.

Verifica:
- Conectividade REST API
- Conectividade DB (via query simples)
- Conectividade Storage
- Status dos últimos firehose checkpoints

Uso:
    python scripts/supabase_health_check.py [--verbose]
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
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


def check_api_health() -> dict:
    """Verifica health da REST API."""
    import httpx

    config = load_cloud_config()
    result = {"ok": False, "latency_ms": None, "error": None}

    if not config.is_configured:
        result["error"] = "não configurado"
        return result

    try:
        start = datetime.now(timezone.utc)
        response = httpx.get(
            f"{config.project_url}/rest/v1/{config.artifact_catalog_table}?select=artifact_key&limit=1",
            headers={
                "apikey": str(config.service_role_key),
                "Authorization": f"Bearer {config.service_role_key}",
            },
            timeout=10.0,
        )
        latency = (datetime.now(timezone.utc) - start).total_seconds() * 1000
        result["latency_ms"] = round(latency, 1)
        result["ok"] = response.status_code == 200
        if not result["ok"]:
            result["error"] = f"HTTP {response.status_code}"
    except Exception as e:
        result["error"] = str(e)

    return result


def check_db_health() -> dict:
    """Verifica conectividade e status do DB."""
    result = {"ok": False, "latency_ms": None, "error": None, "row_counts": {}}

    try:
        client, config = _create_client()
        start = datetime.now(timezone.utc)

        # Query simples para verificar DB
        response = (
            client.table(config.artifact_catalog_table).select("*").limit(1).execute()
        )
        latency = (datetime.now(timezone.utc) - start).total_seconds() * 1000
        result["latency_ms"] = round(latency, 1)
        result["ok"] = True

        # Conta registros nas tabelas principais
        tables = [
            config.artifact_catalog_table,
            config.active_models_table,
            config.snapshot_runs_table,
            config.firehose_checkpoint_table,
            config.firehose_raw_manifest_table,
        ]

        for table in tables:
            try:
                count_resp = (
                    client.table(table).select("*", count="exact").limit(0).execute()
                )
                result["row_counts"][table] = count_resp.count or 0
            except Exception:
                result["row_counts"][table] = -1

    except Exception as e:
        result["error"] = str(e)

    return result


def check_storage_health() -> dict:
    """Verifica conectividade do Storage."""
    result = {"ok": False, "buckets": [], "error": None}

    try:
        client, config = _create_client()
        buckets = client.storage.list_buckets()
        result["ok"] = True
        result["buckets"] = [b.name for b in buckets]

        # Verifica buckets específicos
        result["required_buckets"] = {
            config.storage_bucket: config.storage_bucket in result["buckets"],
            config.firehose_raw_bucket: config.firehose_raw_bucket in result["buckets"],
        }

    except Exception as e:
        result["error"] = str(e)

    return result


def check_firehose_status() -> dict:
    """Verifica status dos firehose checkpoints."""
    result = {"ok": False, "checkpoint": None, "error": None}

    try:
        client, config = _create_client()
        response = (
            client.table(config.firehose_checkpoint_table)
            .select("*")
            .eq("checkpoint_name", "default")
            .maybe_single()
            .execute()
        )

        if response and response.data:
            result["ok"] = True
            result["checkpoint"] = {
                "next_change_id": response.data.get("next_change_id", ""),
                "pages_processed": response.data.get("pages_processed", 0),
                "events_ingested": response.data.get("events_ingested", 0),
                "duplicates_skipped": response.data.get("duplicates_skipped", 0),
                "updated_at": response.data.get("updated_at", ""),
            }
        else:
            result["error"] = "checkpoint não encontrado"

    except Exception as e:
        result["error"] = str(e)

    return result


def check_firehose_raw_manifest_status() -> dict:
    """Verifica status do manifest de raw files."""
    result = {"ok": False, "stats": {}, "error": None}

    try:
        client, config = _create_client()

        # Conta por status
        for status in ["pending", "uploaded", "failed"]:
            try:
                count_resp = (
                    client.table(config.firehose_raw_manifest_table)
                    .select("*", count="exact")
                    .eq("status", status)
                    .limit(0)
                    .execute()
                )
                result["stats"][status] = count_resp.count or 0
            except Exception:
                result["stats"][status] = -1

        result["ok"] = True

    except Exception as e:
        result["error"] = str(e)

    return result


@app.command()
def main(
    verbose: bool = typer.Option(
        False,
        "--verbose",
        help="Output detalhado",
    ),
) -> None:
    """Health check do ambiente Supabase."""
    console.print("\n[bold cyan]Supabase Health Check[/bold cyan]\n")

    # API Health
    api_result = check_api_health()
    api_status = "[green]✓[/green]" if api_result["ok"] else "[red]✗[/red]"
    api_latency = f" ({api_result['latency_ms']}ms)" if api_result["latency_ms"] else ""
    console.print(f"REST API: {api_status}{api_latency}")
    if api_result["error"]:
        console.print(f"  [red]Erro: {api_result['error']}[/red]")

    # DB Health
    db_result = check_db_health()
    db_status = "[green]✓[/green]" if db_result["ok"] else "[red]✗[/red]"
    db_latency = f" ({db_result['latency_ms']}ms)" if db_result["latency_ms"] else ""
    console.print(f"Database: {db_status}{db_latency}")
    if db_result["error"]:
        console.print(f"  [red]Erro: {db_result['error']}[/red]")
    elif verbose and db_result["row_counts"]:
        console.print("  [cyan]Contagem de registros:[/cyan]")
        for table, count in db_result["row_counts"].items():
            console.print(f"    {table}: {count}")

    # Storage Health
    storage_result = check_storage_health()
    storage_status = "[green]✓[/green]" if storage_result["ok"] else "[red]✗[/red]"
    console.print(f"Storage: {storage_status}")
    if storage_result["error"]:
        console.print(f"  [red]Erro: {storage_result['error']}[/red]")
    elif verbose:
        console.print(f"  [cyan]Buckets: {', '.join(storage_result['buckets'])}[/cyan]")
        if "required_buckets" in storage_result:
            for bucket, exists in storage_result["required_buckets"].items():
                icon = "[green]✓[/green]" if exists else "[red]✗[/red]"
                console.print(f"    {icon} {bucket}")

    # Firehose Status
    fh_result = check_firehose_status()
    fh_status = "[green]✓[/green]" if fh_result["ok"] else "[yellow]?[/yellow]"
    console.print(f"Firehose Checkpoint: {fh_status}")
    if fh_result["checkpoint"]:
        cp = fh_result["checkpoint"]
        console.print(f"  [cyan]Change ID: {cp['next_change_id']}[/cyan]")
        console.print(
            f"  [cyan]Pages: {cp['pages_processed']} | Events: {cp['events_ingested']} | Dups: {cp['duplicates_skipped']}[/cyan]"
        )
        console.print(f"  [cyan]Updated: {cp['updated_at']}[/cyan]")

    # Firehose Raw Manifest Status
    manifest_result = check_firehose_raw_manifest_status()
    manifest_status = "[green]✓[/green]" if manifest_result["ok"] else "[red]✗[/red]"
    console.print(f"Firehose Manifest: {manifest_status}")
    if manifest_result["stats"]:
        total = sum(v for v in manifest_result["stats"].values() if v >= 0)
        console.print(
            f"  [cyan]Total: {total} | Uploaded: {manifest_result['stats'].get('uploaded', 0)} | Pending: {manifest_result['stats'].get('pending', 0)} | Failed: {manifest_result['stats'].get('failed', 0)}[/cyan]"
        )

    # Resumo final
    console.print()
    all_ok = api_result["ok"] and db_result["ok"] and storage_result["ok"]
    if all_ok:
        console.print("[bold green]✓ Health OK[/bold green]")
        raise typer.Exit(code=0)
    else:
        console.print("[bold yellow]⚠ Health com problemas[/bold yellow]")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    typer.run(main)
