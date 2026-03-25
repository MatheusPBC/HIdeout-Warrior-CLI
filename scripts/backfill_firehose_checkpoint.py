#!/usr/bin/env python3
"""Backfill one-shot: sincroniza checkpoint atual do SQLite para Supabase.

Uso:
    python -m scripts.backfill_firehose_checkpoint [--db-path data/firehose.db]

Não é necessário rodar frequentemente; uma única vez para migrar estado existente.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.cloud_config import load_cloud_config
from core.supabase_cloud import (
    load_checkpoint_from_supabase,
    sync_firehose_checkpoint_to_supabase,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backfill firehose checkpoint SQLite → Supabase"
    )
    parser.add_argument(
        "--db-path",
        default="data/firehose.db",
        help="Caminho para SQLite do firehose (default: data/firehose.db)",
    )
    args = parser.parse_args()

    config = load_cloud_config()
    if not config.is_configured:
        print(
            "[yellow]Supabase não configurado; nada a fazer (HW_CLOUD_BACKEND=supabase + SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY necessários)[/yellow]"
        )
        return 0

    db_path = Path(args.db_path)
    if not db_path.exists():
        print(f"[red]Banco SQLite não encontrado: {db_path}[/red]")
        return 1

    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute(
            "SELECT next_change_id, pages_processed, events_ingested, duplicates_skipped FROM miner_checkpoint WHERE id = 1"
        ).fetchone()
        if not row:
            print("[yellow]Nenhum checkpoint encontrado no SQLite local[/yellow]")
            return 0

        next_change_id, pages_processed, events_ingested, duplicates_skipped = row
        print(
            f"Checkpoint local encontrado: next_change_id={next_change_id} "
            f"pages={pages_processed} events={events_ingested} dup={duplicates_skipped}"
        )

        # Verificar se Supabase já tem checkpoint mais recente
        cloud_cp = load_checkpoint_from_supabase(config=config)
        if cloud_cp and cloud_cp.get("pages_processed", 0) >= pages_processed:
            print(
                f"[yellow]Supabase já tem checkpoint mais recente (pages={cloud_cp['pages_processed']}); "
                f"skipping backfill[/yellow]"
            )
            return 0

        success = sync_firehose_checkpoint_to_supabase(
            next_change_id=str(next_change_id or ""),
            pages_processed=int(pages_processed or 0),
            events_ingested=int(events_ingested or 0),
            duplicates_skipped=int(duplicates_skipped or 0),
            config=config,
        )
        if success:
            print("[green]Checkpoint sincronizado para Supabase com sucesso[/green]")
            return 0
        else:
            print("[red]Falha ao sincronizar checkpoint para Supabase[/red]")
            return 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
