#!/usr/bin/env python3
"""Consome landing buffer NDJSON e faz upload para Supabase Storage.

Arquivos em data/firehose_raw/{date}/{change_id}.ndjson são enviados para
o bucket 'firehose-raw' no Supabase Storage e removidos localmente após
upload confirmado.

Uso:
    python -m scripts.firehose_to_supabase              # processo normal
    python -m scripts.firehose_to_supabase --dry-run    # lista arquivos sem fazer upload
    python -m scripts.firehose_to_supabase --keep      # não remove arquivos locais após upload
    python -m scripts.firehose_to_supabase --max-age 7  # ignora arquivos > 7 dias
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.cloud_config import SupabaseCloudConfig, load_cloud_config
from core.supabase_cloud import (
    _create_supabase_client,
    _file_sha256,
    upsert_firehose_raw_manifest,
)

logger = logging.getLogger(__name__)

FIREHOSE_RAW_DIR = Path("data/firehose_raw")


def _ndjson_records(file_path: Path) -> list[dict]:
    records = []
    with file_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                logger.warning("linha inválida JSON em %s: %s", file_path, line[:100])
    return records


def _upload_ndjson(
    file_path: Path,
    config: SupabaseCloudConfig,
    client: Any,
) -> dict[str, Any] | None:
    """Faz upload de um arquivo NDJSON para Supabase Storage.

    Returns dict com {object_path, file_size_bytes, content_sha256} ou None se falhar.
    O caminho no Storage é: {date}/{change_id}.ndjson (sem storage_prefix).
    """
    if not file_path.exists() or not file_path.is_file():
        return None

    # Caminho direto no bucket: {date}/{change_id}.ndjson
    relative = file_path.relative_to(FIREHOSE_RAW_DIR)
    object_path = str(relative).replace("\\", "/")

    sha256 = _file_sha256(file_path)
    size_bytes = file_path.stat().st_size

    with file_path.open("rb") as stream:
        client.storage.from_(config.firehose_raw_bucket).upload(
            path=object_path,
            file=stream,
            file_options={
                "cache-control": "3600",
                "upsert": "true",
                "content-type": "application/x-ndjson",
            },
        )

    return {
        "object_path": object_path,
        "file_size_bytes": size_bytes,
        "content_sha256": sha256,
    }


def process_firehose_raw(
    config: SupabaseCloudConfig,
    dry_run: bool = False,
    keep_files: bool = False,
    max_age_days: int | None = None,
) -> dict[str, int]:
    """Processa landing buffer e faz upload para Supabase Storage.

    Returns dict com 'uploaded', 'failed', 'skipped', 'deleted'.
    """
    if not config.is_configured:
        logger.error("Supabase não configurado")
        return {"uploaded": 0, "failed": 0, "skipped": 0, "deleted": 0}

    if not FIREHOSE_RAW_DIR.exists():
        logger.info("Landing buffer vazio ou inexistente: %s", FIREHOSE_RAW_DIR)
        return {"uploaded": 0, "failed": 0, "skipped": 0, "deleted": 0}

    client = _create_supabase_client(config)
    cutoff_time = None
    if max_age_days is not None:
        cutoff_time = datetime.now(timezone.utc) - timedelta(days=max_age_days)

    stats = {"uploaded": 0, "failed": 0, "skipped": 0, "deleted": 0}

    for ndjson_file in sorted(FIREHOSE_RAW_DIR.rglob("*.ndjson")):
        # Verificar idade máxima se configurada
        if cutoff_time is not None:
            file_mtime = datetime.fromtimestamp(
                ndjson_file.stat().st_mtime, tz=timezone.utc
            )
            if file_mtime < cutoff_time:
                logger.debug("pulando arquivo antigo: %s", ndjson_file)
                stats["skipped"] += 1
                continue

        records = _ndjson_records(ndjson_file)
        if not records:
            logger.warning("arquivo vazio ou só com linhas inválidas: %s", ndjson_file)
            if not keep_files:
                try:
                    ndjson_file.unlink()
                    stats["deleted"] += 1
                except OSError as exc:
                    logger.warning(
                        "falha ao deletar arquivo vazio %s: %s", ndjson_file, exc
                    )
            continue

        change_id = ndjson_file.stem  # nome sem .ndjson
        date_folder = ndjson_file.parent.name  # YYYY-MM-DD
        total_items = sum(r.get("items_count", 0) for r in records)
        # first record's change_id = page_start; last record's change_id = page_end
        page_start_change_id = records[0].get("change_id", "") if records else ""
        page_end_change_id = records[-1].get("change_id", "") if records else ""

        if dry_run:
            logger.info(
                "[DRY-RUN] faria upload: %s (%d records, %d items, date=%s)",
                ndjson_file,
                len(records),
                total_items,
                date_folder,
            )
            stats["uploaded"] += 1
            continue

        try:
            upload_meta = _upload_ndjson(ndjson_file, config, client)
            if upload_meta:
                # Upload succeeded and SHA256 was computed before upload
                # content_sha256 is stored for later verification
                manifest_ok = upsert_firehose_raw_manifest(
                    run_id=date_folder,
                    object_path=upload_meta["object_path"],
                    rows_count=total_items,
                    page_start_change_id=page_start_change_id,
                    page_end_change_id=page_end_change_id,
                    file_size_bytes=upload_meta["file_size_bytes"],
                    content_sha256=upload_meta["content_sha256"],
                    status="uploaded",
                    error_message=None,
                    config=config,
                    client=client,
                )
                if not manifest_ok:
                    logger.warning(
                        "upload ok mas registro no manifest falhou para %s",
                        ndjson_file,
                    )
                logger.info(
                    "uploaded: %s (%d records, %d items) manifest=%s",
                    ndjson_file,
                    len(records),
                    total_items,
                    "ok" if manifest_ok else "FAILED",
                )
                stats["uploaded"] += 1
                if not keep_files:
                    try:
                        ndjson_file.unlink()
                        stats["deleted"] += 1
                    except OSError as exc:
                        logger.warning(
                            "falha ao deletar %s após upload: %s", ndjson_file, exc
                        )
            else:
                stats["failed"] += 1
        except Exception as exc:
            # Tentar registrar falha no manifest para rastreabilidade
            try:
                failed_object_path = str(
                    ndjson_file.relative_to(FIREHOSE_RAW_DIR)
                ).replace("\\", "/")
                upsert_firehose_raw_manifest(
                    run_id=date_folder,
                    object_path=failed_object_path,
                    rows_count=total_items,
                    page_start_change_id=page_start_change_id,
                    page_end_change_id=page_end_change_id,
                    file_size_bytes=ndjson_file.stat().st_size,
                    content_sha256="",
                    status="failed",
                    error_message=str(exc)[:500],
                    config=config,
                    client=client,
                )
            except Exception:
                pass  # manifest falhou, não deixar obscured a exceção original
            logger.error("falha ao processar %s: %s", ndjson_file, exc)
            stats["failed"] += 1

    return stats


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description="Upload firehose NDJSON landing buffer para Supabase Storage"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Lista arquivos que seriam processados sem fazer upload",
    )
    parser.add_argument(
        "--keep",
        action="store_true",
        help="Mantém arquivos locais após upload (default: remove após sucesso)",
    )
    parser.add_argument(
        "--max-age",
        type=int,
        metavar="DAYS",
        help="Ignora arquivos mais antigos que DAYS dias",
    )
    args = parser.parse_args()

    config = load_cloud_config()
    if not config.is_configured:
        logger.error(
            "Supabase não configurado. Defina HW_CLOUD_BACKEND=supabase, "
            "SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY."
        )
        return 1

    if args.dry_run:
        logger.info("Modo dry-run: nenhum arquivo será modificado")

    stats = process_firehose_raw(
        config,
        dry_run=args.dry_run,
        keep_files=args.keep,
        max_age_days=args.max_age,
    )

    logger.info(
        "Resultado: uploaded=%d failed=%d skipped=%d deleted=%d",
        stats["uploaded"],
        stats["failed"],
        stats["skipped"],
        stats["deleted"],
    )

    if stats["failed"] > 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
