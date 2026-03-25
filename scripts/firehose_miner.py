import os
import sys
import json
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import requests
import typer
from rich import print

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.ops_metrics import append_metric_event
from core.poe_oauth import (
    DEFAULT_POE_TOKEN_URL,
    DEFAULT_SERVICE_SCOPE,
    resolve_service_oauth_token,
)
import logging

from core.supabase_cloud import (
    load_checkpoint_from_supabase,
    sync_firehose_checkpoint_to_supabase,
)

logger = logging.getLogger(__name__)


def _clean_optional_str(
    value: Optional[str], default: Optional[str] = None
) -> Optional[str]:
    if value is None or isinstance(value, typer.models.OptionInfo):
        return default
    cleaned = str(value).strip()
    return cleaned or default


def _utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _ensure_column(
    conn: sqlite3.Connection,
    table_name: str,
    column_name: str,
    column_definition: str,
) -> None:
    existing_columns = {
        str(row[1])
        for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    }
    if column_name in existing_columns:
        return
    conn.execute(
        f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_definition}"
    )


PUBLIC_STASH_URL = "https://api.pathofexile.com/public-stash-tabs"
DEFAULT_USER_AGENT = (
    "OAuth hideout-warrior-cli/1.0.0 (contact: hideout-warrior-cli@local)"
)

CURRENCY_ALIASES = {
    "chaos": "chaos",
    "c": "chaos",
    "chaosorb": "chaos",
    "chaosorbs": "chaos",
    "divine": "divine",
    "div": "divine",
    "divineorb": "divine",
    "divineorbs": "divine",
    "exalted": "exalted",
    "exa": "exalted",
    "ex": "exalted",
    "exalt": "exalted",
    "exaltedorb": "exalted",
    "alchemy": "alchemy",
    "alch": "alchemy",
    "a": "alchemy",
    "orbofalchemy": "alchemy",
}

RARITY_BY_FRAME_TYPE = {
    2: "rare",
    3: "unique",
}


def parse_price_note(note: str) -> Tuple[Optional[float], Optional[str]]:
    if not note:
        return (None, None)
    lowered = note.strip().lower()
    if not (lowered.startswith("~b/o") or lowered.startswith("~price")):
        return (None, None)

    payload = lowered.replace("~b/o", "", 1).replace("~price", "", 1).strip()
    if not payload:
        return (None, None)

    tokens = payload.replace("/", " ").split()
    if not tokens:
        return (None, None)

    amount = None
    currency = None
    for token in tokens:
        if amount is None:
            try:
                amount = float(token)
                continue
            except ValueError:
                pass

        normalized = "".join(ch for ch in token if ch.isalpha())
        if not normalized:
            continue
        mapped = CURRENCY_ALIASES.get(normalized)
        if mapped:
            currency = mapped
            break

    if amount is None or amount <= 0 or currency is None:
        return (None, None)
    return (float(amount), currency)


def to_chaos_value(amount: float, currency: str) -> Optional[float]:
    rates = {
        "chaos": 1.0,
        "alchemy": 0.25,
        "exalted": 60.0,
        "divine": 125.0,
    }
    rate = rates.get(currency)
    if rate is None or amount <= 0:
        return None
    return round(float(amount) * rate, 2)


def is_useful_item(
    item: Dict[str, Any],
) -> Tuple[bool, Optional[float], Optional[str], Optional[float]]:
    rarity = RARITY_BY_FRAME_TYPE.get(int(item.get("frameType", -1)))
    if rarity not in {"rare", "unique"}:
        return (False, None, None, None)

    amount, currency = parse_price_note(str(item.get("note", "")))
    if amount is None or currency is None:
        return (False, None, None, None)

    chaos_value = to_chaos_value(amount, currency)
    if chaos_value is None or chaos_value <= 0:
        return (False, None, None, None)
    return (True, amount, currency, chaos_value)


def initialize_database(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS stash_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            change_id TEXT NOT NULL,
            item_id TEXT NOT NULL,
            rarity TEXT NOT NULL,
            base_type TEXT,
            item_name TEXT,
            item_level INTEGER,
            league TEXT,
            account_name TEXT,
            stash_name TEXT,
            indexed TEXT,
            price_amount REAL NOT NULL,
            price_currency TEXT NOT NULL,
            price_chaos REAL NOT NULL,
            raw_item_json TEXT NOT NULL,
            collected_at TEXT,
            oauth_source TEXT,
            oauth_scope TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(change_id, item_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS miner_checkpoint (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            next_change_id TEXT,
            pages_processed INTEGER NOT NULL DEFAULT 0,
            events_ingested INTEGER NOT NULL DEFAULT 0,
            duplicates_skipped INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    _ensure_column(conn, "stash_events", "collected_at", "TEXT")
    _ensure_column(conn, "stash_events", "oauth_source", "TEXT")
    _ensure_column(conn, "stash_events", "oauth_scope", "TEXT")
    conn.commit()


def load_checkpoint(conn: sqlite3.Connection) -> Optional[str]:
    """Carrega checkpoint. Preferência: Supabase (se configurado) → SQLite local."""
    from core.cloud_config import load_cloud_config

    config = load_cloud_config()
    if config.is_configured:
        cloud_checkpoint = load_checkpoint_from_supabase(config=config)
        if cloud_checkpoint and cloud_checkpoint.get("next_change_id"):
            logger.info(
                "checkpoint carregado do Supabase: %s",
                cloud_checkpoint["next_change_id"],
            )
            return cloud_checkpoint["next_change_id"]
        logger.warning(
            "Supabase configurado mas checkpoint não encontrado, usando SQLite local"
        )

    row = conn.execute(
        "SELECT next_change_id FROM miner_checkpoint WHERE id = 1"
    ).fetchone()
    if not row:
        return None
    return row[0]


def update_checkpoint(
    conn: sqlite3.Connection,
    next_change_id: str,
    pages_delta: int,
    ingested_delta: int,
    duplicates_delta: int,
) -> None:
    conn.execute(
        """
        INSERT INTO miner_checkpoint (
            id,
            next_change_id,
            pages_processed,
            events_ingested,
            duplicates_skipped,
            updated_at
        )
        VALUES (1, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(id)
        DO UPDATE SET
            next_change_id=excluded.next_change_id,
            pages_processed=miner_checkpoint.pages_processed + excluded.pages_processed,
            events_ingested=miner_checkpoint.events_ingested + excluded.events_ingested,
            duplicates_skipped=miner_checkpoint.duplicates_skipped + excluded.duplicates_skipped,
            updated_at=CURRENT_TIMESTAMP
        """,
        (next_change_id, pages_delta, ingested_delta, duplicates_delta),
    )
    conn.commit()
    try:
        row = conn.execute(
            "SELECT next_change_id, pages_processed, events_ingested, duplicates_skipped FROM miner_checkpoint WHERE id = 1"
        ).fetchone()
        if row:
            success = sync_firehose_checkpoint_to_supabase(
                next_change_id=str(row[0] or ""),
                pages_processed=int(row[1] or 0),
                events_ingested=int(row[2] or 0),
                duplicates_skipped=int(row[3] or 0),
            )
            if not success:
                logger.warning(
                    "sync checkpoint para Supabase retornou False (cloud não configurado ou indisponível)"
                )
    except Exception as exc:
        logger.warning("falha ao sincronizar checkpoint para Supabase: %s", exc)


def fetch_stash_page(
    session: requests.Session,
    next_change_id: Optional[str],
    timeout_seconds: float = 20.0,
    max_retries: int = 3,
) -> Optional[Dict[str, Any]]:
    params = {}
    if next_change_id:
        params["id"] = next_change_id

    for attempt in range(1, max_retries + 1):
        try:
            response = session.get(
                PUBLIC_STASH_URL, params=params, timeout=timeout_seconds
            )
            if response.status_code == 401:
                try:
                    error_payload = response.json().get("error", {})
                except (ValueError, AttributeError):
                    error_payload = {}
                message = str(
                    error_payload.get(
                        "message",
                        "Unauthorized; token missing/invalid for public stash endpoint",
                    )
                )
                raise PermissionError(message)
            if response.status_code == 403:
                try:
                    error_payload = response.json().get("error", {})
                except (ValueError, AttributeError):
                    error_payload = {}
                if int(error_payload.get("code", -1)) == 6:
                    message = str(
                        error_payload.get(
                            "message",
                            "Forbidden; OAuth token required for public stash endpoint",
                        )
                    )
                    raise PermissionError(message)
            response.raise_for_status()
            return response.json()
        except PermissionError:
            raise
        except (requests.RequestException, ValueError) as exc:
            wait_seconds = min(2 * attempt, 10)
            print(
                f"[yellow][retry {attempt}/{max_retries}] falha ao buscar stash page: {exc}. aguardando {wait_seconds}s[/yellow]"
            )
            time.sleep(wait_seconds)
    return None


def _build_session(
    oauth_token: Optional[str],
    user_agent: str,
) -> requests.Session:
    session = requests.Session()
    headers = {
        "User-Agent": user_agent,
        "Accept": "application/json",
    }
    if oauth_token:
        headers["Authorization"] = f"Bearer {oauth_token}"
    session.headers.update(headers)
    return session


FIREHOSE_RAW_DIR = Path("data/firehose_raw")


def _write_ndjson_landing(
    payload: Dict[str, Any],
    change_id: str,
    collected_at: str,
) -> None:
    """Escreve payload bruto do firehose em NDJSON para landing buffer local.

    Arquivo: data/firehose_raw/{date}/{change_id}.ndjson
    Cada linha é um JSON com metadata + items.
    """
    try:
        date_str = collected_at[:10]  # YYYY-MM-DD
        landing_dir = FIREHOSE_RAW_DIR / date_str
        landing_dir.mkdir(parents=True, exist_ok=True)
        file_path = landing_dir / f"{change_id}.ndjson"
        record = {
            "change_id": change_id,
            "collected_at": collected_at,
            "next_change_id": payload.get("next_change_id", ""),
            "stashes_count": len(payload.get("stashes", []) or []),
            "items_count": sum(
                len(s.get("items", []) or []) for s in payload.get("stashes", []) or []
            ),
            "raw_payload": payload,
        }
        with file_path.open("w", encoding="utf-8") as f:
            f.write(
                json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
            )
    except Exception as exc:
        logger.warning(
            "falha ao escrever NDJSON landing para change_id=%s: %s", change_id, exc
        )


def ingest_stash_page(
    conn: sqlite3.Connection,
    payload: Dict[str, Any],
    change_id: str,
    *,
    collected_at: Optional[str] = None,
    oauth_source: Optional[str] = None,
    oauth_scope: Optional[str] = None,
) -> Tuple[int, int]:
    inserted = 0
    duplicates = 0
    effective_collected_at = collected_at or _utc_now_iso()
    effective_oauth_source = str(oauth_source or "")
    effective_oauth_scope = str(oauth_scope or "")

    # NDJSON landing buffer
    _write_ndjson_landing(payload, change_id, effective_collected_at)

    stashes = payload.get("stashes", [])
    with conn:
        for stash in stashes:
            stash_name = stash.get("stash", "")
            league = stash.get("league", "")
            account_name = stash.get("accountName", "")
            for item in stash.get("items", []) or []:
                is_useful, amount, currency, price_chaos = is_useful_item(item)
                if (
                    not is_useful
                    or amount is None
                    or currency is None
                    or price_chaos is None
                ):
                    continue

                item_id = item.get("id")
                if not item_id:
                    continue

                rarity = RARITY_BY_FRAME_TYPE.get(
                    int(item.get("frameType", -1)), "unknown"
                )
                cur = conn.execute(
                    """
                    INSERT OR IGNORE INTO stash_events (
                        change_id,
                        item_id,
                        rarity,
                        base_type,
                        item_name,
                        item_level,
                        league,
                        account_name,
                        stash_name,
                        indexed,
                        price_amount,
                        price_currency,
                        price_chaos,
                        raw_item_json,
                        collected_at,
                        oauth_source,
                        oauth_scope
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        change_id,
                        item_id,
                        rarity,
                        item.get("baseType", ""),
                        item.get("name", ""),
                        int(item.get("ilvl", 0) or 0),
                        league,
                        account_name,
                        stash_name,
                        item.get("indexed", ""),
                        float(amount),
                        currency,
                        float(price_chaos),
                        json.dumps(item, ensure_ascii=True, separators=(",", ":")),
                        effective_collected_at,
                        effective_oauth_source,
                        effective_oauth_scope,
                    ),
                )
                if cur.rowcount == 1:
                    inserted += 1
                else:
                    duplicates += 1

    return inserted, duplicates


app = typer.Typer(help="Public stash firehose miner")


@app.command()
def run(
    db_path: str = typer.Option("data/firehose.db", "--db-path", help="SQLite path"),
    start_change_id: Optional[str] = typer.Option(
        None, "--start-change-id", help="Optional start id"
    ),
    max_pages: int = typer.Option(
        0, "--max-pages", help="Max pages to fetch (0 = unlimited)"
    ),
    sleep_seconds: float = typer.Option(
        1.5, "--sleep-seconds", help="Delay between pages"
    ),
    oauth_token: Optional[str] = typer.Option(
        None,
        "--oauth-token",
        help="OAuth bearer token (fallback: env POE_OAUTH_TOKEN)",
    ),
    oauth_client_id: Optional[str] = typer.Option(
        None,
        "--oauth-client-id",
        help="OAuth client id (fallback: env POE_OAUTH_CLIENT_ID/POE_CLIENT_ID)",
    ),
    oauth_client_secret: Optional[str] = typer.Option(
        None,
        "--oauth-client-secret",
        help="OAuth client secret (fallback: env POE_OAUTH_CLIENT_SECRET/POE_CLIENT_SECRET)",
    ),
    oauth_scope: str = typer.Option(
        DEFAULT_SERVICE_SCOPE,
        "--oauth-scope",
        help="OAuth scope for token resolution",
    ),
    oauth_token_url: str = typer.Option(
        DEFAULT_POE_TOKEN_URL,
        "--oauth-token-url",
        help="OAuth token endpoint",
    ),
    user_agent: str = typer.Option(
        DEFAULT_USER_AGENT,
        "--user-agent",
        help="Identifiable User-Agent required by PoE API policy",
    ),
) -> None:
    run_id = str(int(time.time() * 1000))
    db_file = Path(db_path)
    db_file.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_file))
    initialize_database(conn)

    checkpoint_id = load_checkpoint(conn)
    current_change_id = start_change_id or checkpoint_id

    print(
        f"[cyan]firehose miner iniciado[/cyan] db={db_path} start={current_change_id}"
    )
    effective_oauth_scope = _clean_optional_str(oauth_scope, DEFAULT_SERVICE_SCOPE)
    effective_oauth_token_url = _clean_optional_str(
        oauth_token_url,
        DEFAULT_POE_TOKEN_URL,
    )
    effective_user_agent = _clean_optional_str(user_agent, DEFAULT_USER_AGENT)
    if effective_oauth_scope is None:
        effective_oauth_scope = DEFAULT_SERVICE_SCOPE
    if effective_oauth_token_url is None:
        effective_oauth_token_url = DEFAULT_POE_TOKEN_URL
    if effective_user_agent is None:
        effective_user_agent = DEFAULT_USER_AGENT
    try:
        resolved_oauth = resolve_service_oauth_token(
            access_token=oauth_token,
            client_id=oauth_client_id,
            client_secret=oauth_client_secret,
            scope=effective_oauth_scope,
            token_url=effective_oauth_token_url,
            user_agent=effective_user_agent,
        )
    except ValueError as exc:
        print(f"[red]{exc}[/red]")
        raise typer.Exit(code=2) from exc
    except requests.RequestException as exc:
        print(f"[red]Falha ao obter token OAuth: {exc}[/red]")
        raise typer.Exit(code=2) from exc
    except RuntimeError as exc:
        print(f"[red]{exc}[/red]")
        raise typer.Exit(code=2) from exc

    effective_oauth_token = resolved_oauth.access_token if resolved_oauth else None
    if resolved_oauth is not None:
        print(
            "[cyan]OAuth resolvido[/cyan] "
            f"source={resolved_oauth.source} scope={resolved_oauth.scope or effective_oauth_scope}"
        )

    session = _build_session(effective_oauth_token, user_agent=effective_user_agent)
    total_inserted = 0
    total_duplicates = 0
    fetch_failures = 0
    pages_processed = 0
    started_at = time.time()
    status = "ok"

    try:
        while True:
            if max_pages > 0 and pages_processed >= max_pages:
                break

            try:
                response_payload = fetch_stash_page(session, current_change_id)
            except PermissionError as exc:
                print(
                    "[red]Acesso negado/sem autorização no endpoint public stash. "
                    "Use OAuth token válido com escopo service:psapi.[/red]"
                )
                print(
                    "[yellow]Verifique: 1) token Bearer válido em --oauth-token/POE_OAUTH_TOKEN "
                    "ou client credentials em --oauth-client-id/--oauth-client-secret; "
                    "2) escopo service:psapi; 3) uso do host api.pathofexile.com. "
                    "Docs: https://www.pathofexile.com/developer/docs/authorization[/yellow]"
                )
                raise typer.Exit(code=2) from exc
            if response_payload is None:
                fetch_failures += 1
                print("[red]falha ao obter página; continuando loop[/red]")
                time.sleep(max(sleep_seconds, 1.0))
                continue

            next_change_id = str(response_payload.get("next_change_id", "") or "")
            if not next_change_id:
                print(
                    "[yellow]payload sem next_change_id; aguardando próximo ciclo[/yellow]"
                )
                time.sleep(max(sleep_seconds, 1.0))
                continue

            effective_change_id = current_change_id or "__bootstrap__"
            inserted, duplicates = ingest_stash_page(
                conn,
                response_payload,
                effective_change_id,
                collected_at=_utc_now_iso(),
                oauth_source=(resolved_oauth.source if resolved_oauth else None),
                oauth_scope=(
                    resolved_oauth.scope if resolved_oauth else effective_oauth_scope
                ),
            )

            # checkpoint apenas depois do lote persistido com sucesso.
            update_checkpoint(
                conn,
                next_change_id=next_change_id,
                pages_delta=1,
                ingested_delta=inserted,
                duplicates_delta=duplicates,
            )

            pages_processed += 1
            total_inserted += inserted
            total_duplicates += duplicates
            current_change_id = next_change_id

            elapsed = max(time.time() - started_at, 0.001)
            throughput = total_inserted / elapsed
            print(
                f"[green]page={pages_processed} inserted={inserted} dup={duplicates} total={total_inserted} throughput={throughput:.2f} items/s[/green]"
            )
            time.sleep(max(sleep_seconds, 0.0))
    except Exception:
        status = "error"
        raise
    finally:
        conn.close()
        session.close()

    elapsed = max(time.time() - started_at, 0.001)
    throughput = total_inserted / elapsed
    try:
        append_metric_event(
            component="firehose_miner.run",
            run_id=run_id,
            duration_ms=elapsed * 1000,
            status="error" if fetch_failures > 0 or status == "error" else "ok",
            error_count=fetch_failures,
            payload={
                "pages_processed": pages_processed,
                "events_ingested": total_inserted,
                "duplicates_skipped": total_duplicates,
                "throughput_items_per_sec": round(throughput, 4),
                "db_path": db_path,
                "max_pages": max_pages,
            },
        )
    except Exception:
        pass
    print(
        f"[bold cyan]finalizado[/bold cyan] pages={pages_processed} inserted={total_inserted} duplicates={total_duplicates} elapsed={elapsed:.1f}s"
    )


if __name__ == "__main__":
    app()
