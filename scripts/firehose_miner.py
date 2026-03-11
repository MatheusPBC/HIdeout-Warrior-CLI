import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import requests
import typer
from rich import print

from core.ops_metrics import append_metric_event

PUBLIC_STASH_URL = "https://www.pathofexile.com/api/public-stash-tabs"

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
    conn.commit()


def load_checkpoint(conn: sqlite3.Connection) -> Optional[str]:
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
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            wait_seconds = min(2 * attempt, 10)
            print(
                f"[yellow][retry {attempt}/{max_retries}] falha ao buscar stash page: {exc}. aguardando {wait_seconds}s[/yellow]"
            )
            time.sleep(wait_seconds)
    return None


def ingest_stash_page(
    conn: sqlite3.Connection,
    payload: Dict[str, Any],
    change_id: str,
) -> Tuple[int, int]:
    inserted = 0
    duplicates = 0

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
                        raw_item_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
    session = requests.Session()
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

            response_payload = fetch_stash_page(session, current_change_id)
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
                conn, response_payload, effective_change_id
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
