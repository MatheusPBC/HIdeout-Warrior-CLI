import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import typer
from rich import print

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.market_intelligence import build_market_segments

app = typer.Typer(help="Build offline market intelligence snapshots")


def _utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


@app.callback(invoke_without_command=True)
def build_market_intelligence(
    gold_path: Path = typer.Option(
        Path("data/snapshots/latest/gold.parquet"),
        "--gold-path",
        help="Path to the Gold parquet snapshot.",
    ),
    output: Path = typer.Option(
        Path("data/market_intelligence/latest.json"),
        "--output",
        help="Path to write the market intelligence JSON snapshot.",
    ),
    risk_profile: str = typer.Option(
        "balanced",
        "--risk-profile",
        help="Risk profile used for scoring.",
    ),
    league: str | None = typer.Option(None, "--league", help="Only include one league."),
    top: int = typer.Option(20, "--top", help="Number of top segments to include."),
) -> None:
    if not gold_path.exists():
        raise typer.BadParameter(f"Gold snapshot not found: {gold_path}")

    frame = pd.read_parquet(gold_path)
    if league:
        frame = frame[frame["league"].astype(str) == league]
    segments = build_market_segments(frame)
    payload = {
        "generated_at": _utc_now_iso(),
        "risk_profile": risk_profile,
        "league": league,
        "source": str(gold_path),
        "segments": [segment.to_dict() for segment in segments],
        "top_segments": [segment.to_dict() for segment in segments[:top]],
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    print(f"[green]Market intelligence snapshot written:[/green] {output}")


if __name__ == "__main__":
    app()
