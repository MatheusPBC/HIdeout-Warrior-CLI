import csv
import json
import time
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional
import typer
from rich.console import Console
from rich.panel import Panel
from rich.live import Live
from rich.table import Table
from rich.align import Align
from rich.text import Text
from rich.spinner import Spinner

from core.api_integrator import MarketAPIClient
from core.data_parser import RePoeParser
from core.evaluator import CraftingEvaluator
from core.recombinators import RecombinatorEngine
from core.graph_engine import CraftingGraphEngine, ItemState, CraftingAction
from core.clipboard_watcher import ClipboardScanner
from core.ml_oracle import PricePredictor, CraftingHeuristic
from core.market_scanner import OnDemandScanner

app = typer.Typer(
    help="Hideout Warrior - Path of Exile Trade & Deterministic Crafting CLI",
    add_completion=False,
)

console = Console()

SCAN_BASE_COLUMNS = [
    "base_type",
    "ilvl",
    "listed_price",
    "ml_value",
    "profit",
    "whisper",
    "trade_link",
]

SCAN_EXTRA_COLUMNS = [
    "item_id",
    "listing_currency",
    "listing_amount",
    "seller",
    "indexed_at",
    "corrupted",
    "fractured",
    "influences",
    "explicit_mods",
    "implicit_mods",
]


def _truncate_text(value: str, max_len: int) -> str:
    if len(value) <= max_len:
        return value
    return value[: max_len - 3] + "..."


def _to_display_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def _json_safe_value(value: Any) -> Any:
    if isinstance(value, (list, dict, str, int, float, bool)) or value is None:
        return value
    return str(value)


def _normalize_scan_result(result: Dict[str, Any]) -> Dict[str, Any]:
    normalized = {
        key: _json_safe_value(result.get(key))
        for key in SCAN_BASE_COLUMNS + SCAN_EXTRA_COLUMNS
    }
    for key, value in result.items():
        if key not in normalized:
            normalized[key] = _json_safe_value(value)
    return normalized


def _scan_results_to_csv(results: List[Dict[str, Any]]) -> str:
    if not results:
        return ""

    normalized = [_normalize_scan_result(item) for item in results]
    preferred = [
        key for key in SCAN_BASE_COLUMNS + SCAN_EXTRA_COLUMNS if key in normalized[0]
    ]
    extras = sorted({key for row in normalized for key in row.keys()} - set(preferred))
    fieldnames = preferred + extras

    buffer = StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()

    for row in normalized:
        serialized_row: Dict[str, Any] = {}
        for key in fieldnames:
            value = row.get(key)
            if isinstance(value, (list, dict)):
                serialized_row[key] = json.dumps(value, ensure_ascii=False)
            elif value is None:
                serialized_row[key] = ""
            else:
                serialized_row[key] = value
        writer.writerow(serialized_row)

    return buffer.getvalue()


def _scan_results_to_json(
    results: List[Dict[str, Any]], indent: Optional[int] = 2
) -> str:
    normalized = [_normalize_scan_result(item) for item in results]
    return json.dumps(normalized, indent=indent, ensure_ascii=False)


def _scan_results_to_jsonl(results: List[Dict[str, Any]]) -> str:
    normalized = [_normalize_scan_result(item) for item in results]
    return "\n".join(json.dumps(item, ensure_ascii=False) for item in normalized)


def _render_scan_table(
    results: List[Dict[str, Any]], full: bool = False, title: str = ""
) -> Table:
    table = Table(title=title, expand=True)
    table.add_column("Item Base", style="cyan")
    table.add_column("iLvl", justify="right", style="white")
    table.add_column("Preço (Chaos)", justify="right", style="red")
    table.add_column("Valor ML (Chaos)", justify="right", style="magenta")
    table.add_column("Lucro (Chaos)", justify="right", style="bold")

    if full:
        table.add_column("Seller", style="white")
        table.add_column("Item ID", style="dim")
        table.add_column("Indexed At", style="dim")
        table.add_column("Corrupted", style="yellow", justify="center")
        table.add_column("Fractured", style="yellow", justify="center")
        table.add_column("Influences", style="white")
        table.add_column("Explicit Mods", style="white")
        table.add_column("Implicit Mods", style="white")

    table.add_column("Comando Whisper", style="dim white")
    table.add_column("Link Trade", style="blue")

    for result in results:
        profit = float(result.get("profit") or 0.0)
        profit_style = (
            "bold green" if profit > 50.0 else "yellow" if profit > 0 else "white"
        )

        whisper = _to_display_text(result.get("whisper"))
        trade_link = _to_display_text(result.get("trade_link"))

        if not full:
            whisper = _truncate_text(whisper, 80)
            trade_link = _truncate_text(trade_link, 50)

        row = [
            _to_display_text(result.get("base_type") or "Unknown Base"),
            _to_display_text(result.get("ilvl") or "-"),
            f"{float(result.get('listed_price') or 0.0):.1f}",
            f"{float(result.get('ml_value') or 0.0):.1f}",
            f"[{profit_style}]{profit:.1f}[/]",
        ]

        if full:
            row.extend(
                [
                    _to_display_text(result.get("seller")),
                    _to_display_text(result.get("item_id")),
                    _to_display_text(result.get("indexed_at")),
                    _to_display_text(result.get("corrupted")),
                    _to_display_text(result.get("fractured")),
                    _to_display_text(result.get("influences")),
                    _to_display_text(result.get("explicit_mods")),
                    _to_display_text(result.get("implicit_mods")),
                ]
            )

        row.extend([whisper, trade_link])
        table.add_row(*row)

    return table


def _save_scan_output(output_path: str, payload: str) -> None:
    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(payload, encoding="utf-8")


class HideoutDashboard:
    def __init__(self, target_mods: list, max_budget: float):
        self.target_mods = target_mods
        self.max_budget = max_budget
        self.console = console

        # Engine Boot
        self.market = MarketAPIClient()
        self.parser = RePoeParser()
        self.evaluator = CraftingEvaluator(self.parser)
        self.recombinators = RecombinatorEngine()
        self.predictor = PricePredictor()
        self.heuristic = CraftingHeuristic()
        self.graph_engine = CraftingGraphEngine(
            self.market,
            self.evaluator,
            self.recombinators,
            self.predictor,
            self.heuristic,
        )

        # UI State
        self.current_item: Optional[ItemState] = None
        self.calculating = False
        self.result_path: Optional[List[Any]] = None
        self.result_cost = 0.0

    def generate_layout(self):
        """Desenha a UI Reativa Baseada no Estado Atual."""
        if self.calculating:
            spin = Spinner(
                "dots",
                text=Text(
                    "Oráculo calculando rotas no A* Optimization Engine...",
                    style="cyan",
                ),
            )
            return Panel(
                spin,
                title="[yellow]A-Star Pathfinding Active[/]",
                border_style="yellow",
            )

        if self.result_path is not None:
            return self._generate_results_table()

        # Tela de Padrão Aguardando Ação
        msg = Text.assemble(
            ("⚔️ Hideout Warrior v1.0\n\n", "bold bright_white"),
            ("Aguardando Ctrl+C no Path of Exile...\n", "dim"),
            (
                f"Orçamento Máximo: {self.max_budget}c | Alvos: {', '.join(self.target_mods)}",
                "bold blue",
            ),
        )
        return Panel(Align.center(msg), border_style="bold black", padding=(2, 4))

    def _generate_results_table(self):
        """Desenha a tabela de Ações de Craft do A*."""
        if self.current_item is None:
            return Panel("Sem item carregado.", border_style="red")

        actions = self.result_path or []
        table = Table(
            title=f"Rota Ótima A* - {self.current_item.base_type}",
            expand=True,
            title_style="bold magenta",
        )
        table.add_column("Passo", style="cyan", justify="center")
        table.add_column("Ação de Craft", style="white")
        table.add_column("Chance", justify="right", style="green")
        table.add_column("Custo EV Acumulado (c)", justify="right", style="yellow")

        for i, action in enumerate(actions, 1):
            # Em um cenario ideal, GraphEngine retorna List[CraftingAction].
            # Como retornava strings no esqueleto, adaptamos para display:
            if isinstance(action, CraftingAction):
                table.add_row(
                    str(i),
                    action.action_name,
                    f"{(action.probability * 100):.2f}%",
                    f"{action.ev_cost:.1f}",
                )
            else:
                table.add_row(str(i), str(action), "N/A", "N/A")

        summary_color = "green" if self.result_cost < self.max_budget else "red"

        panel_group = Table.grid(padding=1)
        panel_group.add_row(table)
        panel_group.add_row(
            Text(
                f"\nCusto EV Estimado Total: {self.result_cost:.1f} chaos",
                style=f"bold {summary_color}",
            )
        )

        # ROI é complexo sem bater no Ninja para o preço do alvo final, omitindo para fluidez local.
        if self.result_cost == float("inf"):
            msg = Text(
                "❌ Nenhuma rota plausível encontrada dentro do orçamento!",
                style="bold red",
            )
            return Panel(Align.center(msg), border_style="red")

        return Panel(
            panel_group, border_style="green", title="[bold green]Path Found[/]"
        )

    def on_item_copied(self, item: ItemState):
        """Callback do Daemon de Clipboard."""
        self.current_item = item
        self.calculating = True
        self.result_path = None
        self.result_cost = 0.0

        # Simulando uma leve trava para a UI brilhar no spinner
        time.sleep(0.5)

        # Fire Engine
        try:
            res = self.graph_engine.find_cheapest_route(
                item, self.target_mods, self.max_budget
            )
            if res:
                self.result_path, self.result_cost = res
            else:
                self.result_path = []  # Empty path trigger
                self.result_cost = float("inf")
        except Exception as e:
            self.result_path = [f"System Error: {str(e)}"]
            self.result_cost = float("inf")

        self.calculating = False


@app.command()
def craft_path(
    budget: float = typer.Option(5000.0, help="Orçamento máximo em Chaos"),
    targets: str = typer.Option(
        "maximum_life_1,movement_speed_1", help="Mods alvos separados por vírgula"
    ),
):
    """
    Roda o Dashboard HUD (Fase 4). Conecta o A* Pathfinding direto à área de transferência do sistema.
    """
    target_mod_list = [m.strip() for m in targets.split(",")]

    dashboard = HideoutDashboard(target_mod_list, budget)
    scanner = ClipboardScanner(callback=dashboard.on_item_copied)

    # Render Loop com Rich Live
    try:
        scanner.start()
        with Live(
            dashboard.generate_layout(), refresh_per_second=4, screen=False
        ) as live:
            while True:
                live.update(dashboard.generate_layout())
                time.sleep(0.2)
    except KeyboardInterrupt:
        scanner.stop()
        console.print("[dim]Encerrando Hideout Warrior...[/dim]")


@app.command()
def scan(
    league: str = typer.Option(
        "Standard", "--league", "-l", help="Liga do PoE (ex: Settlers)"
    ),
    type: str = typer.Option("", help="Nome base do item (ex: 'Imbued Wand')"),
    ilvl: int = typer.Option(1, help="Item Level mínimo"),
    rarity: str = typer.Option(
        "rare", help="Raridade do item (ex: rare, unique, normal)"
    ),
    max_items: int = typer.Option(
        30, help="Quantidade máxima de itens para avaliar na paginação"
    ),
    min_profit: float = typer.Option(
        0.0, "--min-profit", help="Lucro mínimo em chaos para exibir resultados"
    ),
    anti_fix: bool = typer.Option(
        True,
        "--anti-fix/--no-anti-fix",
        help="Filtra listagens provavelmente fake (stale + preço anômalo)",
    ),
    stale_hours: float = typer.Option(
        48.0,
        "--stale-hours",
        help="Horas para considerar listagem antiga no filtro anti-fix",
    ),
    format: str = typer.Option(
        "table", "--format", help="Formato de saída: table, json, csv, jsonl"
    ),
    output: Optional[str] = typer.Option(
        None, "--output", help="Caminho opcional para salvar o resultado"
    ),
    full: bool = typer.Option(False, "--full", help="Tabela detalhada sem truncamento"),
):
    """
    (Fase 7) Scanner de Arbitragem Sob Demanda. Interroga a API da GGG e o ML Oracle
    em busca de lucros subvalorizados no mercado.
    """
    from rich.status import Status

    scanner = OnDemandScanner(league=league)

    with Status(
        "[bold cyan]Buscando itens na API da GGG e avaliando rentabilidade via XGBoost...[/]",
        spinner="dots",
    ) as status:
        results = scanner.run_scan(
            item_class=type,
            ilvl_min=ilvl,
            rarity=rarity,
            max_items=max_items,
            min_profit=min_profit,
            anti_fix=anti_fix,
            stale_hours=stale_hours,
        )

    if not results:
        console.print(
            "[yellow]Nenhuma listagem encontrada com os filtros atuais "
            f"(min-profit: {min_profit:.1f}c).[/yellow]"
        )
        return

    output_format = format.lower().strip()
    supported_formats = {"table", "json", "csv", "jsonl"}
    if output_format not in supported_formats:
        raise typer.BadParameter(
            "Formato inválido. Use: table, json, csv ou jsonl.", param_hint="--format"
        )

    title = f"🤑 Oportunidades de Arbitragem (Total: {len(results)})"

    if output_format == "table":
        console.print(_render_scan_table(results, full=full, title=title))
        return

    if output_format == "json":
        payload = _scan_results_to_json(results, indent=2)
    elif output_format == "jsonl":
        payload = _scan_results_to_jsonl(results)
    else:
        payload = _scan_results_to_csv(results)

    if output:
        _save_scan_output(output, payload)
        console.print(f"[green]Resultado salvo em:[/] {output}")

    if output_format == "json":
        console.print_json(payload)
        return

    console.print(
        _render_scan_table(
            results,
            full=full,
            title=f"{title} | Visualização em tabela ({output_format.upper()})",
        )
    )


# --- Legacy Commands Retidos por Compatibilidade ---


@app.command()
def meta_sync():
    """Sincroniza economia (Módulo C)."""
    console.print("Rode as rotinas do poe.ninja aqui.")


@app.command()
def rescue_snipe(budget: float = typer.Option(..., help="Orçamento em Chaos")):
    """Módulo A de arbitragem."""
    console.print("Iniciando varredura na API de Trade da GGG.")


if __name__ == "__main__":
    app()
