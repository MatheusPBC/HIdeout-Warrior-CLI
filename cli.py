import csv
import io
import json
import logging
import time
import typer
from rich.console import Console
from rich.panel import Panel
from rich.live import Live
from rich.table import Table
from rich.align import Align
from rich.text import Text
from rich.spinner import Spinner

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)

logger = logging.getLogger(__name__)

from core.api_integrator import MarketAPIClient
from core.data_parser import RePoeParser
from core.evaluator import CraftingEvaluator
from core.recombinators import RecombinatorEngine
from core.graph_engine import CraftingGraphEngine, ItemState, CraftingAction
from core.clipboard_watcher import ClipboardScanner
from core.ml_oracle import PricePredictor, CraftingHeuristic
from core.market_scanner import OnDemandScanner, ScanStats

app = typer.Typer(
    help="Hideout Warrior - Path of Exile Trade & Deterministic Crafting CLI",
    add_completion=False,
)

console = Console()


def _scan_results_to_json(results):
    return json.dumps(results, indent=2, ensure_ascii=False)


def _scan_results_to_csv(results):
    if not results:
        return ""

    output = io.StringIO()
    fieldnames = list(results[0].keys())
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for row in results:
        writer.writerow(row)
    return output.getvalue()


def _scan_results_to_jsonl(results):
    return "\n".join(json.dumps(r, ensure_ascii=False) for r in results)


def _save_scan_output(path: str, payload: str):
    with open(path, "w", encoding="utf-8") as f:
        f.write(payload)


def _render_scan_table(results, full=False, title=""):
    if not results:
        return Table(title=title or "Sem Resultados")

    table = Table(
        title=title or f"Oportunidades de Arbitragem (Total: {len(results)})",
        expand=True,
        show_lines=full,
    )

    table.add_column("Item Base", style="cyan")
    table.add_column("Preço (Chaos)", justify="right", style="red")
    table.add_column("Valor ML (Chaos)", justify="right", style="magenta")
    table.add_column("Lucro (Chaos)", justify="right", style="bold")
    table.add_column("Comando Whisper", style="dim white")

    full_columns = []
    if full:
        present_keys = set()
        for result in results:
            present_keys.update(result.keys())

        candidate_columns = [
            ("seller", "Seller", lambda v: v or "N/A"),
            ("item_id", "Item ID", lambda v: v or "N/A"),
            ("indexed_at", "Indexed At", lambda v: v or "N/A"),
            (
                "ml_confidence",
                "ML Confidence",
                lambda v: f"{float(v):.2f}" if v is not None else "N/A",
            ),
            ("listing_currency", "Listing Currency", lambda v: v or "N/A"),
            (
                "listing_amount",
                "Listing Amount",
                lambda v: f"{float(v):.1f}" if v is not None else "N/A",
            ),
        ]

        for key, label, formatter in candidate_columns:
            if key in present_keys:
                table.add_column(label, style="dim")
                full_columns.append((key, formatter))

    for r in results:
        profit = r.get("profit", 0)
        profit_style = (
            "bold green" if profit > 50.0 else "yellow" if profit > 0 else "white"
        )

        whisper = r.get("whisper", "")
        whisper_display = whisper[:40] + "..." if len(whisper) > 40 else whisper

        row = [
            r.get("base_type", "Unknown"),
            f"{r.get('listed_price', 0):.1f}",
            f"{r.get('ml_value', 0):.1f}",
            f"[{profit_style}]{profit:.1f}[/]",
            whisper_display,
        ]

        if full:
            for key, formatter in full_columns:
                row.append(formatter(r.get(key)))

        table.add_row(*row)

    return table


def _render_kpi_panel(stats):
    table = Table(
        title="[bold]Resumo do Scan[/bold]",
        expand=True,
        show_header=False,
        box=None,
    )

    table.add_column("Métrica", style="cyan", justify="left")
    table.add_column("Valor", style="white", justify="right")

    table.add_row("Total Encontrado", f"[bold]{stats.total_found}[/bold]")
    table.add_row("Total Avaliados", f"[bold]{stats.total_evaluated}[/bold]")
    table.add_row(
        "Descartados (Anti-Fix)", f"[yellow]{stats.filtered_anti_fix}[/yellow]"
    )
    table.add_row(
        "Descartados (Moeda Inválida)",
        f"[yellow]{stats.skipped_invalid_currency}[/yellow]",
    )
    if stats.filtered_safe_buy_confidence > 0:
        table.add_row(
            "Descartados (Safe-Buy Confiança)",
            f"[yellow]{stats.filtered_safe_buy_confidence}[/yellow]",
        )
    if stats.filtered_safe_buy_age > 0:
        table.add_row(
            "Descartados (Safe-Buy Idade)",
            f"[yellow]{stats.filtered_safe_buy_age}[/yellow]",
        )
    if stats.filtered_safe_buy_price > 0:
        table.add_row(
            "Descartados (Safe-Buy Preço)",
            f"[yellow]{stats.filtered_safe_buy_price}[/yellow]",
        )
    table.add_row("Lucro Médio", f"[green]{stats.avg_profit:.1f}c[/green]")
    table.add_row("Lucro Máximo", f"[bold green]{stats.max_profit:.1f}c[/bold green]")

    panel = Panel(
        table,
        title="[bold cyan]📊 KPI do Scan[/bold cyan]",
        border_style="cyan",
        padding=(1, 2),
    )
    console.print(panel)


def _render_no_results_message(
    min_profit: float, anti_fix: bool, safe_buy: bool, stats
):
    if not isinstance(stats, ScanStats):
        stats = ScanStats()

    console.print(
        Panel(
            Text(
                "Nenhuma listagem com buyout foi encontrada para esses filtros na liga atual.",
                style="yellow",
            ),
            title="[bold red]Sem Resultados[/bold red]",
            border_style="red",
        )
    )

    filter_reasons = []
    if min_profit > 0:
        filter_reasons.append(f"- min-profit={min_profit}c pode estar muito alto")
    if anti_fix:
        filter_reasons.append(
            f"- anti-fix ativado descartou {stats.filtered_anti_fix} itens"
        )
    if safe_buy:
        filter_reasons.append(
            f"- safe-buy ativado (confiança>=0.7, idade<=24h, preço>=5c) pode ser muito restritivo"
        )
    if stats.skipped_invalid_currency > 0:
        filter_reasons.append(
            f"- {stats.skipped_invalid_currency} itens com conversão de moeda inválida"
        )

    if filter_reasons:
        console.print("\n[dim]Possíveis causas do filtro:[/dim]")
        for reason in filter_reasons:
            console.print(f"  [dim]{reason}[/dim]")


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
        self.current_item: ItemState = None
        self.calculating = False
        self.result_path = None
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
        table = Table(
            title=f"Rota Ótima A* - {self.current_item.base_type}",
            expand=True,
            title_style="bold magenta",
        )
        table.add_column("Passo", style="cyan", justify="center")
        table.add_column("Ação de Craft", style="white")
        table.add_column("Chance", justify="right", style="green")
        table.add_column("Custo EV Acumulado (c)", justify="right", style="yellow")

        for i, action in enumerate(self.result_path, 1):
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
    item_type: str = typer.Option(
        "", "--type", help="Nome base do item (ex: 'Imbued Wand')"
    ),
    ilvl: int = typer.Option(1, help="Item Level mínimo"),
    rarity: str = typer.Option(
        "rare", help="Raridade do item (ex: rare, unique, normal)"
    ),
    max_items: int = typer.Option(
        30, min=1, help="Quantidade máxima de itens para avaliar na paginação"
    ),
    stale_hours: float = typer.Option(
        48.0, min=0, help="Filtrar listings com mais de N horas"
    ),
    league: str = typer.Option("Standard", "-l", help="Liga do Path of Exile"),
    min_profit: float = typer.Option(
        0.0, "--min-profit", help="Lucro mínimo em Chaos para filtrar"
    ),
    anti_fix: bool = typer.Option(
        True, "--anti-fix/--no-anti-fix", help="Ativar filtro anti-price-fixing"
    ),
    safe_buy: bool = typer.Option(
        False,
        "--safe-buy/--no-safe-buy",
        help="Modo conservador: apenas confiança >= 0.7, idade <= 24h, preço >= 5c",
    ),
    output: str = typer.Option("", "--output", "-o", help="Salvar saída em arquivo"),
    full: bool = typer.Option(
        False, "--full", help="Tabela detalhada com colunas extras"
    ),
    output_format: str = typer.Option(
        "table", "--format", help="Formato de saída: table|json|csv|jsonl"
    ),
):
    """
    (Fase 7) Scanner de Arbitragem Sob Demanda. Interroga a API da GGG e o ML Oracle
    em busca de lucros subvalorizados no mercado.
    """
    from rich.status import Status

    valid_formats = ["table", "json", "csv", "jsonl"]
    if output_format not in valid_formats:
        raise typer.BadParameter(
            f"Formato inválido: '{output_format}'. Use: {', '.join(valid_formats)}"
        )

    scanner = OnDemandScanner(league=league)

    with Status(
        "[bold cyan]Buscando itens na API da GGG e avaliando rentabilidade via XGBoost...[/]",
        spinner="dots",
    ) as status:
        results, stats = scanner.run_scan(
            item_class=item_type,
            ilvl_min=ilvl,
            rarity=rarity,
            max_items=max_items,
            stale_hours=stale_hours,
            min_profit=min_profit,
            anti_fix=anti_fix,
            safe_buy=safe_buy,
        )

    if not results:
        _render_no_results_message(
            min_profit=min_profit,
            anti_fix=anti_fix,
            safe_buy=safe_buy,
            stats=stats,
        )
        return

    _render_kpi_panel(stats)

    if output_format == "json":
        payload = _scan_results_to_json(results)
    elif output_format == "csv":
        payload = _scan_results_to_csv(results)
    elif output_format == "jsonl":
        payload = _scan_results_to_jsonl(results)
    else:
        payload = None

    if payload:
        if output:
            _save_scan_output(output, payload)
            console.print(f"[green]Saída salva em: {output}[/green]")
        else:
            typer.echo(payload)
    else:
        table = _render_scan_table(results, full=full)
        console.print(table)


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
