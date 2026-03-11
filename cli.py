import csv
import io
import json
import logging
import time

import typer
from rich.align import Align
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.spinner import Spinner
from rich.table import Table
from rich.text import Text

from core.api_integrator import MarketAPIClient
from core.clipboard_watcher import ClipboardScanner
from core.data_parser import RePoeParser
from core.evaluator import CraftingEvaluator
from core.flip_planner import FlipAdvisor
from core.graph_engine import CraftingAction, CraftingGraphEngine, ItemState
from core.market_scanner import OnDemandScanner, ScanStats
from core.ml_oracle import CraftingHeuristic, PricePredictor
from core.recombinators import RecombinatorEngine

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)

logger = logging.getLogger(__name__)

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


def _flip_plans_to_json(plans):
    return json.dumps([plan.to_dict() for plan in plans], indent=2, ensure_ascii=False)


def _save_output(path: str, payload: str):
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
    table.add_column("Preço", justify="right", style="red")
    table.add_column("Valor ML", justify="right", style="magenta")
    table.add_column("Lucro", justify="right", style="bold")
    table.add_column("Trusted", justify="right", style="blue")
    table.add_column("Score", justify="right", style="green")
    table.add_column("Flags", style="dim")

    full_columns = []
    if full:
        candidate_columns = [
            ("seller", "Seller", lambda v: v or "N/A"),
            ("item_id", "Item ID", lambda v: v or "N/A"),
            ("indexed_at", "Indexed At", lambda v: v or "N/A"),
            ("resolved_league", "League", lambda v: v or "N/A"),
            (
                "ml_confidence",
                "ML Conf.",
                lambda v: f"{float(v):.2f}" if v is not None else "N/A",
            ),
            (
                "relative_discount",
                "Discount",
                lambda v: f"{float(v):.2f}x" if v is not None else "N/A",
            ),
        ]
        for key, label, formatter in candidate_columns:
            if key in results[0]:
                table.add_column(label, style="dim")
                full_columns.append((key, formatter))

    for result in results:
        profit = result.get("profit", 0.0)
        trusted_profit = result.get("trusted_profit", 0.0)
        profit_style = (
            "bold green" if profit > 50.0 else "yellow" if profit > 0 else "white"
        )
        flags = ", ".join(result.get("risk_flags", [])) or "clean"
        row = [
            result.get("base_type", "Unknown"),
            f"{result.get('listed_price', 0.0):.1f}c",
            f"{result.get('ml_value', 0.0):.1f}c",
            f"[{profit_style}]{profit:.1f}c[/]",
            f"{trusted_profit:.1f}c",
            f"{result.get('score', 0.0):.1f}",
            flags,
        ]
        if full:
            for key, formatter in full_columns:
                row.append(formatter(result.get(key)))
        table.add_row(*row)

    return table


def _render_kpi_panel(stats: ScanStats):
    table = Table(
        title="[bold]Resumo do Scan[/bold]", expand=True, show_header=False, box=None
    )
    table.add_column("Métrica", style="cyan", justify="left")
    table.add_column("Valor", style="white", justify="right")
    table.add_row("Liga Resolvida", f"[bold]{stats.resolved_league or 'N/A'}[/bold]")
    table.add_row("Perfil", f"[bold]{stats.scan_profile}[/bold]")
    table.add_row("Total Encontrado", f"[bold]{stats.total_found}[/bold]")
    table.add_row("Total Avaliados", f"[bold]{stats.total_evaluated}[/bold]")
    table.add_row("Descartados Anti-Fix", f"[yellow]{stats.filtered_anti_fix}[/yellow]")
    table.add_row(
        "Descartados Preço Mínimo",
        f"[yellow]{stats.filtered_min_listed_price}[/yellow]",
    )
    if stats.scan_profile == "open_market":
        table.add_row(
            "Filtro Open: confiança",
            f"[yellow]{stats.filtered_open_confidence}[/yellow]",
        )
        table.add_row(
            "Filtro Open: barato + confiança",
            f"[yellow]{stats.filtered_open_cheap_low_confidence}[/yellow]",
        )
        table.add_row(
            "Filtro Open: barato + lucro",
            f"[yellow]{stats.filtered_open_cheap_low_profit}[/yellow]",
        )
        table.add_row(
            "Filtro Open: barato + stale",
            f"[yellow]{stats.filtered_open_cheap_stale}[/yellow]",
        )
    table.add_row(
        "Moeda Inválida", f"[yellow]{stats.skipped_invalid_currency}[/yellow]"
    )
    table.add_row("Lucro Médio", f"[green]{stats.avg_profit:.1f}c[/green]")
    table.add_row("Lucro Máximo", f"[bold green]{stats.max_profit:.1f}c[/bold green]")
    table.add_row("Score Médio", f"[green]{stats.avg_score:.1f}[/green]")
    console.print(
        Panel(
            table,
            title="[bold cyan]Scanner KPI[/bold cyan]",
            border_style="cyan",
            padding=(1, 2),
        )
    )


def _render_no_results_message(
    min_profit: float,
    min_listed_price: float,
    anti_fix: bool,
    safe_buy: bool,
    stats,
):
    if not isinstance(stats, ScanStats):
        stats = ScanStats()

    console.print(
        Panel(
            Text(
                f"Nenhuma oportunidade encontrada na liga resolvida '{stats.resolved_league or 'N/A'}'.",
                style="yellow",
            ),
            title="[bold red]Sem Resultados[/bold red]",
            border_style="red",
        )
    )

    filter_reasons = []
    if min_profit > 0:
        filter_reasons.append(f"- min-profit={min_profit}c pode estar alto demais")
    if min_listed_price > 0:
        filter_reasons.append(
            f"- min-listed-price={min_listed_price}c filtrou {stats.filtered_min_listed_price} itens"
        )
    if anti_fix:
        filter_reasons.append(
            f"- anti-fix descartou {stats.filtered_anti_fix} itens suspeitos"
        )
    if stats.scan_profile == "open_market":
        if stats.filtered_open_confidence > 0:
            filter_reasons.append(
                f"- perfil aberto descartou {stats.filtered_open_confidence} itens por confiança muito baixa"
            )
        if stats.filtered_open_cheap_low_confidence > 0:
            filter_reasons.append(
                "- perfil aberto bloqueou itens muito baratos com confiança insuficiente"
            )
        if stats.filtered_open_cheap_low_profit > 0:
            filter_reasons.append(
                "- perfil aberto removeu itens baratos com lucro implícito fraco"
            )
        if stats.filtered_open_cheap_stale > 0:
            filter_reasons.append(
                "- perfil aberto removeu itens baratos antigos demais"
            )
    if safe_buy:
        filter_reasons.append(
            "- safe-buy exige confiança alta, listing recente e preço mínimo"
        )
    if stats.skipped_invalid_currency > 0:
        filter_reasons.append(
            f"- {stats.skipped_invalid_currency} itens tinham moeda sem conversão válida"
        )

    if filter_reasons:
        console.print("\n[dim]Possíveis causas:[/dim]")
        for reason in filter_reasons:
            console.print(f"  [dim]{reason}[/dim]")


def _render_flip_plan(plan):
    plan_table = Table(title=f"Plano: {plan.target.label}", expand=True)
    plan_table.add_column("Passo", justify="center", style="cyan")
    plan_table.add_column("Ação", style="white")
    plan_table.add_column("Mod", style="magenta")
    plan_table.add_column("EV Custo", justify="right", style="yellow")
    plan_table.add_column("Prob.", justify="right", style="green")
    plan_table.add_column("Valor após", justify="right", style="blue")

    for index, step in enumerate(plan.steps, start=1):
        action_label = step.action_name + (" [STOP]" if step.stop_here else "")
        plan_table.add_row(
            str(index),
            action_label,
            step.target_mod,
            f"{step.expected_cost:.1f}c",
            f"{step.probability:.2f}",
            f"{step.expected_value_after_step:.1f}c",
        )

    summary = Table.grid(padding=1)
    summary.add_row(
        Text(
            f"Compra sugerida: {plan.opportunity.base_type} por {plan.buy_cost:.1f}c",
            style="bold cyan",
        )
    )
    summary.add_row(
        Text(
            f"Liga: {plan.opportunity.resolved_league} | Score: {plan.opportunity.score:.1f}",
            style="dim",
        )
    )
    summary.add_row(
        Text(f"Alvo recomendado: {plan.target.label}", style="bold magenta")
    )
    summary.add_row(Text(plan.target.rationale, style="dim"))
    summary.add_row(
        Text(
            f"Custo esperado de craft: {plan.expected_craft_cost:.1f}c", style="yellow"
        )
    )
    summary.add_row(
        Text(f"Valor esperado de venda: {plan.expected_sale_value:.1f}c", style="green")
    )
    summary.add_row(
        Text(f"Lucro esperado líquido: {plan.expected_profit:.1f}c", style="bold green")
    )
    summary.add_row(
        Text(f"Confiança do plano: {plan.plan_confidence:.2f}", style="bold blue")
    )
    summary.add_row(Text(f"Stop-and-sell: {plan.stop_condition}", style="white"))
    if plan.risk_notes:
        summary.add_row(Text(f"Riscos: {', '.join(plan.risk_notes)}", style="red"))
    if plan.alternatives:
        summary.add_row(
            Text(f"Alternativas: {' | '.join(plan.alternatives)}", style="dim")
        )

    console.print(
        Panel(
            summary,
            border_style="magenta",
            title="[bold magenta]Flip Advisor[/bold magenta]",
        )
    )
    console.print(plan_table)


class HideoutDashboard:
    def __init__(self, target_mods: list, max_budget: float):
        self.target_mods = target_mods
        self.max_budget = max_budget
        self.console = console
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
        self.current_item: ItemState | None = None
        self.calculating = False
        self.result_path = None
        self.result_cost = 0.0

    def generate_layout(self):
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
        table = Table(
            title=f"Rota Ótima A* - {self.current_item.base_type}",
            expand=True,
            title_style="bold magenta",
        )
        table.add_column("Passo", style="cyan", justify="center")
        table.add_column("Ação de Craft", style="white")
        table.add_column("Chance", justify="right", style="green")
        table.add_column("Custo EV Acumulado (c)", justify="right", style="yellow")

        for index, action in enumerate(self.result_path, 1):
            if isinstance(action, CraftingAction):
                table.add_row(
                    str(index),
                    action.action_name,
                    f"{(action.probability * 100):.2f}%",
                    f"{action.ev_cost:.1f}",
                )
            else:
                table.add_row(str(index), str(action), "N/A", "N/A")

        if self.result_cost == float("inf"):
            return Panel(
                Align.center(
                    Text(
                        "❌ Nenhuma rota plausível encontrada dentro do orçamento!",
                        style="bold red",
                    )
                ),
                border_style="red",
            )

        panel_group = Table.grid(padding=1)
        panel_group.add_row(table)
        panel_group.add_row(
            Text(
                f"\nCusto EV Estimado Total: {self.result_cost:.1f} chaos",
                style="bold green",
            )
        )
        return Panel(
            panel_group, border_style="green", title="[bold green]Path Found[/]"
        )

    def on_item_copied(self, item: ItemState):
        self.current_item = item
        self.calculating = True
        self.result_path = None
        self.result_cost = 0.0
        time.sleep(0.5)
        try:
            result = self.graph_engine.find_cheapest_route(
                item, self.target_mods, self.max_budget
            )
            if result:
                self.result_path, self.result_cost = result
            else:
                self.result_path = []
                self.result_cost = float("inf")
        except Exception as exc:
            self.result_path = [f"System Error: {str(exc)}"]
            self.result_cost = float("inf")
        self.calculating = False


@app.command()
def craft_path(
    budget: float = typer.Option(5000.0, help="Orçamento máximo em Chaos"),
    targets: str = typer.Option(
        "maximum_life_1,movement_speed_1", help="Mods alvos separados por vírgula"
    ),
):
    """Comando legado de craft-path com clipboard, mantido apenas por compatibilidade."""
    target_mod_list = [mod.strip() for mod in targets.split(",")]
    dashboard = HideoutDashboard(target_mod_list, budget)
    scanner = ClipboardScanner(callback=dashboard.on_item_copied)

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
        "", "--type", help="Nome base do item; sem --type o scan aberto prioriza confiança"
    ),
    ilvl: int = typer.Option(1, help="Item Level mínimo"),
    rarity: str = typer.Option(
        "rare", help="Raridade do item (ex: rare, unique, normal)"
    ),
    max_items: int = typer.Option(
        30, min=1, help="Quantidade máxima de itens para avaliar"
    ),
    stale_hours: float = typer.Option(
        48.0, min=0, help="Hora limite para listings antigos"
    ),
    league: str = typer.Option("auto", "-l", help="Liga do Path of Exile ou 'auto'"),
    min_profit: float = typer.Option(0.0, "--min-profit", help="Lucro mínimo em Chaos"),
    min_listed_price: float = typer.Option(
        0.0,
        "--min-listed-price",
        min=0.0,
        help="Preço mínimo listado (em chaos) para considerar oportunidades",
    ),
    anti_fix: bool = typer.Option(
        True, "--anti-fix/--no-anti-fix", help="Ativar filtro anti-price-fixing"
    ),
    safe_buy: bool = typer.Option(
        False,
        "--safe-buy/--no-safe-buy",
        help="Modo extra-conservador por cima do perfil normal",
    ),
    output: str = typer.Option("", "--output", "-o", help="Salvar saída em arquivo"),
    full: bool = typer.Option(
        False, "--full", help="Tabela detalhada com colunas extras"
    ),
    output_format: str = typer.Option(
        "table", "--format", help="Formato: table|json|csv|jsonl"
    ),
):
    """Scanner de arbitragem; sem --type prioriza confiança, com --type fica mais permissivo."""
    from rich.status import Status

    valid_formats = ["table", "json", "csv", "jsonl"]
    if output_format not in valid_formats:
        raise typer.BadParameter(
            f"Formato inválido: '{output_format}'. Use: {', '.join(valid_formats)}"
        )

    scanner = OnDemandScanner(league=league)
    with Status(
        "[bold cyan]Buscando oportunidades e calculando score...[/]", spinner="dots"
    ):
        results, stats = scanner.run_scan(
            item_class=item_type,
            ilvl_min=ilvl,
            rarity=rarity,
            max_items=max_items,
            stale_hours=stale_hours,
            min_profit=min_profit,
            min_listed_price=min_listed_price,
            anti_fix=anti_fix,
            safe_buy=safe_buy,
        )

    if not results:
        _render_no_results_message(
            min_profit=min_profit,
            min_listed_price=min_listed_price,
            anti_fix=anti_fix,
            safe_buy=safe_buy,
            stats=stats,
        )
        return

    _render_kpi_panel(stats)

    payload = None
    if output_format == "json":
        payload = _scan_results_to_json(results)
    elif output_format == "csv":
        payload = _scan_results_to_csv(results)
    elif output_format == "jsonl":
        payload = _scan_results_to_jsonl(results)

    if payload is not None:
        if output:
            _save_output(output, payload)
            console.print(f"[green]Saída salva em: {output}[/green]")
        else:
            typer.echo(payload)
        return

    console.print(_render_scan_table(results, full=full))


@app.command("flip-plan")
def flip_plan(
    item_type: str = typer.Option("", "--type", help="Limita as bases analisadas"),
    ilvl: int = typer.Option(1, help="Item level mínimo"),
    rarity: str = typer.Option("rare", help="Raridade do item"),
    max_items: int = typer.Option(
        30, min=1, help="Quantidade máxima de listings avaliados"
    ),
    budget: float = typer.Option(
        150.0, min=1.0, help="Orçamento máximo de craft em chaos"
    ),
    top: int = typer.Option(3, min=1, max=10, help="Quantidade de planos a exibir"),
    stale_hours: float = typer.Option(
        48.0, min=0, help="Hora limite para listings antigos"
    ),
    league: str = typer.Option("auto", "-l", help="Liga do Path of Exile ou 'auto'"),
    min_profit: float = typer.Option(
        0.0, "--min-profit", help="Lucro mínimo da oportunidade base"
    ),
    min_listed_price: float = typer.Option(
        0.0,
        "--min-listed-price",
        min=0.0,
        help="Preço mínimo listado (em chaos) para considerar oportunidades base",
    ),
    anti_fix: bool = typer.Option(
        True, "--anti-fix/--no-anti-fix", help="Ativar filtro anti-price-fixing"
    ),
    safe_buy: bool = typer.Option(
        False, "--safe-buy/--no-safe-buy", help="Ativar filtro conservador de compra"
    ),
    output: str = typer.Option("", "--output", "-o", help="Salvar o relatório em JSON"),
    output_format: str = typer.Option("table", "--format", help="Formato: table|json"),
):
    """Flip advisor: escolhe oportunidades do scanner e devolve um plano econômico detalhado."""
    from rich.status import Status

    if output_format not in {"table", "json"}:
        raise typer.BadParameter("Formato inválido. Use 'table' ou 'json'.")

    advisor = FlipAdvisor(league=league)
    with Status(
        "[bold cyan]Procurando flips e calculando plano econômico...[/]", spinner="dots"
    ):
        plans, stats = advisor.recommend_plans(
            item_class=item_type,
            ilvl_min=ilvl,
            rarity=rarity,
            max_items=max_items,
            min_profit=min_profit,
            min_listed_price=min_listed_price,
            anti_fix=anti_fix,
            safe_buy=safe_buy,
            stale_hours=stale_hours,
            budget=budget,
            top_plans=top,
        )

    if not plans:
        _render_no_results_message(
            min_profit=min_profit,
            min_listed_price=min_listed_price,
            anti_fix=anti_fix,
            safe_buy=safe_buy,
            stats=stats,
        )
        console.print(
            "[yellow]Nenhum flip viável coube no orçamento informado.[/yellow]"
        )
        return

    if output_format == "json":
        payload = _flip_plans_to_json(plans)
        if output:
            _save_output(output, payload)
            console.print(f"[green]Relatório salvo em: {output}[/green]")
        else:
            typer.echo(payload)
        return

    console.print(
        Panel(
            Text(f"Liga resolvida: {stats.resolved_league}", style="bold cyan"),
            border_style="cyan",
        )
    )
    for plan in plans:
        _render_flip_plan(plan)


@app.command()
def meta_sync():
    """Sincroniza economia (Módulo C)."""
    console.print("Rode as rotinas do poe.ninja aqui.")


@app.command()
def rescue_snipe(budget: float = typer.Option(..., help="Orçamento em Chaos")):
    """Módulo A de arbitragem."""
    console.print("Iniciando varredura na API de Trade da GGG.")


@app.command()
def rog_assist():
    """Placeholder para futura evolução do Rog Oracle."""
    console.print("Rog Oracle fica para a próxima milestone.")


if __name__ == "__main__":
    app()


