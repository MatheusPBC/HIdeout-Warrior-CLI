import time
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
    add_completion=False
)

console = Console()

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
            self.market, self.evaluator, self.recombinators, 
            self.predictor, self.heuristic
        )
        
        # UI State
        self.current_item: ItemState = None
        self.calculating = False
        self.result_path = None
        self.result_cost = 0.0

    def generate_layout(self):
        """Desenha a UI Reativa Baseada no Estado Atual."""
        if self.calculating:
            spin = Spinner("dots", text=Text("Oráculo calculando rotas no A* Optimization Engine...", style="cyan"))
            return Panel(spin, title="[yellow]A-Star Pathfinding Active[/]", border_style="yellow")
            
        if self.result_path is not None:
             return self._generate_results_table()
             
        # Tela de Padrão Aguardando Ação
        msg = Text.assemble(
            ("⚔️ Hideout Warrior v1.0\n\n", "bold bright_white"),
            ("Aguardando Ctrl+C no Path of Exile...\n", "dim"),
            (f"Orçamento Máximo: {self.max_budget}c | Alvos: {', '.join(self.target_mods)}", "bold blue")
        )
        return Panel(Align.center(msg), border_style="bold black", padding=(2, 4))

    def _generate_results_table(self):
        """Desenha a tabela de Ações de Craft do A*."""
        table = Table(title=f"Rota Ótima A* - {self.current_item.base_type}", expand=True, title_style="bold magenta")
        table.add_column("Passo", style="cyan", justify="center")
        table.add_column("Ação de Craft", style="white")
        table.add_column("Chance", justify="right", style="green")
        table.add_column("Custo EV Acumulado (c)", justify="right", style="yellow")
        
        for i, action in enumerate(self.result_path, 1):
             # Em um cenario ideal, GraphEngine retorna List[CraftingAction]. 
             # Como retornava strings no esqueleto, adaptamos para display:
             if isinstance(action, CraftingAction):
                 table.add_row(str(i), action.action_name, f"{(action.probability*100):.2f}%", f"{action.ev_cost:.1f}")
             else:
                 table.add_row(str(i), str(action), "N/A", "N/A")
                 
        summary_color = "green" if self.result_cost < self.max_budget else "red"
        
        panel_group = Table.grid(padding=1)
        panel_group.add_row(table)
        panel_group.add_row(Text(f"\nCusto EV Estimado Total: {self.result_cost:.1f} chaos", style=f"bold {summary_color}"))
        
        # ROI é complexo sem bater no Ninja para o preço do alvo final, omitindo para fluidez local.
        if self.result_cost == float('inf'):
             msg = Text("❌ Nenhuma rota plausível encontrada dentro do orçamento!", style="bold red")
             return Panel(Align.center(msg), border_style="red")
             
        return Panel(panel_group, border_style="green", title="[bold green]Path Found[/]")

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
            res = self.graph_engine.find_cheapest_route(item, self.target_mods, self.max_budget)
            if res:
                self.result_path, self.result_cost = res
            else:
                self.result_path = [] # Empty path trigger
                self.result_cost = float('inf')
        except Exception as e:
            self.result_path = [f"System Error: {str(e)}"]
            self.result_cost = float('inf')
            
        self.calculating = False


@app.command()
def craft_path(
    budget: float = typer.Option(5000.0, help="Orçamento máximo em Chaos"),
    targets: str = typer.Option("maximum_life_1,movement_speed_1", help="Mods alvos separados por vírgula")
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
        with Live(dashboard.generate_layout(), refresh_per_second=4, screen=False) as live:
            while True:
                live.update(dashboard.generate_layout())
                time.sleep(0.2)
    except KeyboardInterrupt:
        scanner.stop()
        console.print("[dim]Encerrando Hideout Warrior...[/dim]")


@app.command()
def scan(
    type: str = typer.Option("", help="Nome base do item (ex: 'Imbued Wand')"),
    ilvl: int = typer.Option(1, help="Item Level mínimo"),
    rarity: str = typer.Option("rare", help="Raridade do item (ex: rare, unique, normal)"),
    max_items: int = typer.Option(30, help="Quantidade máxima de itens para avaliar na paginação")
):
    """
    (Fase 7) Scanner de Arbitragem Sob Demanda. Interroga a API da GGG e o ML Oracle
    em busca de lucros subvalorizados no mercado.
    """
    from rich.status import Status
    scanner = OnDemandScanner(league="Standard")
    
    with Status("[bold cyan]Buscando itens na API da GGG e avaliando rentabilidade via XGBoost...[/]", spinner="dots") as status:
        results = scanner.run_scan(item_class=type, ilvl_min=ilvl, rarity=rarity, max_items=max_items)
        
    if not results:
        console.print("[yellow]Nenhuma listagem com buyout foi encontrada para esses filtros na liga atual.[/yellow]")
        return
        
    table = Table(title=f"🤑 Oportunidades de Arbitragem (Total: {len(results)})", expand=True)
    table.add_column("Item Base", style="cyan")
    table.add_column("Preço (Chaos)", justify="right", style="red")
    table.add_column("Valor ML (Chaos)", justify="right", style="magenta")
    table.add_column("Lucro (Chaos)", justify="right", style="bold")
    table.add_column("Comando Whisper", style="dim white")
    table.add_column("Link Trade", style="blue")
    
    for r in results:
        profit = r["profit"]
        
        # Colorir lucros altos
        profit_style = "bold green" if profit > 50.0 else "yellow" if profit > 0 else "white"
        
        # Truncate whisper for display
        whisper_display = r["whisper"][:80] + "..." if len(r["whisper"]) > 80 else r["whisper"]
        
        # Truncate link for display
        trade_link = r.get("trade_link", "")
        link_display = trade_link[:50] + "..." if len(trade_link) > 50 else trade_link
        
        table.add_row(
            r["base_type"],
            f"{r['listed_price']:.1f}",
            f"{r['ml_value']:.1f}",
            f"[{profit_style}]{profit:.1f}[/]",
            whisper_display,
            link_display
        )
        
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
