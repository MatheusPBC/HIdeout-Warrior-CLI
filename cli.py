import typer
import json
from core.graph_engine import CraftingGraphEngine
from core.recombinators import RecombinatorMath

app = typer.Typer(
    help="Hideout Warrior - Path of Exile 3.28 Trade & Crafting CLI",
    add_completion=False
)

@app.command()
def rescue_snipe(budget: float = typer.Option(..., help="Orçamento máximo em Divines/Chaos")):
    """
    Roda o Módulo A (Hospital + Broker): Busca itens 'bricked' reparáveis com base no budget.
    """
    typer.echo(f"[RESCUE-SNIPE] Iniciando varredura por itens brickados com budget de {budget}...")

@app.command()
def craft_path(target: str = typer.Option(..., help="Caminho para o JSON do item alvo"), 
               allow_recombinators: bool = typer.Option(False, help="Permitir uso de Recombinators nas rotas de crafting")):
    """
    Roda o Módulo B (Grafo A*): Calcula a melhor rota de craft (EV) para o item alvo.
    """
    typer.echo(f"[CRAFT-PATH] Carregando alvo de {target}...")
    
    # Simulação da Injeção de dependências do Module B
    engine = CraftingGraphEngine()
    recomb = RecombinatorMath()
    
    # Mock states
    start_state = {'affixes': ['lixo1', 'lixo2']}
    goal_state = {'affixes': ['t1_phys', 't1_attack_speed']}
    
    typer.echo("[CRAFT-PATH] Modelando grafo A* de estados possiveis...")
    path, cost = engine.find_best_crafting_path(start_state, goal_state)
    
    if allow_recombinators:
        typer.echo("[CRAFT-PATH] Calculando alternativa via matriz de Recombinators...")
        chance = recomb.calculate_affix_survival_chance({'t1_phys': 1}, {'t1_attack_speed': 1}, 't1_phys')
        typer.echo(f"  -> Chance teórica de sobrevivência do afixo primário: {chance*100:.2f}%")
        typer.echo("  -> Avaliando se EV do Recombinator < EV Grafo Tradicional...")


@app.command()
def meta_sync():
    """
    Roda o Módulo C: Sincroniza meta-weights e trends dinamicamente do poe.ninja.
    """
    typer.echo(f"[META-SYNC] Coletando afixos das top 5 builds do poe.ninja...")

@app.command()
def rog_assist():
    """
    Roda o Módulo D: Daemon de monitoramento do clipboard para interações do NPC Rog.
    """
    typer.echo(f"[ROG-ASSIST] Iniciando daemon de monitoramento O.S. clipboard...")

if __name__ == "__main__":
    app()
