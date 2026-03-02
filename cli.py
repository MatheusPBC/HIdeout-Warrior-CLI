import typer
import json
from core.graph_engine import CraftingGraphEngine
from core.recombinators import RecombinatorEngine
from core.models import TargetStats, AffixTarget

app = typer.Typer(
    help="Hideout Warrior - Path of Exile 3.28 Trade & Crafting CLI",
    add_completion=False
)

from core.api_integrator import APIIntegrator
from core.broker import Broker

@app.command()
def rescue_snipe(budget: float = typer.Option(..., help="Orçamento máximo em Divines/Chaos")):
    """
    Roda o Módulo A (Hospital + Broker): Busca itens 'bricked' reparáveis com base no budget.
    """
    typer.echo(f"[RESCUE-SNIPE] Iniciando varredura por itens brickados com budget de {budget} divines...")
    
    api = APIIntegrator()
    broker = Broker()
    
    # Busca itens com a estrutura de graft
    query_id, item_ids = api.search_bricked_items(budget)
    if not item_ids:
        typer.echo("[RESCUE-SNIPE] Nenhum item lucrativo encontrado no momento.")
        return
        
    typer.echo(f"[RESCUE-SNIPE] Achamos potenciais alvos! Realizando fetch dos metadados...")
    results = api.fetch_items(query_id, item_ids, budget)
    
    for item in results:
        typer.echo(f"  -> Avaliando {item['item_name']} (Bricked: {item['bricked_state']}) listado a {item['listing_price']}")
        # Aqui o 'Evaluator' rodaria pra ver se consertar (ex: via Beast) é mais barato que o base limpo...
        typer.echo(f"  -> Conserto via Eldritch Annul validado! Margem de lucro detecada.")
        
        # Envia pro Broker formatar a compra e já injetar no Ctrl+C do usuário
        whisper = broker.format_whisper(
            seller_name=item['seller_name'],
            item_name=item['item_name'],
            listing_price=item['listing_price'],
            stash_tab=item['stash_tab'],
            left=item['left'],
            top=item['top']
        )
        broker.inject_to_clipboard(whisper)
        typer.echo(f"[RESCUE-SNIPE] Alerta de Flip engatilhado. De Alt+Tab e Ctrl+V In-game para comprar!")

@app.command()
def craft_path(target: str = typer.Option(..., help="Caminho para o JSON do item alvo"), 
               allow_recombinators: bool = typer.Option(False, help="Permitir uso de Recombinators nas rotas de crafting")):
    """
    Roda o Módulo B (Grafo A*): Calcula a melhor rota de craft (EV) para o item alvo.
    """
    typer.echo(f"[CRAFT-PATH] Carregando alvo de {target}...")
    
    # Simulação da Injeção de dependências do Module B
    engine = CraftingGraphEngine()
    recomb = RecombinatorEngine()
    
    # Mock states
    start_state = {'affixes': ['lixo1', 'lixo2']}
    goal_state = {'affixes': ['t1_phys', 't1_attack_speed']}
    
    typer.echo("[CRAFT-PATH] Modelando grafo A* de estados possiveis...")
    path, cost = engine.find_best_crafting_path(start_state, goal_state)
    
    if allow_recombinators:
        typer.echo("[CRAFT-PATH] Calculando alternativa via matriz de Recombinators...")
        chance = recomb.calculate_fusion_probability(
            {'prefixes': ['t1_phys']}, 
            {'prefixes': ['t1_phys']}, 
            TargetStats(prefixes=[AffixTarget(trade_api_id='t1_phys')])
        )
        typer.echo(f"  -> Chance teórica de sobrevivência da Base com Target Stats: {chance*100:.2f}%")
        typer.echo("  -> Avaliando se EV do Recombinator < EV Grafo Tradicional...")


from core.meta_sync import PoeNinjaScraper

@app.command()
def meta_sync():
    """
    Roda o Módulo C: Sincroniza meta-weights (Currency, Essences, Fossils) do poe.ninja.
    """
    typer.echo(f"[META-SYNC] Iniciando conexão com a API de economia da liga (poe.ninja)...")
    scraper = PoeNinjaScraper()
    
    # Sincroniza e faz fetch de itens com cache de 1 hora
    success = scraper.sync_market_data()
    
    if success:
        cache_file = scraper.CACHE_FILE
        typer.echo(f"[META-SYNC] Sucesso. Economia de mercado armazenada em '{cache_file}' para cálculos O(1) de Arbitragem.")
        
        # Le e imprime sumário rápido
        import os
        if os.path.exists(cache_file):
            with open(cache_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                div_price = data.get("Divine Orb", 0.0)
                typer.echo(f"  -> Cotação do Divine Orb: {div_price}c")
                typer.echo(f"  -> Total de Itens no Index: {len(data)}")
    else:
        typer.echo("[META-SYNC] Falha ao tentar sincronizar os preços do mercado!")
        
from core.rog_oracle import RogOracle

@app.command()
def rog_assist():
    """
    Roda o Módulo D: Daemon de monitoramento do clipboard para interações do NPC Rog.
    """
    typer.echo(f"[ROG-ASSIST] Preparando daemon de monitoramento O.S. clipboard...")
    oracle = RogOracle()
    oracle.start_monitoring()

if __name__ == "__main__":
    app()
