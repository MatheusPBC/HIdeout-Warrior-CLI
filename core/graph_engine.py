import sys
import heapq
from typing import Dict, List, Tuple, Set, Optional
from dataclasses import dataclass, field
from core.api_integrator import MarketAPIClient
from core.evaluator import CraftingEvaluator
from core.recombinators import RecombinatorEngine

@dataclass(frozen=True)
class ItemState:
    """
    O(1) Hashable Representation of an Item's Core state.
    Utilizando `frozenset` para que a busca identifique nós vizinhos instataneamente em grafos A*.
    """
    base_type: str
    ilvl: int
    prefixes: frozenset[str]
    suffixes: frozenset[str]
    is_fractured: bool = False
    
    @property
    def open_prefixes(self) -> int:
        return 3 - len(self.prefixes)
        
    @property
    def open_suffixes(self) -> int:
        return 3 - len(self.suffixes)

@dataclass
class CraftingAction:
    """
    Aresta do Grafo A*.
    Define 'O Que Fizemos' (action_name), e 'Quanto Custou' estatisticamente (ev_cost).
    """
    action_name: str
    target_state: ItemState
    ev_cost: float
    probability: float

class CraftingGraphEngine:
    """
    Módulo B: Motor de Engenharia Reversa (Graph Pathfinding).
    Algoritmo A-Star (A*) acoplado que encontra a rota matematicamente mais barata de craft
    considerando o "Expected Value" financeiro de cada passo x Probabilidade.
    """
    def __init__(self, market_api: MarketAPIClient, evaluator: CraftingEvaluator, recombinator: RecombinatorEngine):
        self.market_api = market_api
        self.evaluator = evaluator
        self.recombinator = recombinator
        
        # Preços de exemplo carregados via ninja cache. No mundo real chamamos self.market_api
        self.currency_cache = {
            "Chaos Orb": 1.0,
            "Exalted Orb": 15.0,
            "Divine Orb": 350.0,
            "Veiled Orb": 8000.0, # Exemplo Caríssimo
            "Harvest Reforge": 20.0
        }

    def _get_price(self, currency_name: str) -> float:
         """Coleta o preço da Currency do Cache do Módulo C (Meta-Sync). Dando fallback para tabela dura."""
         # O certo é bater no market_cache O(1) gerado pelo poe.ninja da fase anterior.
         return self.currency_cache.get(currency_name, 1.0)

    def _calculate_ev(self, currency_cost: float, probability: float, fixed_fee: float = 0.0) -> float:
        """
        Calcula o Expected Value financeiro.
        Se a chance for 10% de hitar num exalted (15c), EV = 15c / 0.1 = 150c para o Hit Esperado.
        fixed_fees contabilizam custos do bench craft como 'Prefixes Cannot Be Changed' (+2 Divines).
        """
        if probability <= 0.0:
            return float('inf')
        return (currency_cost / probability) + fixed_fee

    def heuristic_cost_estimate(self, current_state: ItemState, goal_mods: Set[str]) -> float:
        """
        Função H() do A*: Estimativa otimista pra Poda (Pruning).
        Quantos mods ainda nos faltam pra terminar o Item?
        """
        current_mods = current_state.prefixes.union(current_state.suffixes)
        missing_mods = goal_mods - current_mods
        # Heurística Submissiva: Acreditamos otimisticamente que os mods faltantes vão
        # custar pelo menos 5 Chaos Orbs cada num mundo perfeito onde hitamos 100%.
        return len(missing_mods) * 5.0

    def generate_neighbors(self, state: ItemState, target_mods: Set[str]) -> List[CraftingAction]:
        """
        Rotina Principal: Expansão Dinâmica da Árvore.
        Diferente do network.x fixo, no Path of Exile o grafo é Infinito.
        Geramos apenas vizinhos plausíveis em lazy-evaluation.
        """
        neighbors = []
        current_mods = list(state.prefixes.union(state.suffixes))
        
        # Tentativa de Exalted Orb
        if state.open_prefixes > 0 or state.open_suffixes > 0:
            for goal_mod in target_mods:
                 if goal_mod not in current_mods:
                     # Descobrimos o Probability Matrix pelo core Evaluator.
                     action_p = self.evaluator.calculate_mod_chance(
                          base_type=state.base_type,
                          current_mods=current_mods,
                          target_mod_id=goal_mod,
                          action="Exalt"
                     )
                     if action_p > 0:
                         ev = self._calculate_ev(self._get_price("Exalted Orb"), action_p)
                         
                         # Simula o novo estado
                         new_prefixes = set(state.prefixes)
                         new_suffixes = set(state.suffixes)
                         # Ignorando Type pra demo, botamos no prefix ou sufix aberto aleatorio.
                         if state.open_prefixes > 0:
                             new_prefixes.add(goal_mod)
                         else:
                             new_suffixes.add(goal_mod)
                             
                         new_state = ItemState(
                             base_type=state.base_type, 
                             ilvl=state.ilvl, 
                             prefixes=frozenset(new_prefixes), 
                             suffixes=frozenset(new_suffixes)
                         )
                         
                         neighbors.append(CraftingAction("Slam Exalted Orb", new_state, ev, action_p))
                         
        # Tentativa de Harvest Reforge Speed
        if True: # Ignorando regra de block pra simplificação do Snippet A*
            for goal_mod in target_mods:
                if goal_mod not in current_mods:
                    # Rola tudo. Ignorado Bench-Craft Protection aqui.
                    action_p = self.evaluator.calculate_mod_chance(
                          base_type=state.base_type,
                          current_mods=[],
                          target_mod_id=goal_mod,
                          action="Harvest"
                     )
                    if action_p > 0:
                         ev = self._calculate_ev(self._get_price("Harvest Reforge"), action_p)
                         new_state = ItemState(
                             base_type=state.base_type, ilvl=state.ilvl, 
                             prefixes=frozenset([goal_mod]), suffixes=frozenset([])
                         )
                         neighbors.append(CraftingAction(f"Harvest Reforge -> Hit {goal_mod}", new_state, ev, action_p))
                         
        return neighbors

    def find_cheapest_route(self, start_item: ItemState, goal_mods: List[str], max_budget: float) -> Optional[Tuple[List[str], float]]:
        """
        Motor de Busca A* com Early Pruning usando Custom Queues de Alta Performance.
        """
        goal_set = frozenset(goal_mods)
        
        # Priority Queue: (Est_Total_Cost, G_Cost, NodeId_Para_TieBreak, CurrentState, Path_of_Actions)
        # O A* ordena pela Heurística Estimada (F = G + H)
        open_set = []
        heapq.heappush(open_set, (0.0, 0.0, id(start_item), start_item, []))
        
        # Guardamos a distacia percorrida real pra evitar loopear num nó já otimizado (O(1)).
        g_scores: Dict[ItemState, float] = {start_item: 0.0}
        
        while open_set:
            estimated_f, g_cost, _, current_state, current_path = heapq.heappop(open_set)
            
            # Condição de Morte
            if g_cost > max_budget:
                continue # Pruning da arvore
                
            # Verifica Objetivo: O item possui TODOS os targets estritos da busca JSON?
            current_mods = current_state.prefixes.union(current_state.suffixes)
            if goal_set.issubset(current_mods):
                return current_path, g_cost
                
            # Expande Nós
            for action in self.generate_neighbors(current_state, goal_set):
                tentative_g_cost = g_cost + action.ev_cost
                
                # Se extrapolou o bankroll, nem olha
                if tentative_g_cost > max_budget:
                    continue
                    
                # Se nós achamos um caminho PRO MESMO ESTADO mais barato!
                if tentative_g_cost < g_scores.get(action.target_state, float('inf')):
                    g_scores[action.target_state] = tentative_g_cost
                    f_cost = tentative_g_cost + self.heuristic_cost_estimate(action.target_state, goal_set)
                    
                    new_path = current_path + [action.action_name]
                    heapq.heappush(open_set, (f_cost, tentative_g_cost, id(action.target_state), action.target_state, new_path))
                    
        # Nenhuma rota plausível encontrada dentro do Budget.
        return None

if __name__ == "__main__":
    from core.data_parser import RePoeParser
    
    print("--- Teste Unitário: A* Search Graph Engine ---")
    
    # Dependências falsas/básicas simuladas para injeção
    parser = RePoeParser()
    evaluator = CraftingEvaluator(parser)
    market = MarketAPIClient()
    recombinators = RecombinatorEngine()
    
    engine = CraftingGraphEngine(market, evaluator, recombinators)
    
    start = ItemState("Omen Wand", 84, frozenset([]), frozenset([]))
    goals = ["SpellDamage1"]
    
    print("Iniciando varredura termodinâmica do A*...")
    res = engine.find_cheapest_route(start, goals, max_budget=50000.0)
    
    if res:
         path, cost = res
         print(f"✅ Melhor Rota Encontrada! Custo EV Final: {cost} chaos")
         print(f"Passos Necessários ({len(path)}):")
         for i, step in enumerate(path, 1):
             print(f" {i}. {step}")
    else:
         print("❌ Nenhuma Rota possível identificada nesse orçamento.")
