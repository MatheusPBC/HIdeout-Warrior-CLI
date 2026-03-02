import networkx as nx
from typing import Dict, List, Tuple

class CraftingGraphEngine:
    """
    Módulo B: Motor de Engenharia Reversa (Graph Pathfinding).
    Modela o crafting como um grafo direcionado onde:
    - Nós: Representam o estado atual de um item (base, iLvl, afixos presentes).
    - Arestas: Representam as ações de crafting (Alteration, Augment, Regal, Essence, Recombinator).
    - Peso das Arestas: O Custo de Valor Esperado (Expected Value - EV) em Chaos/Divines daquela ação.
    """

    def __init__(self):
        self.graph = nx.DiGraph()

    def _state_to_node_id(self, item_state: Dict) -> str:
        """
        Converte o estado de um item em um identificador único para o nó.
        Exemplo: frozenset dos afixos para garantir que a ordem não importe.
        """
        affixes = item_state.get('affixes', [])
        return "|".join(sorted(affixes))

    def heuristic_cost_estimate(self, current_node: str, goal_node: str) -> float:
        """
        Função Heurística para o A* Search.
        Estima o custo restante do nó atual até o objetivo.
        Deve ser "admissível" (nunca superestimar o custo real) para garantir a rota ótima.
        
        Neste esqueleto: retorna a diferença no número de afixos alvo ausentes.
        """
        current_affixes = set(current_node.split('|')) if current_node else set()
        goal_affixes = set(goal_node.split('|')) if goal_node else set()
        
        missing_affixes = goal_affixes - current_affixes
        # Assumindo um custo base genérico mínimo por afixo faltante (ex: 10 chaos)
        return len(missing_affixes) * 10.0

    def add_crafting_edge(self, source_state: Dict, target_state: Dict, action_name: str, ev_cost: float):
        """
        Adiciona uma aresta representando uma tentativa de craft.
        """
        source_id = self._state_to_node_id(source_state)
        target_id = self._state_to_node_id(target_state)
        
        self.graph.add_edge(source_id, target_id, action=action_name, weight=ev_cost)

    def find_best_crafting_path(self, start_state: Dict, goal_state: Dict) -> Tuple[List[str], float]:
        """
        Executa o A* Pathfinding para encontrar a rota de craft mais barata (menor EV total).
        Retorna a lista de passos (ações) e o custo total estimado.
        """
        start_id = self._state_to_node_id(start_state)
        goal_id = self._state_to_node_id(goal_state)
        
        # Garante que os nós existem no grafo, do contrário falhará (em um caso real,
        # o grafo seria gerado dinamicamente ou pré-computado).
        if start_id not in self.graph or goal_id not in self.graph:
            # Em runtime, o algoritmo expandiria vizinhos ativamente (Lazy Evaluation).
            # Para a estrutura base, assumimos um grafo estático pré-preenchido.
            return [], float('inf')

        try:
            path = nx.astar_path(
                self.graph, 
                start_id, 
                goal_id, 
                heuristic=self.heuristic_cost_estimate, 
                weight='weight'
            )
            
            # Reconstroi as ações tomadas nas arestas e calcula o custo total
            actions = []
            total_cost = 0.0
            
            for i in range(len(path) - 1):
                u = path[i]
                v = path[i+1]
                edge_data = self.graph.get_edge_data(u, v)
                actions.append(edge_data['action'])
                total_cost += edge_data['weight']
                
            return actions, total_cost
            
        except nx.NetworkXNoPath:
            return [], float('inf')
