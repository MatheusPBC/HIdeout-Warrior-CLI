# Hideout Warrior CLI (PoE 3.28 Mirage) - Planning & Progress

## 1. Contrato e Master Document (Visão do Usuário)

**System Role:** Arquiteto de Software Sênior especializado em Python, Algoritmos de Pathfinding (Grafos) e Automação de Dados via APIs. Objetivo: desenvolver a versão definitiva da CLI "Hideout Warrior" para Path of Exile (Liga 3.28 Mirage), uma ferramenta de arbitragem financeira e otimização de crafting.

### Visão Geral e Restrições

O sistema opera 100% via terminal (CLI) e sob demanda. **Não haverá automação de cliques ou injeção de memória (Zero Botting)** para manter compliance com os Termos de Serviço da GGG. O foco é resolver assimetria de informação, calculando Expected Value (EV) de crafts e encontrando itens mal precificados no mercado.

### Stack Tecnológica

* **Linguagem:** Python 3.10+
* **Interface:** `Typer` (otimizado para leitura em terminal/tmux).
* **Bibliotecas Core:** `requests` (API GGG/poe.ninja), `networkx` (Pathfinding de Crafting), `pyperclip` (integração de clipboard), `numpy` (probabilidades de Recombinators).

### Módulos de Inteligência (The Core Engines)

* **Módulo A: O Hospital de Itens & The Broker (Snipe/Rescue)**
  * Lógica: Varre a API de Trade por itens com 2-3 afixos T1/T2 que estão "bricked" e precificados abaixo da base limpa. Calcula limpeza viável via Beastcraft/Eldritch.
  * Broker: Formata whisper target e envia para o clipboard OS.
* **Módulo B: Motor de Engenharia Reversa & Recombinators (Graph Pathfinding)**
  * Lógica: Modela o crafting como um grafo (A* algorithm). Base -> Objetivo. Avalia a chance/custo de craft vs usar a matemática de Recombinators via `numpy`.
* **Módulo C: Meta-Sync (Scraping do poe.ninja)**
  * Lógica: Consome poe.ninja para elencar afixos do meta atual e alimentar o Módulo A.
* **Módulo D: Rog Oracle (Clipboard Listener)**
  * Lógica: Monitora SO Clipboard; quando `Ctrl+C` in-game no NPC Rog, printa no terminal a melhor decisão com base nos weights.

### Comandos da CLI Implementados

* `hideout-warrior rescue-snipe --budget <valor>` -> Roda Módulo A
* `hideout-warrior craft-path --target <json_item> --allow-recombinators` -> Roda Módulo B
* `hideout-warrior meta-sync` -> Roda Módulo C
* `hideout-warrior rog-assist` -> Roda Módulo D

---

## 2. Status Atual (Onde Paramos)

### Concluído (Milestone 1 e Início do Milestone 2)

- **Project Structure**: Diretórios criados.
* **Dependencies (`requirements.txt`)**: Instalado `typer`, `networkx`, `numpy`, `pyperclip`, `requests`.
* **CLI via Typer (`cli.py`)**: Endpoints roteados corretamente sem erros de sintaxe. Mocks básicos criados.
* **Módulo B (Graph Engine & Recombinators)**:
  * `core/recombinators.py`: Criada a `RecombinatorMath` que usa `numpy` para prever e fazer clip realista de ~1% a ~99% da sobrevivência dos afixos no clash de dois itens doadores.
  * `core/graph_engine.py`: Criada a `CraftingGraphEngine` que modela craft como nós do State e arestas de Ações; usa `networkx.astar_path` para caçar a heurística e EV mais barato.
  * O comando typer `--allow-recombinators` já injeta as duas dependências com sucesso como mock.

### Faltando

- **Módulo A (Hospital / Broker)**: Implementar integração com a API da GGG (rate-limits rígidos, fetch de itens), parseamento da estrutura do trade e a automação do clipboard para whisper de compra.
* **Módulo C (Meta-Sync)**: Implementar fetch via `requests` na API do poe.ninja e construir os profiles dinâmicos (weights json).
* **Módulo D (Rog Oracle)**: Fazer o loop principal assíncrono do clipboard viewer usando `pyperclip` pra ler os text-blocks do jogo e reagir com a avaliação do Rog.
