# Hideout Warrior CLI - Architecture Documentation

## Visão Geral do Sistema

O **Hideout Warrior** é uma aplicação CLI (Command Line Interface) desenvolvida em Python para atuar como uma ferramenta de arbitragem financeira e otimização de crafting no jogo Path of Exile (Liga 3.28 Mirage).

O sistema opera de maneira **estrita e sob demanda**, garantindo conformidade total (Zero Botting e Zero Automação de Cliques) com as regras da GGG. O design foca na resolução rápida de assimetria de informações através de Scraping de Meta, A* Pathfinding de grafos e predições baseadas em matemática estatística para *Recombinators*.

---

## Stack Tecnológica Core

* **Linguagem:** Python 3.10+
* **Interface:** `Typer`
* **Network & API:** `requests`
* **Engines de Matemática Estrutural:** `networkx` (Pathfinding), `numpy` (Recombinator Stats)
* **Monitoramento O.S:** `pyperclip`

---

## Módulos e Componentes

### 1. `cli.py` (Command Line Interface)

O arquivo principal de entrada do sistema. Utiliza a biblioteca `Typer` para expor os comandos de forma organizada e elegante no terminal. Ele orquestra e inicializa as injeções de dependência para os 4 módulos *Core*.

### 2. `core/api_integrator.py` (Módulo A - Hospital de Itens)

**Função:** Motor centralizado para se comunicar com as APIs da GGG.
* Responsável por montar e gerenciar os payloads JSON para a `Trade API`.
* Contém a lógica de restrição absoluta de Rate-Limit exigida pela GGG.
* Filtra, busca e realiza o *fetch* de metadados dos itens listados (ex: buscando itens *bricked* que podem ser salvos com processos base-deterministicos como Eldritch Annuls).

### 3. `core/broker.py` (Módulo A - The Broker)

**Função:** Assistente veloz para transações de mercado.
* Roda após a validação do *API Integrator*.
* Formata a string exata do whisper de compra do Path of Exile.
* Injeta o texto no clipboard (área de transferência) do Sistema Operacional, permitindo que o usuário apenas de um `Alt+Tab` e `Ctrl+V` dentro do jogo de forma segura.

### 4. `core/graph_engine.py` (Módulo B - Motor de Grafos A*)

**Função:** Engrahar o crafting de itens.
* Transforma os métodos de craft em "Grafos Direcionados".
* Utiliza `networkx` e a heurística de busca A* (A-Star) para calcular rotas.
* O *edge_weight* da busca é o EV (Expected Value) ou "Custo" em Divines/Chaos daquela ação.
* Ele encontra a rota ótima estritamente matemática ignorando viés emocional (ex: Alteration Spam vs Fossil Crafting).

### 5. `core/recombinators.py` (Módulo B - Engine Recombinator)

**Função:** Engine de modelagem de probabilidade da Sentinel/Settlers re-introduced core.
* O craft de recombinators depende de "pools" colidindo (Sufixos vs Sufixos).
* Utiliza a biblioteca `numpy` para prever a probabilidade estatística teórica do afixo sobreviver na nova base, evitando que o usuário desperdice recursos se os nós de probabilidade do pool exclusivo (ou pool duplicado) não forem favoráveis contra a entropia da fusão.
* Disputa o EV do *GraphEngine* para sugerir se comprar duas bases lixo e combinar é estatisticamente mais barato.

### 6. `core/meta_sync.py` (Módulo C - Poe.Ninja Scraper)

**Função:** Alinhamento estratégico ao Meta do Jogo.
* Sincroniza dados consumindo o web-end do poe.ninja.
* O objetivo é não depender de heurísticas cegas. O robô raspa as Top Skills que estão sendo usadas na ladder e traduz isso pra "pesos" gerando um `current_meta_weights.json` local.
* Esses dados direcionarão a mira do **Módulo A** (Se Lightning Strike for o meta, o módulo de Trade focará bases que escalam Flat Lightning/Attack Speed).

### 7. `core/rog_oracle.py` (Módulo D - O Oráculo do Rog)

**Função:** Daemon Background de suporte live in-game.
* Fica monitorando a área de transferência do usuário iterativamente usando `pyperclip`.
* O jogador entra na interface de Expedição do NPC "Rog" e dá `Ctrl+C` no item apresentado.
* O Daemon intercepta o "Item String Block" do PoE, realiza o parse do ilvl, keywords (Tier, fractured, implicit) e lança no prompt se aquele item vale a pena receber investimento ou se é *Skip/Reroll*.
