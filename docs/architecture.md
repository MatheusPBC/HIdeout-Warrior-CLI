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

## Pipeline de Dados de Mercado

O pipeline de mercado agora deve ser entendido em camadas explícitas:

### Raw

- `stash_events`: observações brutas da Public Stash API;
- `trade_bucket_events`: observações brutas da Trade API por buckets.

Essas tabelas preservam o payload bruto do item e os metadados operacionais mínimos de coleta.

### Bronze

Camada canônica de observações reconciliadas, produzida por `scripts/build_training_snapshot.py`.

Contrato principal por observação:

- `item_id`
- `league`
- `source`
- `source_table`
- `indexed_at`
- `collected_at`
- `first_seen_at`
- `last_seen_at`
- `seen_count`
- `source_count`
- `listing_age_seconds`
- `freshness_band`
- `price_amount`
- `price_currency`
- `price_chaos`
- `raw_item_json`
- `query_context`

Quando o mesmo item aparece em mais de uma fonte dentro da mesma liga, a reconciliação promove a observação para `source=both`, preservando também `source_table` para rastreabilidade do evento bruto.

### Silver

Camada normalizada por item, já compatível com o normalizador e com o parser de features.

Além dos atributos derivados do item, a camada Silver preserva contexto operacional relevante da Bronze, incluindo:

- `source`
- `source_count`
- `seen_count`
- `first_seen_at`
- `last_seen_at`
- `listing_age_seconds`
- `freshness_band`

### Gold

Camada final orientada a treino e avaliação. Ela mantém as features do item e carrega também os sinais estruturais de frescor/origem necessários para evoluir os próximos estágios de qualidade, labeling e modelagem.

### Fluxo resumido

`raw -> bronze -> silver -> gold -> train_oracle -> market_scanner`

Esse fluxo existe para separar claramente:

- ingestão bruta;
- reconciliação de observações;
- normalização de item;
- dataset de treino;
- uso operacional no scanner.

---

## Módulos e Componentes

### 1. `cli.py` (Command Line Interface)

O arquivo principal de entrada do sistema. Utiliza a biblioteca `Typer` para expor os comandos de forma organizada e elegante no terminal. Ele orquestra e inicializa as injeções de dependência para os 4 módulos *Core*.

### 2. `core/models.py` (Módulo Central de Contratos de Dados)

**Função:** Garantia de Estrita Tipagem de Estado para o Motor de Busca e Crafting.

* Utiliza **Pydantic** (`CraftingTargetSchema`, `AffixTarget`) para estruturar de forma escalável os filtros JSON que refletem o desejo do usuário.
* Cada Afixo recebe identificadores diretos do Path of Exile Trade (ex: `pseudo.pseudo_total_mana`), flags de Fraturado e Constraints rigorosas (limite de *Divines* para o craft, exigência de Prefixos/Sufixos vazios no final).

### 3. `core/api_integrator.py` (Módulo A - Hospital de Itens)

**Função:** Motor centralizado para se comunicar com as APIs da GGG.

* Utiliza a classe `GGGTradeAPI` mapeando de forma genuína para `https://www.pathofexile.com/api/trade`.
* Monta e gerencia os payloads JSON (`search_items`) extraídos do `core/models.py`.
* **Prevenção de BAN:** Possui um algoritmo de *Rate Limit Handling* rigoroso (`_handle_rate_limits`) que lê os headers HTTP do Cloudflare da GGG. Ao tomar HTTP 429, ele induz um `time.sleep()` forçado pelo valor lido na flag `Retry-After`.
* Realiza o GET (`fetch_item_details`) respeitando o hard limit de no máximo 10 itens por lote.

### 4. `core/broker.py` (Módulo A - The Broker)

**Função:** Assistente veloz para transações de mercado.

* Roda após a validação do *API Integrator*.
* Com base nas matrizes e hashes retornados da API oficial, formata a string exata do whisper de compra do Path of Exile (ex: `@Player Hi, I would like to buy your Tabula Rasa...`).
* Injeta o texto no clipboard (área de transferência) do Sistema Operacional via `pyperclip`, permitindo que o usuário apenas de um `Alt+Tab` e `Ctrl+V` dentro do jogo de forma segura.

### 5. `core/graph_engine.py` (Módulo B - Motor de Grafos A*)

**Função:** Engrahar o crafting de itens.

* Transforma os métodos de craft em "Grafos Direcionados".
* Utiliza `networkx` e a heurística de busca A* (A-Star) para calcular rotas.
* O *edge_weight* da busca é o EV (Expected Value) ou "Custo" em Divines/Chaos daquela ação lida pelo Módulo C.

### 6. `core/recombinators.py` (Módulo B - Engine Recombinator)

**Função:** Engine de modelagem de probabilidade da Sentinel/Settlers re-introduced core.

* O craft de recombinators depende de "pools" colidindo (Sufixos vs Sufixos).
* Utiliza matrizes do `numpy` para prever a probabilidade estatística teórica baseada nativamente nas regras de Retenção de Slots (Pool Size de 1 a 6 mods).
* Processa afixos Únicos vs. afixos Compartilhados utilizando cálculo hipergeométrico.

### 7. `core/meta_sync.py` (Módulo C - Poe.Ninja Scraper)

**Função:** Alinhamento estratégico ao Mercado Real.

* Sincroniza dados consumindo o web-end do `poe.ninja` para *Currency*, *Essences* e *Fossils*.
* Emprega um mecanismo de Defesa/Cache (`data/market_prices.json`) de 1-Hora para evitar onerar os servidores de terceiros.
* Fornece Complexidade O(1) de acesso real a Economy via função `get_price("Item Name")`.

### 7. `core/rog_oracle.py` (Módulo D - O Oráculo do Rog)

**Função:** Daemon Background de suporte live in-game.

* Fica monitorando a área de transferência do usuário iterativamente usando `pyperclip`.
* O jogador entra na interface de Expedição do NPC "Rog" e dá `Ctrl+C` no item apresentado.
* O Daemon intercepta o "Item String Block" do PoE, realiza o parse do ilvl, keywords (Tier, fractured, implicit) e lança no prompt se aquele item vale a pena receber investimento ou se é *Skip/Reroll*.
