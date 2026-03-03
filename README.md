```markdown
# ⚔️ Hideout Warrior CLI

![Python Version](https://img.shields.io/badge/python-3.10%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)
![Status](https://img.shields.io/badge/status-Active-success)

O **Hideout Warrior CLI** é uma ferramenta avançada de terminal (TUI) desenhada para jogadores de *Path of Exile* que operam na economia de *endgame*. Não é apenas uma calculadora de *crafting*; é um motor de arbitragem de mercado alimentado por Inteligência Artificial (XGBoost) e algoritmos de procura de caminhos (A*).

Construído para correr de forma nativa e leve num painel de `tmux` ou no teu terminal favorito, o Hideout Warrior avalia o *Expected Value* (EV) de itens e descobre rotas de lucro que passam despercebidas ao jogador comum.

---

## ✨ Funcionalidades Principais

* 🧠 **Oráculo de Machine Learning (XGBoost):** Avalia a sinergia de *tags* e *Tiers* (T1/T2) de um item para prever o seu valor real em *Chaos/Divines*, mesmo para itens mutantes gerados por *Recombinators* que não existem no mercado.
* 🗺️ **Motor de Procura A* (A-Star):** Calcula a rota matemática exata e mais barata para o *crafting* perfeito (Chaos, Fossils, Harvest, Recombinators), utilizando os *weights* reais extraídos do *Craft of Exile/RePoE*.
* 👁️ **Integração Clipboard (Sniper Passivo):** Fica à escuta em *background*. Dá `Ctrl+C` num item no jogo ou no site de *Trade*, e a CLI cospe instantaneamente a margem de lucro e a rota de *craft* no terminal.
* 📡 **Scanner de Mercado Sob Demanda:** Conecta-se à API Oficial de Trade, respeitando os *Rate Limits* (Circuit Breaker integrado), e varre dezenas de itens procurando discrepâncias de preço (Arbitragem).
* 🎨 **Interface Rica no Terminal:** Utiliza a biblioteca `rich` para desenhar tabelas, barras de progresso e *spinners* elegantes e responsivos.

---

## 🏗️ Arquitetura (Under the Hood)

O projeto está dividido em domínios estritos:
- **Data Layer:** `data_parser.py` e `api_integrator.py` gerem a ingestão e cache de dados da API do poe.ninja e da GGG.
- **Simulation Engine:** `evaluator.py` e `recombinators.py` gerem a manipulação determinística de estados imutáveis (`ItemState`).
- **Optimization Layer:** `graph_engine.py` (A*) e `ml_oracle.py` (XGBoost) trabalham em conjunto. A IA atua como uma heurística para podar ramos de decisão inúteis, evitando a explosão combinatória do A*.

---

## 🚀 Instalação e Setup

**Nota Importante:** O repositório contém apenas o código-fonte. Por motivos de arquitetura e precisão económica, o modelo de IA (`.xgb`) deve ser gerado localmente na tua máquina com os dados da liga atual.

1. **Clona o repositório:**
   ```bash
   git clone [https://github.com/teu-usuario/hideout-warrior-cli.git](https://github.com/teu-usuario/hideout-warrior-cli.git)
   cd hideout-warrior-cli

```

2. **Instala as dependências:**
```bash
pip install -r requirements.txt

```


3. **Gera o teu Cérebro Local (Treino do Oráculo):**
Antes de usar a ferramenta, precisas de treinar o modelo XGBoost. Este script vai extrair os preços atuais e gerar o ficheiro em `data/price_oracle.xgb`.
```bash
python scripts/train_oracle.py

```



---

## 🕹️ Como Usar

A ferramenta possui dois modos principais de operação:

### 1. Modo Passivo (Clipboard Watcher)

Ideal para teres num painel lateral enquanto jogas.

```bash
python cli.py

```

* **Ação:** Vai ao jogo ou ao site oficial de *Trade*, coloca o rato por cima de um item raro e prime `Ctrl+C`.
* **Resultado:** O terminal deteta o item, calcula as rotas e mostra-te uma tabela com o lucro esperado.

### 2. Modo Ativo (Market Scanner)

Ideal para arbitragem de mercado ("Flipping"). Procura discrepâncias entre o preço listado e o valor calculado pela nossa IA.

```bash
python cli.py scan --type "Imbued Wand" --ilvl 84 --rarity "rare"

```

* **Resultado:** A CLI faz uma chamada à API, analisa o lote de itens e devolve uma lista com os mais subfaturados, incluindo o comando de *whisper* já copiado para o teu rato.

---

## 🛡️ Conformidade TOS (Termos de Serviço)

O **Hideout Warrior CLI** é 100% *TOS-Compliant* com as regras da Grinding Gear Games (GGG).

* Não automatiza ações do cliente (sem *botting*).
* O Modo Passivo lê apenas a Área de Transferência do sistema operacional.
* O Modo Scanner utiliza a API REST pública, implementando pausas dinâmicas (*Exponential Backoff*) e respeitando rigorosamente os *headers* de *Rate Limit* para proteger o teu IP.

---

## 🗺️ Roadmap Futuro

* [ ] Implementação do **Rog Oracle** (Simulação da árvore de decisão de *crafting* de Expedition).
* [ ] Integração de Agente MCP para análise de meta/tendências em tempo real (Reddit/Fóruns).

---
