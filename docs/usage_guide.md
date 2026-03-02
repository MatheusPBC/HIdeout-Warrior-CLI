# Hideout Warrior CLI - Guia de Uso

Este documento detalha o passo-a-passo para operar a CLI "Hideout Warrior" e os comandos disponíveis.

## Instalação Rápida

Certifique-se de estar rodando **Python 3.10+**.
Na pasta raiz do repositório `/hideout_warrior`, instale as dependências:

```bash
pip install -r requirements.txt
```

---

## Comandos Disponíveis (CLI Reference)

O sistema foi montado utilizando a biblioteca `Typer`. Você sempre pode ver os comandos rodando:

```bash
python cli.py --help
```

### 1. Rescue Snipe (Hospital de Itens)

**Comando:** `python cli.py rescue-snipe --budget <valor>`

**Descrição:** Varre a API de trade da GGG buscando por itens "brickados" (com afixos lixo prendendo algo valioso) que estejam sendo listados por um preço menor que o custo puro da base e que possuam reparação base-determinística (ex: salvar um prefixo com Eldritch Annuls).

**Exemplo de Uso:**

```bash
python cli.py rescue-snipe --budget 5.0
```

> O sistema rodará, usará a classe Integrator da API, detectará bons flips e copiará o whisper da compra automatizado imediatamente para o seu "Ctrl+C" (Clipboard). Você deverá apenas entrar in-game e dar Ctrl+V no chat para comprar.

### 2. Crafting Path & Recombinators Engine

**Comando:** `python cli.py craft-path --target <seu_json_file> [--allow-recombinators]`

**Descrição:** Avalia qual é o caminho ótimo absoluto pra craftar um determinado item modelando todas os processos de crafting através de um algoritmo de A* (Grafos) com o motor `networkx`.

**Flags Extras:** Se usar `--allow-recombinators`, o sistema passará as stats pelo módulo do Numpy e calculará as teóricas chances do Pool dos recombinadores e comparará o resultado pra te dizer se sai mais barato misturar dois iten lixos que possuem o mod no banco dos Recombinators ou se é melhor usar Essences.

**Exemplo de Uso:**

```bash
python cli.py craft-path --target my_dream_item.json --allow-recombinators
```

### 3. Meta-Sync Oracle

**Comando:** `python cli.py meta-sync`

**Descrição:** Sincronização essencial que deve ser rodada de tempo em tempo. Ela aciona o Scraper do `poe.ninja` para pegar estatísticas on-the-fly de quais habilidades e mods estão liderando o servidor de Experiência e constrói dinamicamente um arquivo `current_meta_weights.json`. Seus bots de Sniping (Comando 1) irão obedecer o que esse Json achar mais relevante.

**Exemplo de Uso:**

```bash
python cli.py meta-sync
```

### 4. Rog Assist Daemon (Expedition Monitor)

**Comando:** `python cli.py rog-assist`

**Descrição:** Inicia o processo de *Daemon* (serviço bloqueante). Ele rodará no terminal até ser cancelado com `Ctrl+C` (no terminal).
Durante o seu funcionamento, você deve ir pro lado do jogo. Sempre que abrir a tela do vendedor e não souber avaliar o item que o **NPC Rog** te ofereceu, basta dar um `Ctrl+C` no item DENTRO DO JOGO.
O daemon vai interceptar esse texto instataneamente no OS, rodar o parsing dos weights e te dizer no outro monitor se você deve pular aquilo ou apertar algum upgrade do npc.

**Exemplo de Uso:**

```bash
python cli.py rog-assist
```
