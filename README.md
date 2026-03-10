# Hideout Warrior CLI

Hideout Warrior CLI e uma ferramenta de terminal para `Path of Exile` focada em dois problemas economicos:

- achar itens subprecificados no mercado;
- sugerir flips de item com um plano de craft economicamente viavel.

O projeto continua `TOS-safe`: usa apenas APIs publicas e analise local. Nao automatiza clique nem injeta nada no cliente do jogo.

## Fluxos principais

### 1. `scan`
Busca itens na trade API, resolve a liga automaticamente por padrao, calcula valuation, confianca, lucro, `trusted_profit` e score, e devolve oportunidades ranqueadas.

Sem `--type`, o scan entra em modo aberto e prioriza confianca: listings baratos e pouco confiaveis sao filtrados antes do ranking.

Com `--type`, o scan fica mais permissivo para explorar bases especificas.

Exemplo:

```bash
python cli.py scan --type "Imbued Wand" --ilvl 84 --min-profit 20
```

Modo aberto:

```bash
python cli.py scan -l Mirage --max-items 20
```

### 2. `flip-plan`
Consome oportunidades do scanner, escolhe candidatas promissoras e monta um relatorio detalhado com alvo recomendado, passos de craft, custo esperado, valor esperado e lucro liquido.

Exemplo:

```bash
python cli.py flip-plan --type "Imbued Wand" --budget 150 --top 3
```

## Setup

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## Formatos de saida

`scan` suporta:

- `table`
- `json`
- `csv`
- `jsonl`

`flip-plan` suporta:

- `table`
- `json`

## Roadmap imediato

- scanner confiavel com score e flags de risco;
- flip advisor guiado por heuristica;
- ligas com auto-resolucao + override manual.

## Futuro

- Rog Oracle reaproveitando o mesmo valuation/planner;
- expansao do catalogo de crafts;
- modelagem mais profunda de recombinator e rotas complexas.
