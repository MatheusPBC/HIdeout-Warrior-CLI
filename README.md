# Hideout Warrior CLI

Hideout Warrior CLI e uma ferramenta de terminal para `Path of Exile` focada em:

- detectar oportunidades de arbitragem na Trade API;
- gerar plano de flip com custo esperado e risco controlado;
- operar um pipeline offline de dados -> snapshot -> treino -> registry.

O projeto permanece **TOS-safe**: somente APIs publicas, sem automacao de clique e sem injecao no cliente do jogo.

## Estado atual (2026-03)

- Scanner hibrido com **Stage A (macro/micro sweep)** + **Stage B (consenso ML + mercado)**.
- Normalizacao `tier-first` com protecao de plausibilidade por `ilvl`.
- Bloqueios para outlier low ilvl/high ticket e cap de valor em fallback com pouca evidencia.
- Explicabilidade de valuation por item via `valuation_explanation`.
- Pipeline offline completo:
  - `firehose_miner` (stash stream -> SQLite)
  - `trade_bucket_collector` (amostragem estratificada por preco)
  - `build_training_snapshot` (Bronze/Silver/Gold parquet)
  - `train_oracle` (qualidade, treino por familia + bandas de ilvl, registry)
  - `ops_cycle` / `ops_report` (orquestracao e observabilidade)

## Setup

### Linux/macOS

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Windows (PowerShell)

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Comandos de produto (CLI)

### `scan`

Busca oportunidades de arbitragem.

```bash
python cli.py scan [opcoes]
```

Opcoes principais:

- `--type`: base especifica (ex.: `Imbued Wand`).
  - sem `--type`: perfil `open_market` (mais conservador).
  - com `--type`: perfil `targeted` (mais permissivo para explorar uma base).
- `--ilvl`: ilvl minimo de busca.
- `--rarity`: raridade (`rare`, `unique`, ...).
- `--max-items`: maximo de oportunidades retornadas.
- `--stale-hours`: limite de idade de listing para flags/filtros de stale.
- `-l, --league`: liga (`auto` por padrao).
- `--min-profit`: lucro minimo em chaos.
- `--min-listed-price`: preco minimo listado em chaos.
- `--anti-fix/--no-anti-fix`: liga/desliga filtro de price-fixing.
- `--safe-buy/--no-safe-buy`: modo super conservador.
- `--format`: `table|json|csv|jsonl`.
- `--full`: tabela detalhada (inclui coluna `Why` com `valuation_explanation`).
- `--output`: salva saida em arquivo.

Exemplos:

```bash
python cli.py scan -l Mirage --max-items 30 --format table
python cli.py scan --type "Imbued Wand" --ilvl 84 --min-profit 20 --safe-buy
python cli.py scan -l Mirage --format json --output data/scan_mirage_latest.json
```

### `flip-plan`

Gera plano economico de flip em cima das oportunidades do scanner.

```bash
python cli.py flip-plan [opcoes]
```

Opcoes principais:

- herda filtros de busca (`--type`, `--ilvl`, `--min-profit`, `--min-listed-price`, `--anti-fix`, `--safe-buy`, `--stale-hours`, `--league`);
- `--budget`: teto de custo de craft;
- `--top`: quantidade de planos.
- `--format`: `table|json`.
- `--output`: salva relatorio JSON.

Exemplo:

```bash
python cli.py flip-plan --type "Imbued Wand" --budget 150 --top 3 --format table
```

### Outros comandos da CLI

- `craft-path` (modo legado de rota de craft via clipboard, mantido por compatibilidade).
- `meta-sync` (placeholder).
- `rescue-snipe` (placeholder).
- `rog-assist` (placeholder para evolucao futura).

## Como os filtros e estagios funcionam

### Stage A (coleta e pre-filtro)

- Macro sweep segmentado por buckets de preco e ilvl.
- Micro queries para bases prioritarias.
- Budget por ciclo (`query`/`fetch`) + rotacao deterministica de segmentos.
- Dedupe TTL em memoria para nao reprocessar item repetido no curto prazo.
- Descarte precoce de `fractured_low_ilvl_brick`.

### Stage B (consenso)

Uma oportunidade so passa se houver consenso entre sinal de ML e contexto de mercado.
Regras importantes atuais:

- bloqueia outlier em `low_ilvl_context` sem `twink_override`;
- bloqueia high ticket low ilvl sem override;
- endurece `family_fallback` com `comparables_count < 3`;
- aplica cap de `ml_value` em baixa evidencia de mercado;
- recalcula score com penalidades de risco.

## Risk flags e interpretacao

Flags comuns em `risk_flags`:

- `price_fix_suspected`: listing velho, muito barato e gap extremo.
- `stale_listing`: anuncio antigo.
- `low_confidence`: confianca baixa do valuation.
- `high_ticket_low_confidence`: ticket alto com confianca insuficiente.
- `cheap_listing`: preco muito baixo.
- `corrupted`, `fractured`, `influenced`: estado do item.
- `family_fallback`: sem modelo forte carregado para o caso.
- `low_ilvl_context`: ilvl baixo para o contexto/familia.
- `tier_ilvl_mismatch`: tier detectado nao plausivel para ilvl.
- `fallback_low_evidence`: fallback com poucos comparaveis.

## Tags, tokens e features

### `item_family`

Classificacao principal usada por scanner/ML:

- `wand_caster`
- `body_armour_defense`
- `jewel_cluster`
- `accessory_generic`
- `generic`

### `mod_tokens`

Tokens canonicos extraidos de mods (ex.: `SpellDamage`, `CastSpeed`, `Life`, `Resist`).
Quando tier nativo existe e e plausivel, vira token como `SpellDamage_T1`.
Quando tier nativo conflita com ilvl/contexto, vira `SpellDamage_T1_approx` + flag `tier_ilvl_mismatch`.

### `tag_tokens`

Tags semanticas para contexto (ex.: `wand`, `caster`, `spell`, `life`, `resistance`, `crit`, etc.).
Ajudam na classificacao de familia e sinais secundarios.

### `numeric_mod_features`

Features continuas que reduzem dependencia de heuristica textual:

- `spell_damage_pct`
- `cast_speed_pct`
- `spell_crit_pct`
- `life_flat`
- `resist_total`
- `plus_all_spell_gems`

## Explicabilidade (`valuation_explanation`)

Cada oportunidade pode trazer uma explicacao textual com:

- valor ML vs preco listado;
- comparaveis de mercado (floor/mediana/spread/posicao);
- flags de risco;
- dados de plausibilidade (`tier_source`, mismatch, override);
- cap aplicado (`ml_value_before_cap` -> `ml_value_after_cap`);
- decisao final do consenso (`aprovado` ou `bloqueado` + motivo).

## Pipeline offline (dados e treino)

### 1) Miner de Public Stash -> SQLite

```bash
python scripts/firehose_miner.py run \
  --db-path data/firehose.db \
  --max-pages 100 \
  --sleep-seconds 1.5 \
  --oauth-token "$POE_OAUTH_TOKEN"
```

Notas:

- usa endpoint `api.pathofexile.com`;
- requer token OAuth valido com escopo `service:psapi`;
- salva em `stash_events` + checkpoint em `miner_checkpoint`.

### 2) Coleta Trade API estratificada por bucket

```bash
python scripts/trade_bucket_collector.py \
  --db-path data/firehose.db \
  --league Mirage \
  --max-items-per-bucket 30 \
  --max-searches-per-run 60 \
  --max-fetches-per-run 300
```

- grava em `trade_bucket_events` com dedupe de eventos.

### 3) Snapshot Bronze/Silver/Gold

```bash
python scripts/build_training_snapshot.py build \
  --db-path data/firehose.db \
  --output-dir data/training_snapshots
```

- Bronze: raw unificado + dedupe de evento.
- Silver: item normalizado.
- Gold: features de treino.

### 4) Treino e registry

```bash
python scripts/train_oracle.py train \
  --source parquet \
  --parquet-path data/training_snapshots/gold \
  --league Mirage \
  --promotion-max-rmse-ratio 1.0 \
  --promotion-min-abs-improvement 0.0 \
  --registry-path data/model_registry/registry.json
```

- quality gates de dataset antes do treino;
- treino por familia e por banda de ilvl (`low`, `mid`, `high`) com fallback controlado;
- registra candidato e promocao no model registry.

### 5) Ciclo operacional completo

```bash
python scripts/ops_cycle.py run \
  --db-path data/firehose.db \
  --league Mirage \
  --train-source parquet \
  --snapshot-output-dir data/training_snapshots \
  --oauth-token "$POE_OAUTH_TOKEN"
```

- orquestra miner -> snapshot -> treino;
- emite metrica operacional por etapa.

### 6) Relatorio operacional

```bash
python scripts/ops_report.py build \
  --metrics-dir data/ops_metrics \
  --registry-path data/model_registry/registry.json
```

- consolida erros, latencias e estado ativo do registry.

## Estrutura de saidas e artefatos

- `data/firehose.db`: base SQLite operacional.
- `data/training_snapshots/`: camadas Bronze/Silver/Gold em parquet.
- `data/model_registry/registry.json`: estado de candidatos/ativos por familia.
- `data/model_metadata/`: metadata de runs de treino.
- `data/ops_metrics/*.jsonl`: eventos operacionais.
- `data/ops_reports/*.json`: consolidado operacional.
- `data/scan_*.json`: capturas de scans reais.

## Testes

```bash
pytest -q
```

Suite atual validada com **86 passed**.
