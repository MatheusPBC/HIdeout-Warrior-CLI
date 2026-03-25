# Hideout Warrior CLI - OAuth Max Data Plan

## Goal

Usar o acesso OAuth de servico ao `Public Stash API` para empurrar a coleta e o treino o mais perto possivel do teto util de dados publicos, transformando o projeto numa fabrica continua de dados de mercado para scanner, planner e modelos.

## Core Idea

OAuth resolveu o gargalo de acesso ao firehose. O problema principal agora deixa de ser autenticacao e passa a ser:

- cobertura
- retencao historica
- qualidade da observacao
- labels melhores
- observabilidade da base

## O Que Ja Existe

- `core/poe_oauth.py` resolve token de servico por `client_credentials`
- `scripts/firehose_miner.py` coleta `Public Stash API` com OAuth e grava metadados de coleta
- `scripts/trade_bucket_collector.py` complementa a cobertura com amostragem estratificada da Trade API
- `scripts/build_training_snapshot.py` gera camadas `Bronze/Silver/Gold`
- `scripts/train_oracle.py` ja consome `Gold` em parquet
- `scripts/ops_cycle.py` e `scripts/ops_report.py` dao uma base operacional para pipeline continuo

## Strategic Goal

Sair de um scanner com dataset funcional para uma plataforma continua de inteligencia de mercado.

Resultados esperados:

- melhor cobertura temporal
- melhor qualidade de snapshots
- melhor treino por familia/faixa
- menos ruido de stale e price-fixing
- melhor confianca operacional no scanner e no planner

## Phase 1 - Continuous Firehose Collection

### Objective

Rodar o `firehose_miner` de forma continua ou quase continua para maximizar cobertura temporal.

### Why It Matters

O firehose com OAuth permite registrar melhor:

- `first_seen_at`
- `last_seen_at`
- `seen_count`
- recorrencia de listing
- envelhecimento de anuncios

### Recommended Output

- `stash_events` crescendo continuamente
- checkpoint consistente em `miner_checkpoint`
- throughput e falhas monitorados por metricas operacionais

## Phase 2 - Stratified Trade Coverage

### Objective

Nao depender so do firehose. Usar `trade_bucket_collector` para complementar nichos, faixas de preco e familias menos frequentes.

### Why It Matters

Sem essa camada, o dataset tende a ficar enviesado para:

- itens muito baratos
- familias mais comuns
- ruido do mercado aberto

### Recommended Output

- buckets recorrentes por liga
- reforco de cobertura para bases prioritarias
- melhor distribuicao por `item_family` e `price band`

## Phase 3 - Historical Retention

### Objective

Preservar historico suficiente para treino temporal, comparacao entre snapshots e analise de deriva.

### Keep Long Enough

- `stash_events`
- `trade_bucket_events`
- Bronze reconciliado
- snapshots Silver/Gold particionados por data

### Why It Matters

Sem historico, voce tem volume, mas nao tem contexto temporal.

## Phase 4 - Data Quality First

### Objective

Tornar cada observacao explicitamente qualificada antes de usa-la no treino.

### Quality Signals

Cada observacao idealmente deve carregar:

- `freshness_band`
- `source`
- `source_count`
- `seen_count`
- `price_fix_suspected`
- `low_evidence`
- `stale_listing`
- `tier_ilvl_mismatch`
- `quality_score`
- `quality_flags`

### Why It Matters

O ganho grande de precisao agora nao vem de mais auth; vem de reduzir ruido sem perder rastreabilidade.

## Phase 5 - Better Labels

### Objective

Parar de depender so de `listed price` como target bruto de treino.

### Labels To Introduce

- `fair_value`
- `quick_sale_value`
- `high_confidence_value`

### Inputs To Combine

- `market_floor`
- `market_median`
- `market_spread`
- `comparables_count`
- `listing_age_seconds`
- `quality_score`

### Why It Matters

OAuth aumenta volume e frescor, mas nao resolve sozinho o problema do label.

## Phase 6 - Gold As Official Training Source

### Objective

Consolidar o `Gold` como fonte oficial de treino.

### Recommended Flow

- coletar continuamente
- gerar snapshots
- treinar a partir do `Gold`
- registrar metadata e registry

### Why It Matters

O caminho principal deixa de ser treino direto da API e passa a ser treino em dataset consolidado, rastreavel e repetivel.

## Phase 7 - Coverage Observability

### Objective

Medir se a coleta esta realmente aumentando cobertura e nao apenas volume bruto.

### KPIs To Monitor

- ingestao por hora/dia
- distribuicao por `freshness_band`
- porcentagem de `source=both`
- cobertura por `item_family`
- cobertura por `ilvl_band`
- taxa de dedupe
- taxa de `price_fix_suspected`
- proporcao de `low_evidence`
- fallback rate do modelo

## Recommended Cadence

### Firehose

- continuo

### Trade Bucket Collector

- recorrente por janela fixa

### Snapshot Generation

- varias vezes por dia

### Training

- validacao diaria
- promocao conservadora via registry

### Reporting

- relatorio operacional diario

## Ideal Pipeline

```text
OAuth -> firehose_miner (continuo)
      -> trade_bucket_collector (amostragem estrategica)
      -> Bronze (reconciliacao)
      -> Silver (normalizacao)
      -> Gold (treino)
      -> train_oracle
      -> model_registry
      -> market_scanner / flip_planner
      -> ops_report
```

## Main Risks

- coletar muito dado barato e enviesado
- confundir volume com qualidade
- treinar em `listed price` sem corrigir o label
- ter firehose forte sem retencao historica suficiente
- nao medir cobertura por familia e por nicho

## Definition Of Success

A base passa a:

- crescer continuamente
- preservar historico
- medir qualidade explicitamente
- treinar sempre em snapshots consolidados
- melhorar a precisao do modelo de forma mensuravel

## Recommended Next Execution Order

1. fortalecer coleta continua do firehose com OAuth
2. ampliar cobertura estratificada via Trade API
3. fechar `quality_score` e `quality_flags`
4. melhorar labels de treino
5. consolidar `Gold` como fonte oficial de treino
6. expandir KPIs de cobertura e deriva
