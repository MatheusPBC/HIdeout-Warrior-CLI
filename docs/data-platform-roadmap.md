# Hideout Warrior CLI - Plano de Evolucao de Dados, Coleta e Uso

## Objetivo

Evoluir a base do projeto de um scanner/treino funcional para uma plataforma de inteligencia de mercado mais confiavel, com melhor cobertura, melhor qualidade de dados, labels mais realistas, modelos mais robustos e uso operacional mais inteligente do scanner.

## Principios

- priorizar qualidade de dado antes de aumentar volume;
- manter rastreabilidade por fonte (`stash`, `trade`, `both`);
- separar claramente dado bruto, dado enriquecido e dado de treino;
- usar regras conservadoras quando a evidencia de mercado for fraca;
- medir cobertura, qualidade e deriva operacionalmente;
- evoluir em fases pequenas, testaveis e revertiveis.

## Estado atual

- `scripts/firehose_miner.py` ja coleta `Public Stash API` com OAuth de servico.
- `scripts/trade_bucket_collector.py` ja coleta amostras da Trade API por buckets de preco.
- `scripts/build_training_snapshot.py` ja materializa camadas Bronze/Silver/Gold.
- `scripts/train_oracle.py` ja aplica quality gates, filtros de stale/outlier e treino por `item_family` + `ilvl_band`.
- `core/market_scanner.py` ja usa Stage A/Stage B, contexto de mercado, explicabilidade e hardening de low ilvl.
- `core/ops_metrics.py` e `scripts/ops_report.py` ja fornecem observabilidade operacional basica.

## Fase 0 - Contrato de dados e arquitetura

### Meta

Definir o contrato de observacao de listing e as fronteiras entre dado bruto, enriquecido e dado de treino.

### Escopo

- definir schema canonico para observacoes:
  - `item_id`
  - `league`
  - `source`
  - `collected_at`
  - `indexed_at`
  - `first_seen_at`
  - `last_seen_at`
  - `seen_count`
  - `listing_age_seconds`
  - `price_amount`
  - `price_currency`
  - `price_chaos`
  - `raw_item_json`
  - `query_context`
- definir taxonomia de qualidade e risco:
  - `fresh_listing`
  - `stale_listing`
  - `price_fix_suspected`
  - `low_evidence`
  - `low_ilvl_context`
  - `tier_ilvl_mismatch`
- documentar o fluxo `raw -> bronze -> silver -> gold -> model -> scanner`.

### Arquivos alvo

- `docs/architecture.md`
- `scripts/build_training_snapshot.py`
- `scripts/train_oracle.py`

### Criterios de aceite

- qualquer amostra pode ser rastreada por fonte, idade e evidencias;
- o schema fica claro o suficiente para evolucoes futuras sem reprocessamento ambiguo.

## Fase 1 - Coleta e fundacao de dados

### Meta

Transformar `stash + trade` em uma base unificada de observacoes de mercado.

### 1.1 Firehose enriquecido

- adicionar ou calcular:
  - `collected_at`
  - `first_seen_at`
  - `last_seen_at`
  - `seen_count`
  - `oauth_source`
  - `oauth_scope`
- preservar `account_name`, `stash_name` e `indexed` de forma consistente.

### 1.2 Trade bucket collector enriquecido

- guardar metadados operacionais:
  - `scan_profile`
  - `query_shape`
  - `bucket_label`
  - `listing_age_seconds`
  - `search_batch` e `fetch_batch`
- manter deduplicacao forte por janela temporal e item.

### 1.3 Camada unificada de observacoes

- consolidar `stash_events` e `trade_bucket_events` no Bronze com colunas canonicas;
- promover itens observados nas duas fontes para `source=both`;
- materializar `freshness_band` e `source_count`.

### Arquivos alvo

- `scripts/firehose_miner.py`
- `scripts/trade_bucket_collector.py`
- `scripts/build_training_snapshot.py`
- testes correspondentes

### Criterios de aceite

- Bronze deixa de ser apenas uniao simples de tabelas;
- a mesma entidade observada em fontes diferentes pode ser reconciliada.

## Fase 2 - Qualidade dos dados

### Meta

Impedir que mais volume signifique mais ruido.

### Escopo

- criar `quality_score` por amostra;
- gerar `quality_flags` com base em:
  - stale age
  - baixa evidencia
  - low ilvl sem override
  - mismatch tier/ilvl
  - price fixing
  - spread extremo
- melhorar dedupe por `item_id + league + time window`;
- particoes operacionais adicionais:
  - `source`
  - `freshness_band`
  - `item_family`
  - `ilvl_band`

### Arquivos alvo

- `scripts/build_training_snapshot.py`
- `scripts/train_oracle.py`
- `core/item_normalizer.py`
- testes de snapshot/treino

### Criterios de aceite

- cada linha do Gold possui origem e qualidade explicitas;
- o pipeline consegue filtrar ou ponderar exemplos ruins sem apagar rastreabilidade.

## Fase 3 - Labels melhores

### Meta

Treinar valor de mercado e liquidez, nao apenas preco pedido.

### Escopo

- introduzir labels derivados:
  - `fair_value`
  - `quick_sale_value`
  - `high_confidence_value`
- usar mistura controlada entre:
  - `market_floor`
  - `market_median`
  - `market_spread`
  - `comparables_count`
  - `listing_age_seconds`
  - `quality_score`
- definir politica para baixa evidencia:
  - cap conservador;
  - ou exclusao do treino principal.

### Arquivos alvo

- `scripts/train_oracle.py`
- `core/ml_oracle.py`
- possivel helper novo em `core/`

### Criterios de aceite

- `price_chaos` deixa de ser o unico target relevante;
- metadata de treino explicita qual label foi usada.

## Fase 4 - Modelo e treino

### Meta

Usar a base nova para reduzir extrapolacao e melhorar confianca.

### Escopo

- adicionar features novas:
  - `listing_age_seconds`
  - `freshness_band`
  - `source`
  - `source_count`
  - `seen_count`
  - `quality_score`
  - `market_floor`
  - `market_median`
  - `market_spread`
  - `comparables_count`
- fortalecer fallback hierarquico:
  - `family + ilvl_band + league`
  - `family + ilvl_band`
  - `family`
  - `generic`
- calibrar confianca quando houver:
  - fallback;
  - cobertura baixa;
  - baixa qualidade.

### Arquivos alvo

- `scripts/train_oracle.py`
- `core/ml_oracle.py`
- `scripts/model_registry.py`
- testes de treino/oracle

### Criterios de aceite

- metadata reporta desempenho por banda, familia e bucket de preco;
- o modelo expõe claramente quando a predicao vem de fallback forte ou fraco.

## Fase 5 - Scanner e uso operacional

### Meta

Transformar as predições em decisoes operacionais melhores.

### Escopo

- introduzir modos de operacao:
  - `fresh_snipe`
  - `stable_arbitrage`
- enriquecer o scanner em runtime com historico de observacao:
  - `first_seen_at`
  - `seen_count`
  - `freshness_band`
  - `source`
- combinar no score final:
  - gap de valuation;
  - evidencia de mercado;
  - frescor;
  - qualidade da amostra;
  - risco;
  - confianca do modelo.
- classificar resultados em buckets de decisao:
  - `high_confidence`
  - `watchlist`
  - `experimental`

### Arquivos alvo

- `core/market_scanner.py`
- `core/flip_planner.py`
- `cli.py`
- testes do scanner/planner

### Criterios de aceite

- o scanner entrega oportunidade + classe de oportunidade + racional de evidencias;
- o output fica mais util para execucao manual rapida.

## Fase 6 - Observabilidade e governanca

### Meta

Medir cobertura, qualidade, degradacao e impacto das regras.

### Escopo

- emitir metricas adicionais por execucao:
  - ingestao por fonte
  - taxa de dedupe
  - cobertura por familia
  - cobertura por `ilvl_band`
  - distribuicao de `freshness_band`
  - porcentagem de `source=both`
  - porcentagem de fallback do modelo
  - itens bloqueados por regra
  - amostras com `native_tier_count > 0`
  - qualidade media do snapshot
- expandir `ops_report` com resumo de cobertura e qualidade;
- comparar snapshots consecutivos para sinais simples de drift.

### Arquivos alvo

- `core/ops_metrics.py`
- `scripts/ops_report.py`
- `scripts/ops_cycle.py`
- testes de observabilidade

### Criterios de aceite

- o report operacional mostra nao so duracao/erro, mas tambem confianca estrutural da base;
- fica facil detectar cegueira de cobertura ou degradacao silenciosa.

## Ordem recomendada

1. Fase 0 + Fase 1
2. Fase 2
3. Fase 3
4. Fase 4
5. Fase 5
6. Fase 6

## Dependencias

- Fase 2 depende da consolidacao da Fase 1.
- Fase 3 depende de Fase 2 para labels mais confiaveis.
- Fase 4 depende de Fase 2 e Fase 3.
- Fase 5 se beneficia muito de Fase 1 pronta.
- Fase 6 pode comecar cedo, mas ganha valor real apos Fase 2.

## Riscos principais

- misturar `stash` e `trade` sem manter identidade da fonte;
- aumentar volume sem melhorar a qualidade do label;
- endurecer demais o scanner e perder recall;
- inflar o schema cedo demais sem contrato claro;
- treinar modelos mais sofisticados em base ainda enviesada.

## Sprints sugeridos

### Sprint A - Fundacao de dados

- Fase 0
- Fase 1

### Sprint B - Qualidade e labels

- Fase 2
- Fase 3

### Sprint C - Modelo e scanner

- Fase 4
- Fase 5

### Sprint D - Governanca

- Fase 6

## Definicao de pronto por sprint

### Sprint A

- snapshots carregam `source`, `freshness_band`, `seen_count` e `listing_age_seconds`;
- o pipeline roda ponta a ponta sem quebrar contratos existentes.

### Sprint B

- Gold materializa `quality_score` e labels melhores;
- quality gates rejeitam datasets bons no motivo e ruins na hora certa.

### Sprint C

- metadata do treino mostra ganho real por familia/banda;
- scanner passa a diferenciar `fresh_snipe` de `stable_arbitrage`.

### Sprint D

- `ops_report` mostra cobertura, qualidade e fallback de forma operacional.

## Recomendacao imediata

Comecar pela Sprint A.

O maior ROI agora e consolidar a fundacao de dados, porque o acesso OAuth desbloqueou volume e frescor, mas o ganho real so aparece quando a base passa a preservar origem, idade, recorrencia e qualidade da observacao.
