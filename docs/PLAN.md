# Hideout Warrior CLI - Plano de Implementação

## Visão Geral

Evoluir a base de dados e treino do Hideout Warrior para:
1. train_oracle readiness — consumir snapshots Gold em Parquet
2. reconciliação mais forte na Bronze — deduplicação e reconciliação multi-fonte
3. observabilidade do snapshot — métricas e relatórios operacionais

**Estado atual:** Sprint A (Bronze/Silver/Gold + metadata) funcional. Train oracle existente com quality gates.

---

## Bloco 1: train_oracle Readiness

### Objetivo
Garantir que `train_oracle.py` consuma dados do snapshot Parquet Gold de forma robusta.

### O que existe
- `fetch_training_data_from_parquet()` já existe em train_oracle.py
- `build_training_snapshot.py` já gera Gold particionado

### O que fazer
1. **Testar end-to-end**: validar que `train_oracle.py --source parquet` consome `data/training_snapshots/gold/` sem erros
2. **Fix path handling**: garantir que `fetch_training_data_from_parquet()` aceita diretório particionado (não só arquivo único)
3. **CLI default**: mudar default de `--source api` para `--source parquet` após validação
4. **Persistir metadata**: garantir que `persist_model_metadata()` salva hash do dataset e snapshot_date

### Arquivos
- `scripts/train_oracle.py` — ajustes em `fetch_training_data_from_parquet()`
- `tests/test_train_oracle.py` — novo teste de leitura de diretório particionado

### Verificação
- `python -m scripts.train_oracle train --source parquet` executa sem erro
- metadata JSON contém `dataset_hash` e `snapshot_date`

---

## Bloco 2: Reconciliação Mais Forte na Bronze

### Objetivo
Melhorar deduplicação e reconciliação de itens observados em múltiplas fontes (`stash` + `trade`).

### O que existe
- `_enrich_bronze_observations()` já calcula `seen_count`, `source_count`, `source=both`
- `event_key` baseado em `item_id + indexed + price_chaos`

### O que fazer
1. **Reforçar event_key**: adicionar `account_name` + `base_type` ao hash de event_key para reduzir falsos positivos
2. **Reconciliação por `item_id`**: itens com mesmo `item_id` e preços diferentes devem ser marcados como `price_fix_suspected`
3. **Tracking de preços**: manter lista de preços observados por `item_id` e detectar variações >50% como anomalias
4. **Persistir query_context**: garantir que `query_context` é preenchido corretamente para ambas as fontes

### Arquivos
- `scripts/build_training_snapshot.py` — ajustes em `_stable_event_key()`, `_enrich_bronze_observations()`
- `tests/test_training_snapshot_job.py` — atualizar testes de reconciliação

### Verificação
- Mesmo item em stash_events e trade_bucket_events resulta em `source=both`
- Variação de preço >50% no mesmo item gera `price_fix_suspected` flag

---

## Bloco 3: Observabilidade do Snapshot

### Objetivo
Emitir métricas operacionais por execução do snapshot.

### O que existe
- `core/ops_metrics.py` e `scripts/ops_report.py` existem
- `build_training_snapshot()` retorna summary dict

### O que fazer
1. **Métricas por camada**:
   - Bronze: `rows_read`, `rows_valid`, `rows_deduped`, `invalid_json`, taxa de deduplicação
   - Silver: `rows_input`, `rows_output`, `normalization_failures`
   - Gold: `rows_input`, `rows_output`, `feature_extraction_failures`
2. **Métricas de fonte**:
   - Distribuição de `source` (stash/trade/both)
   - Distribuição de `freshness_band`
3. **Emitir JSON de métricas**: `data/ops_metrics/snapshot_{date}.json`
4. **Expandir `ops_report.py`**: resumir métricas de snapshot +ops cycle

### Arquivos
- `scripts/build_training_snapshot.py` — retornar métricas detalhadas
- `scripts/ops_report.py` — incluir sección de snapshot metrics
- `core/ops_metrics.py` — adicionar funcs de agregação

### Verificação
- `python -m scripts.ops_report` mostra métricas de snapshot
- `data/ops_metrics/snapshot_*.json` existe após execução

---

## Dependências

| Bloco | Depende de |
|-------|------------|
| 1. train_oracle readiness | Bloco 3 (ops_metrics) |
| 2. Reconciliação Bronze | — |
| 3. Observabilidade | — |

**Ordem de execução:** 3 → 2 → 1 (Bloco 3 não tem dependências e fornece base de métricas para Bloco 1)

---

## Riscos

1. **Parquet particionado**: `pd.read_parquet()` com diretório pode ter comportamento variável entre engines
2. **Reconciliação agressiva**: pode rejeitar itens válidos se event_key for muito restritivo
3. **Performance**: métricas pesadas em dataset grande podem impactar tempo de snapshot

---

## Recomendação Antes de Implementar

**Aprovar a ordem de execução (3 → 2 → 1) e validar:**
- Que `ops_report.py` atual existe e tem estrutura extensível para métricas de snapshot
- Que não há breaking changes planejadas para `item_normalizer.py` ou `train_oracle.py` que impactem os ajustes

---

*Plano criado: 2026-03-24*
