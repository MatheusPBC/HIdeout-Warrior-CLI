# Hideout Warrior CLI - Plano de Migração Supabase Full Cloud

## Visão Geral

Migrar o estado operacional de local-first (SQLite `data/firehose.db`) para Supabase cloud, mantendo compatibilidade local durante a transição.

**Arquitetura alvo:**
- **Postgres/Supabase** = checkpoint, catalog, registry, runs, metadata
- **Supabase Storage** = raw dumps, snapshots parquet, models, metrics, reports
- **Evitar** Postgres como data lake bruto

**Supabase dump land (NDJSON)** → Postgres (checkpoint/catalog) → Storage (arquivos grandes)

---

## Bloco 1: Checkpoint/State Migration

### Objetivo
Migrar `firehose_checkpoints` e `load_checkpoint` para Postgres como fonte oficial, eliminando dependência do `miner_checkpoint` local em SQLite.

### O que existe
- `firehose_checkpoints` table já existe em `supabase/schema.sql`
- `sync_firehose_checkpoint_to_supabase()` em `core/supabase_cloud.py` já faz upsert
- `update_checkpoint()` em `firehose_miner.py` chama sync com `except: pass` (silencioso)
- `load_checkpoint()` lê apenas do SQLite local

### O que fazer
1. **Criar `load_checkpoint_from_supabase()`** em `core/supabase_cloud.py` — lê `next_change_id` da tabela `firehose_checkpoints` via `sync_firehose_checkpoint_to_supabase`
2. **Atualizar `load_checkpoint()`** em `scripts/firehose_miner.py` — fallback: Postgres → SQLite
3. **Tornar sync obrigatório** em `update_checkpoint()` — remover `except: pass`; se Supabase falhar, logar warning mas não parar
4. **Criar script de backfill** — sincroniza checkpoint atual do SQLite para Postgres uma única vez

### Arquivos
- `core/supabase_cloud.py` — adicionar `load_checkpoint_from_supabase()`
- `scripts/firehose_miner.py` — atualizar `load_checkpoint()`, `update_checkpoint()`
- `scripts/backfill_firehose_checkpoint.py` — script one-shot SQLite → Postgres

### Verificação
- `python -m scripts.firehose_miner run --max-pages 1` executa e persiste checkpoint no Postgres
- `load_checkpoint()` retorna valor mesmo que `data/firehose.db` seja deletado

### Riscos
- Postgres indisponível durante execução → fallback para SQLite (deve funcionar sem quebrar)
- Conflito de checkpoint se duas instâncias rodarem simultaneamente (requer `ON CONFLICT`)

---

## Bloco 2: Raw Ingest Landing Strategy

### Objetivo
Evitar que Postgres vire data lake. Raw items do firehose vão para Supabase Storage como NDJSON, não para tabelas Postgres.

### O que existe
- Firehose currently dumps raw items to local SQLite `stash_events` table (milhares de rows por página)
- `data/firehose_raw/` não existe ainda — sem landing zone

### O que fazer
1. **Criar landing buffer NDJSON** — modificar `ingest_stash_page()` para também escrever cada page payload em `data/firehose_raw/{date}/{page_change_id}.ndjson`
2. **Criar `scripts/firehose_to_supabase.py`** — consume `data/firehose_raw/`, faz upload para Supabase Storage bucket `firehose-raw`, apaga local após upload confirmado
3. **Manter Postgres apenas para checkpoint/catalog** — não fazer INSERT de items no Postgres, apenas no SQLite local (para query ad-hoc) E no NDJSON landing
4. **Table `firehose_raw_manifest`** (Postgres) — cataloga cada NDJSON dump: `{run_id, object_path, rows_count, uploaded_at}`

### Arquivos
- `scripts/firehose_miner.py` — alterar `ingest_stash_page()` para escrever NDJSON
- `scripts/firehose_to_supabase.py` — upload + manifest + cleanup
- `supabase/schema.sql` — adicionar tabela `firehose_raw_manifest`
- `core/cloud_config.py` — adicionar `firehose_raw_manifest_table`

### Verificação
- Após 10 páginas, `data/firehose_raw/` contém 10 arquivos NDJSON
- `python -m scripts.firehose_to_supabase --dry-run` lista uploads sem executar
- `firehose_raw_manifest` no Postgres tem registro de cada upload

### Riscos
- Disco local lota se `firehose_to_supabase` falhar por longos períodos — agendar cleanup ou tamanho máximo do buffer
- Ordem de ingest não garantida em uploads paralelos — resolver com page_change_id no nome do arquivo

---

## Bloco 3: Command/Runtime Migration

### Objetivo
Tornar Supabase o backend default e garantir que todos os scripts operacionais funcionam com `HW_CLOUD_BACKEND=supabase`.

### O que existe
- `core/cloud_config.py` já suporta `HW_CLOUD_BACKEND=supabase`
- `sync_file_to_supabase()`, `sync_directory_to_supabase()` — upload para Storage
- `supabase_sync.py` — CLI para sync manual de artefatos
- `sync_snapshot_summary_to_supabase()`, `sync_registry_state_to_supabase()` — metadata

### O que fazer
1. **Atualizar `build_training_snapshot.py`** — fazer upload automático do snapshot Gold Parquet para Supabase Storage ao final da execução
2. **Atualizar `train_oracle.py`** — adicionar flag `--cloud` que baixa snapshots de Supabase Storage se `HW_CLOUD_BACKEND=supabase`
3. **Atualizar `model_registry.py`** — `sync_registry_state_to_supabase()` chamado automaticamente após mutations no registry
4. **Atualizar `ops_cycle.py`** — coletar métricas e fazer upload para Supabase ao final do ciclo
5. **Mudar default** — `HW_CLOUD_BACKEND` default para `supabase` (com `local` como fallback se `SUPABASE_URL` não estiver setado)
6. **Documentar** — criar `.env.supabase.example` com todas as vars necessárias

### Arquivos
- `scripts/build_training_snapshot.py` — adicionar `sync_directory_to_supabase()` após gerar Gold
- `scripts/train_oracle.py` — adicionar `--cloud` + download de snapshots do Storage
- `scripts/model_registry.py` — chamar `sync_registry_state_to_supabase()` após upsert/delete
- `scripts/ops_cycle.py` — upload de métricas ao final do ciclo
- `core/cloud_config.py` — inverter default para `supabase`
- `.env.supabase.example` — template de variáveis

### Verificação
- `HW_CLOUD_BACKEND=supabase python -m scripts.build_training_snapshot` faz upload do snapshot
- `HW_CLOUD_BACKEND=supabase python -m scripts.train_oracle train --cloud` baixa e consome do Storage
- Suite local passa com `HW_CLOUD_BACKEND=local` E com `HW_CLOUD_BACKEND=supabase`

### Riscos
- Train oracle sem connectivity não funciona em cloud mode — needs graceful fallback para local
- Storage costs crescem com snapshots frequentes — implementar retention policy

---

## Ordem de Execução

| Bloco | Depende de | Prioridade |
|-------|------------|------------|
| 1. Checkpoint/State | — | **P0** (mais crítico) |
| 2. Raw Ingest Landing | Bloco 1 | P1 |
| 3. Command/Runtime | Blocos 1 + 2 | P2 |

---

## Riscos Globais

1. **Postgres como gargalo** — se firehose miner escrever checkpoint no Postgres a cada page, latency pode impactar throughput
2. **Storage cost** — snapshots Parquet + raw NDJSON crescem rápido; retention policy é essencial
3. **Dual write complexity** — manter SQLite + Postgres durante transição duplica estado; Bloco 3 deve consolidar
4. **Auth/env vars** — `SUPABASE_SERVICE_ROLE_KEY` no CI/local; nunca commitar

---

## Recomendação Antes de Implementar

**Validar que o schema atual do `supabase/schema.sql` está correto no Supabase remoto** (tabelas criadas e com permissões adequadas para service_role). Sem isso, Bloco 1 vai falhar em produção.

---

*Plano criado: 2026-03-25*
