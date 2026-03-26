# Hideout Warrior CLI - Plano de Migração Supabase — Fechamento

## Visão Geral

Migração local-first → Supabase cloud. Estado atual: **Fase 2 concluída** (2026-03-25).
Este plano cobre apenas o fechamento final.

---

## Status: Blocos Anteriores ✅ Concluídos

| Bloco | Descrição | Status |
|-------|-----------|--------|
| 1 | Checkpoint/State Migration (SQLite → Postgres) | ✅ Concluído |
| 2 | Raw Ingest Landing (NDJSON → Storage + manifest) | ✅ Concluído |
| 3 | Runtime Cloud-Aware (default=supabase, --cloud flags) | ✅ Concluído |

Entregas verificadas: 226 testes passed, py_compile limpo.

---

## Bloco A: Alinhamento Docs / Estado Atual

### Objetivo
Sincronizar `docs/PLAN.md` com o estado real já entregue. É o que este documento faz.

### Ações
- [ ] Marcar Blocos 1-3 como ✅ Concluídos (este arquivo)
- [ ] Confirmar que `CHANGELOG.md` reflete todos os deliverables da Fase 2

### Verificação
- `docs/PLAN.md` versões anteriores não contradizem o estado atual

---

## Bloco B: Validação Remota — Schema + Health/Bootstrap

### Objetivo
Confirmar que o Supabase remoto tem schema e bootstrap funcionais.

### Ações
1. **`scripts/supabase_health_check.py`** — executar contra o projeto remoto real
   - Verificar conectividade, tabelas acessíveis, storage buckets
2. **`scripts/bootstrap_supabase.py --dry-run`** — confirmar que não há erros de schema
3. **`supabase/schema.sql` vs. remoto** — validar que todas as tabelas/índices existem com colunas corretas:
   - `artifact_catalog`, `active_models`, `snapshot_runs`, `firehose_checkpoints`, `firehose_raw_manifest`
   - Colunas de governança: `checksum_validated`, `last_verified_at`, `retention_expires_at`, `deleted_at`

### Verificação
```bash
python -m scripts.supabase_health_check
python -m scripts.bootstrap_supabase --dry-run
```
- Ambos retornam **exit 0** sem erros

---

## Bloco C: Verificação Operacional Final + Decisão de Encerramento

### Objetivo
Confirmar que o pipeline operacional funciona end-to-end com Supabase cloud e declarar migração como consolidada.

### Ações
1. **Checkpoint cloud-readiness**: `python -m scripts.firehose_miner run --max-pages 1` persiste em `firehose_checkpoints` remoto
2. **Raw landing**: `data/firehose_raw/` recebe NDJSON por página
3. **Upload operacional**: `python -m scripts.firehose_to_supabase --dry-run` lista uploads sem erro
4. **Retention dry-run**: `python -m scripts.retention_policy --dry-run` executa sem erro

### Critério de Encerramento (Migração Consolidadas)

**A migração será declarada consolidada quando TODOS os itens abaixo forem verdadeiros:**

| # | Critério | Como verificar |
|---|----------|----------------|
| 1 | `supabase_health_check` retorna OK | `python -m scripts.supabase_health_check` exit 0 |
| 2 | `bootstrap_supabase --dry-run` sem erros | exit 0, sem diff de schema |
| 3 | Suite de testes passa (226+) | `pytest -q` |
| 4 | Checkpoint persiste no Supabase (não só SQLite) | Query `firehose_checkpoints` após miner run |
| 5 | Raw landing NDJSON existe após páginas | `ls data/firehose_raw/` não vazio |

### Risco Residual Curto

| Risco | Probabilidade | Impacto | Mitigação |
|-------|---------------|---------|-----------|
| Schema drift entre local `schema.sql` e remoto | Baixa | Média | Bloco B cobre isso; revisar antes de cada deploy |
| Retention policy não executar em produção | Baixa | Baixa | `cleanup_firehose_raw.py` + `retention_policy.py` prontos; agendar via cron/CI |

---

*Plano atualizado para fechamento: 2026-03-26*
