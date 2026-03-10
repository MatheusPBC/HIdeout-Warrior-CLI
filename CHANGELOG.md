# CHANGELOG

## 2026-03-10

- Adicionado o filtro `--min-listed-price` nos comandos `scan` e `flip-plan`, com propagação para o scanner/planner e aplicação no fluxo de seleção de oportunidades.
- Incluídas novas métricas/KPIs relacionadas ao filtro de preço mínimo listado, incluindo contadores de itens filtrados (`filtered_min_listed_price`) e exibição no resumo/CLI.
- Endurecido o treino em `scripts/train_oracle.py` com melhorias anti-data-leakage: auditoria de duplicatas e overlap train/test, split temporal quando timestamp é confiável, e fallback controlado para split aleatório.
- Expandida a avaliação do treino com métricas adicionais: `MAE`, baseline por mediana de treino, `RMSE` de baseline e `RMSE` por buckets de preço.
- Aumentada a robustez em `core/ml_oracle.py` com resolução de path de modelo mais resiliente e consolidação de schema único de features para treino/inferência.
- Ajustado o score/risco em `core/market_scanner.py` com nova flag `high_ticket_low_confidence`, penalidades explícitas no score e comportamento `safe_buy` dinâmico por faixa de preço/confiança.
- Adicionados/atualizados testes em `tests/test_train_oracle.py`, `tests/test_ml_oracle.py` e `tests/test_market_scanner.py` para cobrir split temporal, métricas por bucket, schema de features, flags de risco, penalidades e filtro de preço mínimo.
- Validação de testes executados com `pytest -q` neste fluxo: **41 passed, 11 warnings, 0 falhas**.
- Refatorado `core/market_scanner.py` para arquitetura híbrida de ingestão (Macro Sweep + Micro Snipe), com execução assíncrona, pool de candidatos e deduplicação global com TTL por `item_id`.
- Implementado pipeline em 2 estágios no scanner: `Stage A` (filtro heurístico barato) e `Stage B` (avaliação ML + construção de oportunidade), reduzindo custo de avaliação em candidatos fracos.
- Adicionada valuation híbrida no scanner (`ml_value` + comparáveis de mercado por base/ilvl/sinais de mods), exigindo consenso mínimo antes de aprovar oportunidade.
- Aplicadas regras não-lineares de ticket para aprovação final: 20-50c (>=5c), 50-150c (>=12c), 150c+ (>=30c + confiança alta).
- Expandido `ScanStats` com métricas de qualidade/cobertura (`coverage_by_bucket`, `candidates_macro`, `candidates_micro`, `deduped`, `stage_a_passed`, `stage_b_passed`, `final_approval_rate`).
- Hotfix de produção no scanner híbrido: remoção de filtros inválidos da Trade API (`influence`/`fractured`) no payload principal, fallback defensivo para limpeza automática de filtros inválidos e prevenção de loop de erro 400.
- Hotfix de estabilidade no macro sweep: adicionado orçamento de segmentos por execução e rotação determinística por cursor para evitar burst de queries e reduzir risco de timeout/429.
- Ajustada extração de identificador para dedupe com suporte resiliente a `id` no topo do payload e fallback para `item.id`.
- Endurecido `scripts/train_oracle.py` para coleta robusta em alto volume: coleta incremental em JSONL, checkpoint/resume, deduplicação por `item_id` e separação dos modos `collect`, `train` (offline) e `collect-train`.
- Melhorado `core/api_integrator.py` com throttle adaptativo, retries com backoff/jitter para `429/5xx`, respeito a `Retry-After` e substituição de sleep fixo por controle contextual de ritmo.
- Observação operacional (Mirage): mesmo com scanner híbrido, janelas de rate limit agressivas da GGG podem causar `429` imediato; o fluxo foi ajustado para degradar com segurança (sem crash), com continuidade por rotação/ciclos e retomada incremental no treino.
- Validação de regressão após refactors/hotfixes: **63 passed, 0 falhas** (`pytest` completo).
