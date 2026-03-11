# CHANGELOG

## 2026-03-11

- Adicionada a camada compartilhada `NormalizedMarketItem` em `core/item_normalizer.py` para unificar a normalização usada por scanner, valuation, treino e planner.
- Segmentado o valuation por família em `core/ml_oracle.py`, com `ValuationResult` estruturado, roteamento por `wand_caster`, `body_armour_defense`, `jewel_cluster`, `accessory_generic` e fallback `generic`.
- Enriquecido o scanner em `core/market_scanner.py` com contexto de mercado (`market_floor`, `market_median`, `comparables_count`, `market_spread`, `pricing_position`) e serialização desses campos em `ScanOpportunity`.
- Adaptado o `flip-plan` em `core/flip_planner.py` para consumir `item_family`, `valuation_result` e sinais de mercado na escolha de alvo e ordenação dos planos.
- Reestruturado `scripts/train_oracle.py` para treinar artefatos por família usando a mesma normalização do scanner e com seleção explícita do schema de features de cada família.
- Corrigida a inferência dos modelos treinados em `core/ml_oracle.py`, alinhando as colunas do `DMatrix` com o schema esperado pelo booster e evitando queda indevida para `family_fallback`.
- Identificado na validação real da liga `Mirage` que várias bases usadas no treino estão concentradas em listings de `1c` a `2c`, o que limita a qualidade econômica dos modelos mesmo com o pipeline corrigido.
- Adicionados testes para normalização compartilhada e atualizados os testes de scanner, valuation e planner para cobrir contratos e comportamento por família.
- Validação executada neste ciclo: `pytest -q` com **36 passed** e `python -m compileall cli.py core scripts tests` sem erros.
- Criado o `scripts/firehose_miner.py` com integração à Public Stash API, uso de `next_change_id`, parser de preços (`~b/o`/`~price`), filtro de raros/únicos e persistência idempotente em SQLite.
- Implementado storage local com as tabelas `stash_events` e `miner_checkpoint`, deduplicação por `(change_id, item_id)` e avanço de checkpoint apenas após commit.
- Atualizado o `train_oracle` com suporte a `--source api|sqlite|parquet`, mantendo `api` como padrão.
- Adicionada leitura de treino via `sqlite`/`parquet` com pipeline de features, filtros e split por família.
- Adicionados novos testes em `tests/test_firehose_miner.py` e atualizações em `tests/test_train_oracle.py`.
- Validação executada no último ciclo: alvo com `pytest` em **8 passed** e suíte completa em **42 passed**.
- Criado o script `scripts/build_training_snapshot.py` para materialização de snapshot de treino.
- Implementado pipeline Bronze/Silver/Gold a partir da tabela SQLite `stash_events`.
- Aplicado particionamento Hive-style: Bronze por `snapshot_date`+`league`; Silver/Gold por `snapshot_date`+`league`+`item_family`.
- Adotada deduplicação por camada: Bronze por `event_key`, Silver por `item_id` mais recente e Gold por duplicata exata de `features`+`target`.
- Ajustado o `train_oracle` para aceitar `source=parquet` com entrada em arquivo único ou diretório particionado.
- Adicionados/atualizados testes em `tests/test_training_snapshot_job.py` e `tests/test_train_oracle.py`.
- Validação executada neste ciclo: `pytest` alvo com **6 passed** e suíte completa com **44 passed**.

## 2026-03-10

- Adicionado o filtro `--min-listed-price` nos comandos `scan` e `flip-plan`, com propagação para o scanner/planner e aplicação no fluxo de seleção de oportunidades.
- Incluídas novas métricas/KPIs relacionadas ao filtro de preço mínimo listado, incluindo contadores de itens filtrados (`filtered_min_listed_price`) e exibição no resumo/CLI.
- Endurecido o treino em `scripts/train_oracle.py` com melhorias anti-data-leakage: auditoria de duplicatas e overlap train/test, split temporal quando timestamp é confiável, e fallback controlado para split aleatório.
- Expandida a avaliação do treino com métricas adicionais: `MAE`, baseline por mediana de treino, `RMSE` de baseline e `RMSE` por buckets de preço.
- Aumentada a robustez em `core/ml_oracle.py` com resolução de path de modelo mais resiliente e consolidação de schema único de features para treino/inferência.
- Ajustado o score/risco em `core/market_scanner.py` com nova flag `high_ticket_low_confidence`, penalidades explícitas no score e comportamento `safe_buy` dinâmico por faixa de preço/confiança.
- Adicionados/atualizados testes em `tests/test_train_oracle.py`, `tests/test_ml_oracle.py` e `tests/test_market_scanner.py` para cobrir split temporal, métricas por bucket, schema de features, flags de risco, penalidades e filtro de preço mínimo.
- Validação de testes executados com `pytest -q` neste fluxo: **41 passed, 11 warnings, 0 falhas**.
