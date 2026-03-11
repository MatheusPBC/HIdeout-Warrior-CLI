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

## 2026-03-10

- Adicionado o filtro `--min-listed-price` nos comandos `scan` e `flip-plan`, com propagação para o scanner/planner e aplicação no fluxo de seleção de oportunidades.
- Incluídas novas métricas/KPIs relacionadas ao filtro de preço mínimo listado, incluindo contadores de itens filtrados (`filtered_min_listed_price`) e exibição no resumo/CLI.
- Endurecido o treino em `scripts/train_oracle.py` com melhorias anti-data-leakage: auditoria de duplicatas e overlap train/test, split temporal quando timestamp é confiável, e fallback controlado para split aleatório.
- Expandida a avaliação do treino com métricas adicionais: `MAE`, baseline por mediana de treino, `RMSE` de baseline e `RMSE` por buckets de preço.
- Aumentada a robustez em `core/ml_oracle.py` com resolução de path de modelo mais resiliente e consolidação de schema único de features para treino/inferência.
- Ajustado o score/risco em `core/market_scanner.py` com nova flag `high_ticket_low_confidence`, penalidades explícitas no score e comportamento `safe_buy` dinâmico por faixa de preço/confiança.
- Adicionados/atualizados testes em `tests/test_train_oracle.py`, `tests/test_ml_oracle.py` e `tests/test_market_scanner.py` para cobrir split temporal, métricas por bucket, schema de features, flags de risco, penalidades e filtro de preço mínimo.
- Validação de testes executados com `pytest -q` neste fluxo: **41 passed, 11 warnings, 0 falhas**.
