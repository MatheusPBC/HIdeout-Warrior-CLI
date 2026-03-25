# Hideout Warrior CLI - Craft Intelligence Plan

## Goal

Expandir a CLI para virar uma multiferramenta com dois trilhos claros:

- `flip-plan` -> manter foco em `fix-up flips`
- `craft-plan` -> novo comando para `high-end crafting`

## Product Decisions

- nao substituir o fluxo atual de `fix-up`
- adicionar um trilho novo e separado para `high-end`
- evitar misturar UX, catalogos e heuristicas dos dois modos
- usar dados versionados tipo RePoE/PyPoE em vez de depender de scraping direto do site do Craft of Exile

## Commands

- `python cli.py flip-plan ...` -> fluxo atual conservador/fix-up
- `python cli.py craft-plan ...` -> novo fluxo high-end

## Track A - Fix-Up Flips

Manter e evoluir o modo atual para:

- bench crafts
- essences simples
- harvests conservadores
- flips rapidos de baixo a medio risco
- quality-of-life e melhor score operacional

## Track B - High-End Crafting

Novo fluxo para:

- bases i86+
- influencia
- Dense Fossil e combinacoes de fossils
- metacrafts
- Awakener's Orb
- Veiled Orb
- Eldritch crafting
- donor-pair / merge mode
- quality/enchant/implicit flips

## Architecture

### 1. Strategy Split

Separar a logica de planejamento por estrategia:

- `fixup`
- `high_end`
- `merge`
- `quality`

### 2. Planner Split

Em `core/flip_planner.py`, extrair a logica compartilhada e preparar um novo planner/entrypoint para `craft-plan`.

Sugestao:

- manter `FlipAdvisor` para fix-up
- criar `CraftAdvisor` para high-end

### 3. Action Catalog Split

Separar catalogos:

- `_FIXUP_ACTION_CATALOG`
- `_HIGH_END_ACTION_CATALOG`

### 4. Profile Split

Separar perfis:

- `_FAMILY_PROFILES_FIXUP`
- `_FAMILY_PROFILES_HIGH_END`

Perfis high-end iniciais:

- `es_influence_shield`
- `es_body_armour_influenced`
- `wand_plus_gems`
- `suppress_evasion_chest`
- `recombinator_donor_pair`

### 5. Shared Probability Engine

Criar um modulo novo:

- `core/probability_engine.py`

Responsabilidades:

- carregar weights/tags/tiers
- calcular chance por metodo
- calcular custo esperado
- calcular brick risk
- expor conflitos/exclusive groups
- servir `craft-plan`, `evaluator`, `recombinators` e depois `graph_engine`

## Phases

### Phase 1 - Dual-Path CLI

Objetivo:

- introduzir `craft-plan` sem quebrar `flip-plan`

Escopo:

- novo comando CLI
- wiring inicial com `CraftAdvisor`
- catalogos e profiles separados
- shared models onde fizer sentido

Arquivos provaveis:

- `cli.py`
- `core/flip_planner.py`
- possivel `core/craft_planner.py`
- `core/models.py`

Aceite:

- `flip-plan` segue funcionando igual
- `craft-plan` existe e responde com estrutura basica

### Phase 2 - Probability Engine MVP

Objetivo:

- substituir hardcodes mais criticos por probabilidades reais

Escopo MVP:

- fonte de dados versionada
- suporte inicial a:
  - `Dense Fossil`
  - `Harvest Reforge Defence`
  - `Essence`
- calculo:
  - hit probability
  - expected craft cost
  - brick risk
  - tier targeting basico

Arquivos provaveis:

- `core/probability_engine.py`
- `core/evaluator.py`
- parser/data loader novo ou extensao do parser atual
- `tests/...`

Aceite:

- engine responde probabilidades entre `0.0` e `1.0`
- fallback existe quando faltar dado
- planner consegue comparar metodos com EV real

### Phase 3 - High-End Action Catalog

Objetivo:

- ensinar o `craft-plan` a usar moedas/metodos high-end

Acoes iniciais:

- `suffixes_cannot_be_changed`
- `prefixes_cannot_be_changed`
- `awakener_orb_merge`
- `dense_fossil_roll`
- `eldritch_implicit_roll`
- `veiled_orb_attempt`

Arquivos provaveis:

- `core/craft_planner.py`
- `core/probability_engine.py`
- `core/models.py`

Aceite:

- `craft-plan` consegue montar passos high-end com custo e probabilidade
- output deixa claro metodo, EV e risco

### Phase 4 - Niche Profiles

Objetivo:

- sair de perfis genericos e mirar nichos de ouro

Perfis prioritarios:

- `es_influence_shield`
- `recover_es_on_block`
- `es_body_armour_influenced`
- `wand_plus_gems`

Arquivos provaveis:

- `core/craft_planner.py`
- `core/models.py`
- `core/ml_oracle.py` se precisar sinal especifico de valor

Aceite:

- planner escolhe nicho certo por base/meta
- output explica por que o nicho faz sentido

### Phase 5 - Merge Mode

Objetivo:

- procurar pares de itens para merge/recomb/awakener logic

Escopo:

- scanner buscar item base + donor
- planner calcular EV conjunto
- usar recombinator/exclusive-group logic

Arquivos provaveis:

- `core/market_scanner.py`
- `core/recombinators.py`
- `core/craft_planner.py`

Aceite:

- `craft-plan --mode merge` sugere par de compra
- output mostra custo combinado, chance e EV

### Phase 6 - Non-Affix Flips

Objetivo:

- criar flips por quality/enchant/implicit sem rerrolar afixos principais

Escopo:

- `quality`
- `implicit`
- `enchant`
- `catalyst/rune` uplift quando fizer sentido

Arquivos provaveis:

- `core/craft_planner.py`
- `core/ml_oracle.py`
- `core/market_scanner.py`

Aceite:

- planner detecta flips de melhoria estrutural
- output separa claramente esse modo de craft de afixos

## Data Source Recommendation

Preferencia:

- RePoE export/versionado

Fallbacks:

- PyPoE
- dataset local derivado/importado

Nao recomendado no inicio:

- scraping direto do site do Craft of Exile

## Risks

- misturar fix-up com high-end e degradar a UX
- engine probabilistico sem dados confiaveis
- escopo grande demais cedo
- alto custo computacional no merge mode
- heuristicas antigas conflitarem com probabilidades reais

## Implementation Order

1. `craft-plan` command
2. strategy/catalog/profile split
3. `probability_engine` MVP
4. high-end action catalog
5. niche profiles
6. merge mode
7. non-affix flips

## Success Criteria

- `flip-plan` continua conservador e estavel
- `craft-plan` nasce isolado e extensivel
- high-end usa EV probabilistico real, nao apenas hardcode
- recombinator/merge recebe exclusive-group logic melhor
- CLI vira multiferramenta sem perder o fluxo atual
