# Hideout Warrior CLI - MVP do `craft-plan`

## Visão Geral

Próximo épico após a migração Supabase: adicionar um trilho novo de high-end crafting sem quebrar o fluxo atual de `flip-plan`.

Decisões já aprovadas:
- manter `flip-plan` para fix-up flips
- criar `craft-plan` como comando novo
- MVP focado em **probability engine + EV básico**
- primeiro nicho: **`es_influence_shield`**
- fonte inicial: **RePoE/export versionado**

---

## Fora do MVP

Não entra nesta etapa:
- merge mode
- recombinator advanced logic
- quality/enchant/implicit flips
- multiplos nichos
- actions high-end completas além do mínimo necessário

---

## Bloco 1: CLI `craft-plan` isolado

### Objetivo
Criar o comando novo sem quebrar `flip-plan`.

### O que fazer
1. Adicionar `craft-plan` em `cli.py`
2. Criar o entrypoint básico do planner novo
3. Manter `flip-plan` intacto

### Arquivos prováveis
- `cli.py`
- `core/craft_planner.py`
- `core/models.py`

### Verificação
- `python cli.py craft-plan --help` funciona
- `python cli.py flip-plan --help` continua funcionando

### Risco principal
- acoplamento indevido com `flip-plan`

---

## Bloco 2: `core/probability_engine.py` MVP

### Objetivo
Substituir heurística estática mínima por probabilidade real/estimada com EV básico.

### Escopo
Métodos iniciais:
- `Dense Fossil`
- `Harvest Reforge Defence`
- `Essence`

Saídas mínimas:
- `hit_probability`
- `expected_cost`
- `brick_risk`
- `data_source`
- `used_fallback`

### Arquivos prováveis
- `core/probability_engine.py`
- `core/evaluator.py`
- dados versionados em diretório local do projeto

### Verificação
- engine retorna probabilidades entre `0.0` e `1.0`
- fallback explícito quando RePoE não cobrir um caso

### Risco principal
- cobertura incompleta do RePoE para fossil/influenced crafting

---

## Bloco 3: Perfil `es_influence_shield`

### Objetivo
Focar o MVP em um único nicho high-end com comparação simples de métodos por EV.

### Escopo
Perfil inicial:
- `es_influence_shield`

Métodos comparados:
- Dense Fossil
- Harvest Reforge Defence
- Essence

Output mínimo do `craft-plan`:
- método
- hit%
- expected_cost (chaos)
- brick_risk
- recommended (Y/N)

### Arquivos prováveis
- `core/craft_planner.py`
- `core/probability_engine.py`
- `core/models.py`
- `core/flip_planner.py` apenas se precisar extrair lógica compartilhada

### Verificação
- `craft-plan` gera comparação de EV para `es_influence_shield`
- um método sai como recomendado com justificativa simples

### Risco principal
- modelagem simplificada demais do nicho gerar saída enganosa

---

## Bloco 4: Integração e Verificação

### Objetivo
Garantir que o MVP nasce sem regressão no fluxo atual.

### O que fazer
1. Cobrir `craft-plan` e `probability_engine` com testes
2. Confirmar que `flip-plan` não mudou de comportamento
3. Validar output básico do nicho inicial

### Arquivos prováveis
- `tests/test_craft_plan.py`
- `tests/test_probability_engine.py`
- ajustes em testes existentes se necessário

### Verificação
- suíte alvo do novo MVP passa
- `pytest -q` continua verde

### Risco principal
- regressão indireta no planner atual

---

## Critério de Aceite do MVP

O MVP será considerado pronto quando:

1. `craft-plan` existir como comando separado
2. `flip-plan` continuar intacto
3. o nicho `es_influence_shield` gerar comparação de EV entre 3 métodos
4. o engine indicar claramente quando usou fallback
5. a suíte de testes passar

---

## Ordem de Execução

| Bloco | Prioridade | Dependência |
|-------|------------|-------------|
| 1. CLI `craft-plan` | P0 | — |
| 2. `probability_engine.py` MVP | P0 | — |
| 3. Perfil `es_influence_shield` | P1 | 1 + 2 |
| 4. Integração e Verificação | P1 | 1 + 2 + 3 |

---

## Risco Global do MVP

**Cobertura incompleta do RePoE para métodos de craft específicos** pode exigir fallback em parte dos cenários. Isso é aceitável no MVP, desde que o output deixe claro quando o fallback foi usado.

---

*Plano atualizado para MVP do `craft-plan`: 2026-03-26*

---

## Bloco 5: RePoE Real no `probability_engine.py` para `es_influence_shield`

### Objetivo

Substituir o fallback hardcoded (valores fixos em `_DENSE_FOSSIL_PARAMS`, `_HARVEST_REFORGE_DEFENCE_PARAMS`, `_ESSENCE_DREAD_PARAMS`) por consulta real ao RePoE via `RePoeParser.get_weight()` e `get_total_weight_by_tag()`.

### O que sai do fallback e passa a usar RePoE real

| Método | Tag consultada no RePoE | Mod target |
|--------|------------------------|------------|
| Dense Fossil | `defence` | `Spell Suppression` suffix |
| Harvest Reforge Defence | `defence` | `Spell Suppression` suffix |
| Essence of Dread | `essence` + filtro `ES prefix` | `Maximum Energy Shield` prefix |

**Cálculo de `hit_probability`**:
```
P(hit) = weight(target_mod) / total_weight_by_tag(tag)
```

**Cálculo de `hit_probability` para Essence** (caso especial — pool restrito):
```
P(hit) = weight(target_mod) / total_essence_mods_for(base_type)
```

### O que continua fora do escopo

- Outros nichos além de `es_influence_shield`
- Dense Fossil + Harvest com-tags (ainda não mapeado)
- Lógica de brick risk por mod (permanece fallback conservativo)
- Cache persistente de pesos RePoE (usa o que já está em `data/`)

### Arquivos prováveis

- `core/probability_engine.py` (substitui lógica do `_get_method_params()`)
- `core/data_parser.py` (pode precisar de ajuste em `get_total_weight_by_tag` para filtrar por `mod_group` ou `base_type`)

### Verificação mínima

1. `craft-plan` para `es_influence_shield` retorna `data_source: "repoe_live"` para todos os 3 métodos (sem fallback)
2. `hit_probability` varia entre 0.0 e 1.0
3. `data_source` ainda indica `"repoe_fallback"` quando o mod não existe no RePoE (graceful degradation)

### Risco principal

**Mod ID incorreto**: se os `mod_id` do RePoE não baterem com os nomes dos mods do jogo para `es_influence_shield`, o peso retorna 0 e o fallback é acionado silenciosamente. Mitigação: log explícito quando peso = 0.

---

## Ordem de Execução (Atualizada)

| Bloco | Prioridade | Dependência |
|-------|------------|-------------|
| 1. CLI `craft-plan` | P0 | — |
| 2. `probability_engine.py` MVP | P0 | — |
| 3. Perfil `es_influence_shield` | P1 | 1 + 2 |
| 4. Integração e Verificação | P1 | 1 + 2 + 3 |
| **5. RePoE Real** | **P0** | **3** |

---

*Plano atualizado com Bloco 5 (RePoE real): 2026-03-26*

---

## Bloco 6: Endurecer o Nicho `es_influence_shield`

### Objetivo

Reduzir fallback, melhorar mapeamento de mod IDs do RePoE e refinar o cálculo por contexto/pool para o nicho `es_influence_shield` — sem reescrever o sistema inteiro.

---

### O que entra

1. **Mapear os 3–5 mod IDs mais relevantes** do RePoE para `es_influence_shield`:
   - Prefix: `Maximum Energy Shield` (targeado pelo pool de Essence)
   - Suffix: `Spell Suppression` (target do Dense Fossil / Harvest)
   - Verificar se há mods de `+1 to Level of all Spell Skill Gems` (influência) que competem no pool

2. **Validar e corrigir os pesos** do RePoE para cada mod ID identificado:
   - Conferir que `weight(target_mod) > 0` no RePoE
   - Se peso = 0, investigar se o mod existe com nome diferente ou se é exilado no ladder
   - Log explícito quando peso = 0 (em vez de fallback silencioso)

3. **Refinar pool/context para Essence**:
   - O pool de Essence é restrito a mods do tipo `essence` filtrados por base_type
   - Se o RePoE expõe `mod_group` ou `gen_type`, usar para filtrar o denominador correto
   - Confirmar se `Maximum Energy Shield` é de fato prefix-only no pool de Essence de ES shield

4. **Refinar pool/context para Dense Fossil**:
   - Tag `defence` inclui vários mods que não são elegíveis em shield (ex.: evasion, armor)
   - Se o RePoE expõe `item_classes`, usar para filtrar o denominador para `ES shield` especificamente

5. **Melhorar `data_source` e `used_fallback`**:
   - `data_source` = `"repoe_verified"` quando mod ID foi validado com peso > 0
   - `data_source` = `"repoe_fallback"` quando houve graceful degradation
   - `used_fallback` = `true` apenas quando houve interpolação ou valor estimado

---

### O que continua fora do escopo

- Outros nichos além de `es_influence_shield`
- Lógica de brick risk por mod (permanece fallback conservativo)
- Cache persistente de pesos RePoE
- Merge mode ou recombinator
- Dense Fossil + Harvest com-tags (ainda não mapeado)

---

### Arquivos prováveis

- `core/probability_engine.py` — ajuste na lógica de `_get_method_params()` e no cálculo de denominador por contexto
- `core/data_parser.py` — adicionar método de filtragem por `item_classes` / `mod_group` se disponível no RePoE
- `core/craft_planner.py` — ajustar output de `data_source` e `used_fallback`

---

### Verificação mínima

1. `craft-plan` para `es_influence_shield` retorna `data_source: "repoe_verified"` para todos os 3 métodos (sem fallback)
2. Nenhum método no nicho ativa `used_fallback: true`
3. Logs não mostram `weight = 0` para os mods-alvo identificados
4. `hit_probability` de Essence para `Maximum Energy Shield` é diferente (e maior) que o fallback anterior

---

### Risco principal

**Mod ID errado no RePoE**: se os identificadores de mod não corresponderem exatamente aos nomes internos do jogo, o peso será 0 e o fallback permanecerá. Mitigação: validação explícita com log antes de qualquer cálculo.

---

## Ordem de Execução (Atualizada)

| Bloco | Prioridade | Dependência | Status |
|-------|------------|-------------|--------|
| 1. CLI `craft-plan` | P0 | — | ✅ Completo |
| 2. `probability_engine.py` MVP | P0 | — | ✅ Completo |
| 3. Perfil `es_influence_shield` | P1 | 1 + 2 | ✅ Completo |
| 4. Integração e Verificação | P1 | 1 + 2 + 3 | ✅ Completo |
| 5. RePoE Real | P0 | 3 | ✅ Completo |
| **6. Endurecer `es_influence_shield`** | **P1** | **5** | ✅ **Completo** |

---

## Bloco 6 - Completo ✅

### Realizações

1. **Mod IDs mapeados corretamente:**
   - Spell Suppression: `ChanceToSuppressSpells2`, `ChanceToSuppressSpells3`, `ChanceToSuppressSpells4`
   - ES% Prefix: `LocalIncreasedEnergyShieldPercent8` (e tiers)

2. **Spawn weights validados:**
   - Tag correta para ES Shield: `dex_int_armour` (peso 500)
   - Dense Fossil usa tag: `defences` (com 's')
   - Pool total do grupo `ChanceToSuppressSpells`: 5000

3. **Hit probability calculada com RePoE real:**
   - Dense Fossil: ~30% (1500/5000)
   - Harvest Reforge: ~30% (mesmo pool)
   - Essence: fallback (pool não mapeado)

4. **`data_source` melhorado:**
   - `repoe_verified`: mods encontrados com peso > 0
   - `repoe_fallback`: graceful degradation
   - Logs explícitos quando weight = 0

5. **Testes:**
   - 275 testes passando
   - Nenhuma regressão

---

*Plano atualizado - Bloco 6 completo: 2026-03-26*
