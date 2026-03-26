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
