"""
Testes para core/probability_engine.py — MVP craft-plan.

Valida:
- compare_methods() retorna exatamente 3 métodos
- hit_probability e brick_risk entre 0.0 e 1.0
- data_source, used_fallback, fallback_reason presentes em todos os resultados
- fallback_reason preenchido quando used_fallback=True
- nenhum ou exatamente 1 método recommended
- engine expõe get_metadata()
"""

import pytest
from unittest.mock import MagicMock
from core.probability_engine import (
    ProbabilityEngine,
    CraftMethodResult,
    create_engine,
)


class TestProbabilityEngineInit:
    def test_create_engine_returns_engine_instance(self):
        engine = create_engine("es_influence_shield")
        assert isinstance(engine, ProbabilityEngine)

    def test_engine_has_niche_attribute(self):
        engine = ProbabilityEngine(niche="es_influence_shield")
        assert engine.niche == "es_influence_shield"

    def test_engine_default_niche(self):
        engine = ProbabilityEngine()
        assert engine.niche == "es_influence_shield"


class TestCompareMethods:
    """Testes sobre compare_methods() - núcleo do MVP."""

    def test_compare_methods_returns_three_results(self):
        """Deve retornar exatamente 3 métodos comparados."""
        engine = create_engine()
        results = engine.compare_methods()
        assert len(results) == 3

    def test_methods_are_dense_fossil_harvest_reforge_defence_essence(self):
        """Os 3 métodos esperados são Dense Fossil, Harvest Reforge Defence e Essence."""
        engine = create_engine()
        results = engine.compare_methods()
        names = {r.method_name for r in results}
        assert names == {"Dense Fossil", "Harvest Reforge Defence", "Essence of Dread"}

    def test_hit_probability_between_zero_and_one(self):
        """hit_probability deve estar entre 0.0 e 1.0 para todos os métodos."""
        engine = create_engine()
        results = engine.compare_methods()
        for r in results:
            assert 0.0 <= r.hit_probability <= 1.0, (
                f"{r.method_name} tem hit_probability={r.hit_probability} fora do intervalo [0,1]"
            )

    def test_brick_risk_between_zero_and_one(self):
        """brick_risk deve estar entre 0.0 e 1.0 para todos os métodos."""
        engine = create_engine()
        results = engine.compare_methods()
        for r in results:
            assert 0.0 <= r.brick_risk <= 1.0, (
                f"{r.method_name} tem brick_risk={r.brick_risk} fora do intervalo [0,1]"
            )

    def test_expected_cost_is_positive(self):
        """expected_cost deve ser um número positivo (custo em chaos)."""
        engine = create_engine()
        results = engine.compare_methods()
        for r in results:
            assert r.expected_cost >= 0, (
                f"{r.method_name} tem expected_cost={r.expected_cost} negativo"
            )

    def test_ev_net_value_is_finite(self):
        """ev_net_value deve ser um número finito."""
        engine = create_engine()
        results = engine.compare_methods()
        for r in results:
            assert r.ev_net_value != float("inf")
            assert r.ev_net_value != float("-inf")

    def test_zero_or_one_recommended(self):
        """Zero ou exatamente 1 método deve ser recommended."""
        engine = create_engine()
        results = engine.compare_methods()
        recommended_count = sum(1 for r in results if r.recommended)
        assert recommended_count <= 1, (
            f"Múltiplos métodos recomendados: {[r.method_name for r in results if r.recommended]}"
        )

    def test_recommended_method_has_highest_ev(self):
        """Se há um método recommended, ele deve ter o maior EV líquido."""
        engine = create_engine()
        results = engine.compare_methods()
        recommended = [r for r in results if r.recommended]
        if recommended:
            best_ev = max(r.ev_net_value for r in results)
            assert recommended[0].ev_net_value == best_ev


class TestSafetyFields:
    """Validação dos campos obrigatórios de segurança e rastreabilidade."""

    def test_all_results_have_data_source(self):
        """Todos os resultados devem ter data_source não vazio."""
        engine = create_engine()
        results = engine.compare_methods()
        for r in results:
            assert hasattr(r, "data_source")
            assert isinstance(r.data_source, str)
            assert r.data_source != ""

    def test_all_results_have_used_fallback_bool(self):
        """Todos os resultados devem ter used_fallback como bool."""
        engine = create_engine()
        results = engine.compare_methods()
        for r in results:
            assert hasattr(r, "used_fallback")
            assert isinstance(r.used_fallback, bool)

    def test_used_fallback_true_implies_fallback_reason_filled(self):
        """Se used_fallback=True, fallback_reason deve estar preenchido."""
        engine = create_engine()
        results = engine.compare_methods()
        # No MVP atual, todos usam fallback
        for r in results:
            if r.used_fallback:
                assert r.fallback_reason != ""
                assert len(r.fallback_reason) > 5

    def test_fallback_reason_empty_when_not_using_fallback(self):
        """fallback_reason deve ser vazio quando used_fallback=False."""
        engine = create_engine()
        results = engine.compare_methods()
        for r in results:
            if not r.used_fallback:
                assert r.fallback_reason == ""

    def test_metadata_has_required_fields(self):
        """get_metadata() deve expor data_source, used_fallback, fallback_reason."""
        engine = create_engine()
        meta = engine.get_metadata()
        assert "data_source" in meta
        assert "used_fallback" in meta
        assert "fallback_reason" in meta
        assert isinstance(meta["used_fallback"], bool)


class TestCraftMethodResultDataclass:
    """Testes sobre o dataclass CraftMethodResult."""

    def test_result_is_frozen(self):
        """CraftMethodResult deve ser frozen (imutável)."""
        r = CraftMethodResult(
            method_name="Test",
            hit_probability=0.5,
            expected_cost=100.0,
            brick_risk=0.1,
            ev_net_value=50.0,
            recommended=False,
            notes="Test note",
            data_source="test_source",
            used_fallback=False,
            fallback_reason="",
        )
        with pytest.raises(AttributeError):
            r.recommended = True  # type: ignore

    def test_result_fields_types(self):
        """Todos os campos devem ter os tipos corretos."""
        r = CraftMethodResult(
            method_name="Dense Fossil",
            hit_probability=0.18,
            expected_cost=120.0,
            brick_risk=0.12,
            ev_net_value=-90.0,
            recommended=False,
            notes="Test",
            data_source="repoe_fallback",
            used_fallback=True,
            fallback_reason="RePoE não disponível",
        )
        assert isinstance(r.method_name, str)
        assert isinstance(r.hit_probability, float)
        assert isinstance(r.expected_cost, float)
        assert isinstance(r.brick_risk, float)
        assert isinstance(r.ev_net_value, float)
        assert isinstance(r.recommended, bool)
        assert isinstance(r.notes, str)
        assert isinstance(r.data_source, str)
        assert isinstance(r.used_fallback, bool)
        assert isinstance(r.fallback_reason, str)


class TestProbabilityEngineRepoeLive:
    """Bloco 5: Testes para cenário repoe_live via monkeypatch do parser."""

    def _make_fake_repoe_parser_with_mods(self, mods_data: dict) -> MagicMock:
        """Factory: cria um parser fake com mods_data no db."""
        fake_parser = MagicMock()
        fake_parser.db = mods_data
        fake_parser.get_spawn_weight_for_tag = lambda mod_id, tag: (
            mods_data.get(mod_id, {}).get("spawn_weights", [])
        )
        return fake_parser

    def test_dense_fossil_uses_repoe_verified_when_mods_found(self):
        """Dense Fossil deve usar repoe_verified quando mods-alvo existem no pool."""
        engine = ProbabilityEngine(niche="es_influence_shield")

        # Setup: mods-alvo com peso conhecido para item_tag "dex_int_armour"
        fake_db = {
            "ChanceToSuppressSpells2": {
                "spawn_weights": [{"tag": "dex_int_armour", "weight": 500}],
                "groups": ["ChanceToSuppressSpells"],
                "generation_type": "suffix",
            },
            "ChanceToSuppressSpells3": {
                "spawn_weights": [{"tag": "dex_int_armour", "weight": 300}],
                "groups": ["ChanceToSuppressSpells"],
                "generation_type": "suffix",
            },
        }

        # total_spawn_weight_by_groups = 1000 (dois mods com 500 + 300, mais um filler)
        def fake_get_total_spawn_weight_by_groups(item_tag, groups, gen_type):
            return 1000

        engine._repoe_parser = MagicMock()
        engine._repoe_parser.db = fake_db
        engine._repoe_parser.get_spawn_weight_for_tag = lambda mod_id, tag: (
            500
            if mod_id == "ChanceToSuppressSpells2"
            else 300
            if mod_id == "ChanceToSuppressSpells3"
            else 100
            if mod_id == "ChanceToSuppressSpells4"
            else 0
        )
        engine._repoe_parser.get_total_spawn_weight_by_groups = (
            fake_get_total_spawn_weight_by_groups
        )
        engine._repoe_parser.get_total_spawn_weight_by_tag = (
            lambda item_tag, generation_type=None: 1000
        )
        engine._repoe_loaded = True
        engine._used_fallback = False
        engine._fallback_reason = ""

        result = engine.calculate_ev("dense_fossil", "Dense Fossil")

        assert result.data_source == "repoe_verified"
        assert result.used_fallback is False
        assert result.fallback_reason == ""
        # hit_prob = (500 + 300 + 100) / 1000 = 0.9, capped at 0.99
        assert result.hit_probability > 0

    def test_dense_fossil_fallback_when_mods_not_in_pool(self):
        """Dense Fossil deve usar fallback quando mods-alvo não existem no pool."""
        engine = ProbabilityEngine(niche="es_influence_shield")

        # Setup: mods-alvo NÃO existem no pool (weight = 0 para todos)
        engine._repoe_parser = MagicMock()
        engine._repoe_parser.db = {}
        engine._repoe_parser.get_spawn_weight_for_tag = lambda mod_id, tag: 0
        engine._repoe_parser.get_total_spawn_weight_by_groups = (
            lambda item_tag, groups, gen_type: 1000
        )
        engine._repoe_parser.get_total_spawn_weight_by_tag = (
            lambda item_tag, generation_type=None: 1000
        )
        engine._repoe_loaded = True

        result = engine.calculate_ev("dense_fossil", "Dense Fossil")

        assert result.data_source == "repoe_fallback"
        assert result.used_fallback is True
        assert (
            "weight=0" in result.fallback_reason
            or "não encontrados" in result.fallback_reason
        )

    def test_harvest_reforge_uses_repoe_verified_when_mods_found(self):
        """Harvest Reforge Defence deve usar repoe_verified quando mods existem."""
        engine = ProbabilityEngine(niche="es_influence_shield")

        # Setup: mesmo padrão que dense_fossil
        engine._repoe_parser = MagicMock()
        engine._repoe_parser.db = {
            "ChanceToSuppressSpells2": {
                "spawn_weights": [{"tag": "dex_int_armour", "weight": 400}],
                "groups": ["ChanceToSuppressSpells"],
                "generation_type": "suffix",
            },
        }
        engine._repoe_parser.get_spawn_weight_for_tag = (
            lambda mod_id, tag: 400 if mod_id.startswith("ChanceToSuppress") else 0
        )
        engine._repoe_parser.get_total_spawn_weight_by_groups = (
            lambda item_tag, groups, gen_type: 800
        )
        engine._repoe_parser.get_total_spawn_weight_by_tag = (
            lambda item_tag, generation_type=None: 800
        )
        engine._repoe_loaded = True

        result = engine.calculate_ev("harvest_reforge", "Harvest Reforge Defence")

        assert result.data_source == "repoe_verified"
        assert result.used_fallback is False

    def test_essence_always_uses_explicit_fallback(self):
        """Essence deve SEMPRE usar fallback explícito (pool não mapeado no RePoE)."""
        engine = ProbabilityEngine(niche="es_influence_shield")

        # Setup: mesmo com parser carregado, essence deve usar fallback
        engine._repoe_parser = MagicMock()
        engine._repoe_parser.db = {"SomeMod": {"spawn_weights": []}}
        engine._repoe_loaded = True

        result = engine.calculate_ev("essence", "Essence of Dread")

        assert result.data_source == "repoe_fallback"
        assert result.used_fallback is True
        assert "não mapeado no RePoE" in result.fallback_reason


class TestProbabilityEngineFallback:
    """Bloco 5: Testes para cenários de fallback explícito."""

    def test_engine_fallback_when_repoe_not_available(self):
        """Engine deve marcar used_fallback=True quando RePoE não carrega."""
        engine = ProbabilityEngine(niche="es_influence_shield")

        # Simula RePoE não disponível
        engine._repoe_loaded = False
        engine._used_fallback = True
        engine._fallback_reason = "RePoE local não disponível"

        result = engine.calculate_ev("dense_fossil", "Dense Fossil")

        assert result.used_fallback is True
        assert result.fallback_reason != ""

    def test_fallback_reason_contains_tag_info_when_tag_empty(self):
        """Fallback reason deve indicar quando método não tem tag definida."""
        engine = ProbabilityEngine(niche="es_influence_shield")
        engine._repoe_loaded = True
        engine._repoe_parser = MagicMock()

        # Essence não tem tag definida no MVP
        result = engine.calculate_ev("essence", "Essence of Dread")

        # O fallback_reason deve indicar que essence não tem tag
        assert result.used_fallback is True
        # O código source em _get_method_params seta fallback_reason para essence
        assert "Essence pool não mapeado" in result.fallback_reason

    def test_notes_contains_repoe_verified_indicator(self):
        """Notas devem indicar [RePoE: dados verificados] quando source=repoe_verified."""
        engine = ProbabilityEngine(niche="es_influence_shield")

        engine._repoe_parser = MagicMock()
        engine._repoe_parser.db = {
            "ChanceToSuppressSpells2": {
                "spawn_weights": [{"tag": "dex_int_armour", "weight": 500}],
                "groups": ["ChanceToSuppressSpells"],
            },
        }
        engine._repoe_parser.get_spawn_weight_for_tag = (
            lambda mod_id, tag: 500 if mod_id.startswith("ChanceToSuppress") else 0
        )
        engine._repoe_parser.get_total_spawn_weight_by_groups = (
            lambda item_tag, groups, gen_type: 500
        )
        engine._repoe_parser.get_total_spawn_weight_by_tag = (
            lambda item_tag, generation_type=None: 500
        )
        engine._repoe_loaded = True

        result = engine.calculate_ev("dense_fossil", "Dense Fossil")

        assert "[RePoE: dados verificados]" in result.notes

    def test_notes_contains_fallback_indicator(self):
        """Notas devem indicar [FALLBACK: ...] quando source=repoe_fallback."""
        engine = ProbabilityEngine(niche="es_influence_shield")

        result = engine.calculate_ev("essence", "Essence of Dread")

        assert "[FALLBACK:" in result.notes
