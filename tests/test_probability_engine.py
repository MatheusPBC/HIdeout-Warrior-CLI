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
