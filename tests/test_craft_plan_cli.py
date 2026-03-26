"""
Testes de integração para o comando CLI `craft-plan`.

Valida:
- help do comando existe
- output JSON é um JSON válido com schema esperado
- nicho inválido gera erro
- campos de rastreabilidade estão presentes no JSON
- flip-plan não quebra (smoke)
"""

import json
import subprocess
import sys

import pytest


class TestCraftPlanCLI:
    """Testes de integração do comando craft-plan via CLI."""

    def _run_craft_plan(self, *args: str) -> subprocess.CompletedProcess:
        """Helper para rodar cli.py craft-plan com argumentos."""
        result = subprocess.run(
            [sys.executable, "cli.py", "craft-plan", *args],
            capture_output=True,
            text=True,
            cwd="/tmp/hideout_warrior_continue_tmp",
        )
        return result

    def test_craft_plan_help_succeeds(self):
        """--help deve retornar código 0."""
        result = self._run_craft_plan("--help")
        assert result.returncode == 0

    def test_craft_plan_help_shows_niche_option(self):
        """Help deve mencionar o nicho es_influence_shield."""
        result = self._run_craft_plan("--help")
        assert "es_influence_shield" in result.stdout

    def test_craft_plan_json_returns_valid_json(self):
        """--format json deve retornar JSON parseável."""
        result = self._run_craft_plan("--format", "json")
        assert result.returncode == 0
        # Deve ser parseável sem exceção
        data = json.loads(result.stdout)
        assert data is not None

    def test_craft_plan_json_has_metadata(self):
        """Output JSON deve ter chave 'metadata'."""
        result = self._run_craft_plan("--format", "json")
        data = json.loads(result.stdout)
        assert "metadata" in data

    def test_craft_plan_json_metadata_has_required_fields(self):
        """metadata deve conter data_source, used_fallback, fallback_reason."""
        result = self._run_craft_plan("--format", "json")
        data = json.loads(result.stdout)
        meta = data["metadata"]
        assert "data_source" in meta
        assert "used_fallback" in meta
        assert "fallback_reason" in meta

    def test_craft_plan_json_has_methods_array(self):
        """Output JSON deve ter chave 'methods' com exatamente 3 itens."""
        result = self._run_craft_plan("--format", "json")
        data = json.loads(result.stdout)
        assert "methods" in data
        assert isinstance(data["methods"], list)
        assert len(data["methods"]) == 3

    def test_craft_plan_json_method_has_required_fields(self):
        """Cada método no JSON deve ter todos os campos obrigatórios."""
        result = self._run_craft_plan("--format", "json")
        data = json.loads(result.stdout)
        required_fields = [
            "method",
            "hit_probability",
            "expected_cost",
            "brick_risk",
            "ev_net_value",
            "recommended",
            "notes",
            "data_source",
            "used_fallback",
            "fallback_reason",
        ]
        for method in data["methods"]:
            for field in required_fields:
                assert field in method, (
                    f"Campo '{field}' ausente no método {method.get('method', '?')}"
                )

    def test_craft_plan_json_probabilities_in_range(self):
        """hit_probability e brick_risk devem estar entre 0 e 1 no JSON."""
        result = self._run_craft_plan("--format", "json")
        data = json.loads(result.stdout)
        for method in data["methods"]:
            hp = method["hit_probability"]
            br = method["brick_risk"]
            assert 0.0 <= hp <= 1.0, (
                f"hit_probability {hp} fora de [0,1] em {method['method']}"
            )
            assert 0.0 <= br <= 1.0, (
                f"brick_risk {br} fora de [0,1] em {method['method']}"
            )

    def test_craft_plan_json_recommended_is_boolean(self):
        """recommended deve ser bool no JSON."""
        result = self._run_craft_plan("--format", "json")
        data = json.loads(result.stdout)
        for method in data["methods"]:
            assert isinstance(method["recommended"], bool)

    def test_craft_plan_invalid_niche_returns_error(self):
        """Nicho inválido deve retornar erro (código != 0)."""
        result = self._run_craft_plan("--niche", "nao_existe")
        assert result.returncode != 0
        assert "não suportado" in result.stderr or "não suportado" in result.stdout

    def test_craft_plan_invalid_format_returns_error(self):
        """Formato inválido deve retornar erro."""
        result = self._run_craft_plan("--format", "xml")
        assert result.returncode != 0

    def test_flip_plan_help_still_works(self):
        """flip-plan --help deve continuar funcionando (não regressão)."""
        result = subprocess.run(
            [sys.executable, "cli.py", "flip-plan", "--help"],
            capture_output=True,
            text=True,
            cwd="/tmp/hideout_warrior_continue_tmp",
        )
        assert result.returncode == 0
        assert "flip" in result.stdout.lower() or "Flip" in result.stdout

    def test_flip_plan_command_exists(self):
        """O comando flip-plan deve existir e responder."""
        result = subprocess.run(
            [sys.executable, "cli.py", "--help"],
            capture_output=True,
            text=True,
            cwd="/tmp/hideout_warrior_continue_tmp",
        )
        assert result.returncode == 0
        assert "flip-plan" in result.stdout

    def test_craft_plan_command_exists_in_help(self):
        """O comando craft-plan deve aparecer no --help do cli."""
        result = subprocess.run(
            [sys.executable, "cli.py", "--help"],
            capture_output=True,
            text=True,
            cwd="/tmp/hideout_warrior_continue_tmp",
        )
        assert result.returncode == 0
        assert "craft-plan" in result.stdout
