"""
Testes para core/data_parser.py — Bloco 5: Cobertura do RePoeParser.

Valida:
- get_weight_for_tag() retorna peso correto para mod/tag existente
- get_weight_for_tag() retorna 0 para mod inexistente
- get_weight_for_tag() retorna 0 para tag inexistente no mod
- get_weight_for_tag() retorna 0 quando db está vazio
- get_total_weight_by_tag() soma corretamente
"""

import pytest
from unittest.mock import patch, MagicMock
from core.data_parser import RePoeParser


class TestGetWeightForTag:
    """Cobertura para o método get_weight_for_tag() do Bloco 5."""

    def test_get_weight_for_tag_returns_weight_for_existing_mod_and_tag(self):
        """Deve retornar o peso correto quando mod e tag existem."""
        parser = RePoeParser(data_dir="/tmp/nonexistent_data_dir")
        parser.db = {
            "SpellSuppression1": {
                "mod_id": "SpellSuppression1",
                "weights": [
                    {"tag": "defence", "weight": 500},
                    {"tag": "shield", "weight": 300},
                ],
            }
        }

        weight = parser.get_weight_for_tag("SpellSuppression1", "defence")
        assert weight == 500

    def test_get_weight_for_tag_returns_0_for_nonexistent_mod(self):
        """Deve retornar 0 quando o mod_id não existe no db."""
        parser = RePoeParser(data_dir="/tmp/nonexistent_data_dir")
        parser.db = {
            "SpellSuppression1": {
                "mod_id": "SpellSuppression1",
                "weights": [{"tag": "defence", "weight": 500}],
            }
        }

        weight = parser.get_weight_for_tag("NonExistentMod", "defence")
        assert weight == 0

    def test_get_weight_for_tag_returns_0_for_nonexistent_tag(self):
        """Deve retornar 0 quando o mod existe mas não tem a tag."""
        parser = RePoeParser(data_dir="/tmp/nonexistent_data_dir")
        parser.db = {
            "SpellSuppression1": {
                "mod_id": "SpellSuppression1",
                "weights": [{"tag": "defence", "weight": 500}],
            }
        }

        weight = parser.get_weight_for_tag("SpellSuppression1", "nonexistent_tag")
        assert weight == 0

    def test_get_weight_for_tag_returns_0_when_db_empty(self):
        """Deve retornar 0 quando o db está vazio."""
        parser = RePoeParser(data_dir="/tmp/nonexistent_data_dir")
        parser.db = {}

        weight = parser.get_weight_for_tag("SpellSuppression1", "defence")
        assert weight == 0

    def test_get_weight_for_tag_returns_0_when_mod_has_no_weights(self):
        """Deve retornar 0 quando o mod existe mas não tem weights."""
        parser = RePoeParser(data_dir="/tmp/nonexistent_data_dir")
        parser.db = {
            "SpellSuppression1": {"mod_id": "SpellSuppression1", "weights": []}
        }

        weight = parser.get_weight_for_tag("SpellSuppression1", "defence")
        assert weight == 0

    def test_get_weight_for_tag_with_multiple_tags_returns_correct(self):
        """Deve retornar peso correto quando mod tem múltiplas tags."""
        parser = RePoeParser(data_dir="/tmp/nonexistent_data_dir")
        parser.db = {
            "EnergyShield1": {
                "mod_id": "EnergyShield1",
                "weights": [
                    {"tag": "defence", "weight": 400},
                    {"tag": "shield", "weight": 600},
                    {"tag": "es", "weight": 800},
                ],
            }
        }

        assert parser.get_weight_for_tag("EnergyShield1", "defence") == 400
        assert parser.get_weight_for_tag("EnergyShield1", "shield") == 600
        assert parser.get_weight_for_tag("EnergyShield1", "es") == 800


class TestGetTotalWeightByTag:
    """Cobertura para get_total_weight_by_tag()."""

    def test_get_total_weight_by_tag_sums_all_mods_with_tag(self):
        """Deve somar os pesos de todos os mods que têm a tag."""
        parser = RePoeParser(data_dir="/tmp/nonexistent_data_dir")
        parser.db = {
            "SpellSuppression1": {
                "mod_id": "SpellSuppression1",
                "weights": [{"tag": "defence", "weight": 500}],
                "tags": ["defence"],
            },
            "SpellSuppression2": {
                "mod_id": "SpellSuppression2",
                "weights": [{"tag": "defence", "weight": 300}],
                "tags": ["defence"],
            },
            "FireResist1": {
                "mod_id": "FireResist1",
                "weights": [{"tag": "resistance", "weight": 700}],
                "tags": ["fire", "resistance"],
            },
        }

        total = parser.get_total_weight_by_tag("defence")
        assert total == 800  # 500 + 300

    def test_get_total_weight_by_tag_returns_0_when_no_mods_have_tag(self):
        """Deve retornar 0 quando nenhum mod tem a tag."""
        parser = RePoeParser(data_dir="/tmp/nonexistent_data_dir")
        parser.db = {
            "SpellSuppression1": {
                "mod_id": "SpellSuppression1",
                "weights": [{"tag": "defence", "weight": 500}],
                "tags": ["defence"],
            }
        }

        total = parser.get_total_weight_by_tag("nonexistent_tag")
        assert total == 0

    def test_get_total_weight_by_tag_returns_0_when_db_empty(self):
        """Deve retornar 0 quando o db está vazio."""
        parser = RePoeParser(data_dir="/tmp/nonexistent_data_dir")
        parser.db = {}

        total = parser.get_total_weight_by_tag("defence")
        assert total == 0
