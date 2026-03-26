"""
RePoE Data Parser - Motor de Pesos para Path of Exile.

Fase 1: Data Layer.
Baixa os mod weights (pesos) do RePoE e converte para consulta O(1).

Estrutura corrigida:
- spawn_weights: define em quais itens o mod pode spawnar e com qual peso
- generation_weights: mods têm vazio (não usado para este propósito)
- generation_type: prefix/suffix
- groups: grupos de mods relacionados

Para Dense Fossil:
- Usa tag "defences" no positive_mod_weights
- Mods de defesa têm grupo "DefencesPercent" ou spawn_weights com tags de armour/shield
"""

import os
import json
import logging
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)


class RePoeParser:
    """
    Motor de Pesos do RePoE (Path of Exile Repository of Data).
    """

    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.mods_url = "https://raw.githubusercontent.com/brather1ng/RePoE/master/RePoE/data/mods.json"
        self.fossils_url = "https://raw.githubusercontent.com/brather1ng/RePoE/master/RePoE/data/fossils.json"

        self.parsed_weights_path = os.path.join(self.data_dir, "parsed_weights.json")
        self.parsed_fossils_path = os.path.join(self.data_dir, "parsed_fossils.json")
        self.db: Dict[str, Any] = {}
        self.fossils_db: Dict[str, Any] = {}

        os.makedirs(self.data_dir, exist_ok=True)
        self._load_local_db()

    def _load_local_db(self):
        """Carrega o banco JSON local se existir."""
        if os.path.exists(self.parsed_weights_path):
            try:
                with open(self.parsed_weights_path, "r", encoding="utf-8") as f:
                    self.db = json.load(f)
                logger.info(
                    f"RePoeParser: Carregados {len(self.db)} mods do cache local"
                )
            except json.JSONDecodeError:
                self.db = {}

        if os.path.exists(self.parsed_fossils_path):
            try:
                with open(self.parsed_fossils_path, "r", encoding="utf-8") as f:
                    self.fossils_db = json.load(f)
            except json.JSONDecodeError:
                self.fossils_db = {}

    def _download_raw_data(self, url: str) -> Dict[str, Any]:
        """Faz download de dados do RePoE."""
        logger.info(f"RePoeParser: Baixando dados de {url}...")
        try:
            import requests

            response = requests.get(url, timeout=60)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"RePoeParser: Erro ao baixar dados: {e}")
            return {}

    def build_local_db(self, force_download: bool = False):
        """
        Constrói o DB local otimizado para consulta O(1).
        """
        if not force_download and os.path.exists(self.parsed_weights_path):
            logger.info(
                "RePoeParser: Cache local encontrado. Use force_download=True para atualizar."
            )
            return

        raw_mods = self._download_raw_data(self.mods_url)
        if not raw_mods:
            logger.error("RePoeParser: Falha ao baixar mods.json")
            return

        logger.info(f"RePoeParser: Processando {len(raw_mods)} mods...")
        parsed_db = {}

        for mod_id, mod_data in raw_mods.items():
            # Ignorar mods de unique/implicit que não são crafting normal
            gen_type = mod_data.get("generation_type", "")
            if gen_type in ("unique", "implicit"):
                continue

            # Extrair spawn_weights (importante para cálculo de pool)
            spawn_weights = mod_data.get("spawn_weights", [])
            clean_spawn_weights = []
            for sw in spawn_weights:
                clean_spawn_weights.append(
                    {"tag": sw.get("tag"), "weight": sw.get("weight", 0)}
                )

            # Extrair stats para identificar mods de defesa/ES
            stats = mod_data.get("stats", [])
            stat_ids = [s.get("id", "") for s in stats]

            parsed_db[mod_id] = {
                "mod_id": mod_id,
                "name": mod_data.get("name", ""),
                "generation_type": gen_type,
                "groups": mod_data.get("groups", []),
                "spawn_weights": clean_spawn_weights,
                "stats": stat_ids,
                "required_level": mod_data.get("required_level", 1),
                "is_essence_only": mod_data.get("is_essence_only", False),
            }

        with open(self.parsed_weights_path, "w", encoding="utf-8") as f:
            json.dump(parsed_db, f, indent=2)

        self.db = parsed_db
        logger.info(f"RePoeParser: {len(self.db)} mods processados e salvos")

        # Processar fossils
        self._build_fossils_db(force_download)

    def _build_fossils_db(self, force_download: bool = False):
        """Processa dados de fossils para entender positive_mod_weights."""
        if not force_download and os.path.exists(self.parsed_fossils_path):
            return

        raw_fossils = self._download_raw_data(self.fossils_url)
        if not raw_fossils:
            return

        parsed_fossils = {}
        for fossil_id, fossil_data in raw_fossils.items():
            fossil_name = fossil_data.get("name", "")
            if not fossil_name:
                continue

            parsed_fossils[fossil_name.lower()] = {
                "name": fossil_name,
                "positive_mod_weights": fossil_data.get("positive_mod_weights", []),
                "negative_mod_weights": fossil_data.get("negative_mod_weights", []),
                "allowed_tags": fossil_data.get("allowed_tags", []),
                "forbidden_tags": fossil_data.get("forbidden_tags", []),
            }

        with open(self.parsed_fossils_path, "w", encoding="utf-8") as f:
            json.dump(parsed_fossils, f, indent=2)

        self.fossils_db = parsed_fossils
        logger.info(f"RePoeParser: {len(parsed_fossils)} fossils processados")

    def get_spawn_weight_for_tag(self, mod_id: str, item_tag: str) -> int:
        """
        Retorna o spawn_weight de um mod para uma tag de item específica.

        Args:
            mod_id: ID do mod (ex: "ChanceToSuppressSpells2")
            item_tag: Tag do item (ex: "shield", "dex_armour", "dex_int_armour")

        Returns:
            Peso de spawn para o item, ou 0 se o mod não spawnar nesse tipo de item.
        """
        if not self.db:
            self._load_local_db()

        mod_data = self.db.get(mod_id)
        if not mod_data:
            return 0

        for sw in mod_data.get("spawn_weights", []):
            if sw.get("tag") == item_tag:
                return sw.get("weight", 0)

        return 0

    def get_mod_data(self, mod_id: str) -> Optional[Dict[str, Any]]:
        """Retorna todos os dados de um mod."""
        if not self.db:
            self._load_local_db()
        return self.db.get(mod_id)

    def get_total_spawn_weight_by_tag(
        self,
        item_tag: str,
        generation_type: Optional[str] = None,
        mod_group: Optional[str] = None,
    ) -> int:
        """
        Calcula o peso total de spawn para todos os mods que podem spawnar em um item.

        Args:
            item_tag: Tag do item (ex: "shield", "dex_int_armour")
            generation_type: Filtrar por prefix/suffix (opcional)
            mod_group: Filtrar por grupo (ex: "DefencesPercent") (opcional)

        Returns:
            Soma total dos spawn_weights para a tag do item.
        """
        if not self.db:
            self._load_local_db()

        total_weight = 0
        for mod_id, mod_data in self.db.items():
            # Filtrar por generation_type
            if generation_type and mod_data.get("generation_type") != generation_type:
                continue

            # Filtrar por mod_group
            if mod_group and mod_group not in mod_data.get("groups", []):
                continue

            # Somar peso para a tag do item
            for sw in mod_data.get("spawn_weights", []):
                if sw.get("tag") == item_tag:
                    total_weight += sw.get("weight", 0)

        return total_weight

    def get_total_spawn_weight_by_groups(
        self,
        item_tag: str,
        groups: List[str],
        generation_type: Optional[str] = None,
    ) -> int:
        """
        Calcula o peso total de spawn para mods de grupos específicos.

        Args:
            item_tag: Tag do item
            groups: Lista de grupos (ex: ["DefencesPercent", "ChanceToSuppressSpells"])
            generation_type: Filtrar por prefix/suffix

        Returns:
            Soma total dos spawn_weights para os grupos especificados.
        """
        if not self.db:
            self._load_local_db()

        total_weight = 0
        for mod_id, mod_data in self.db.items():
            # Filtrar por generation_type
            if generation_type and mod_data.get("generation_type") != generation_type:
                continue

            # Verificar se o mod pertence a um dos grupos
            mod_groups = mod_data.get("groups", [])
            if not any(g in mod_groups for g in groups):
                continue

            # Somar peso para a tag do item
            for sw in mod_data.get("spawn_weights", []):
                if sw.get("tag") == item_tag:
                    total_weight += sw.get("weight", 0)

        return total_weight

    def get_fossil_data(self, fossil_name: str) -> Optional[Dict[str, Any]]:
        """Retorna dados de um fossil pelo nome."""
        if not self.fossils_db:
            self._load_local_db()
        return self.fossils_db.get(fossil_name.lower())

    def get_mod_ids_by_stats(
        self,
        stat_patterns: List[str],
        generation_type: Optional[str] = None,
        item_tag: Optional[str] = None,
    ) -> List[str]:
        """
        Busca mods por padrões em seus stats.

        Args:
            stat_patterns: Padrões a buscar nos stat IDs (ex: ["energy_shield", "spell_suppress"])
            generation_type: Filtrar por prefix/suffix
            item_tag: Filtrar por tag de item que pode spawnar

        Returns:
            Lista de mod_ids que match os critérios.
        """
        if not self.db:
            self._load_local_db()

        matching_mods = []
        for mod_id, mod_data in self.db.items():
            # Filtrar por generation_type
            if generation_type and mod_data.get("generation_type") != generation_type:
                continue

            # Verificar stats
            stats = mod_data.get("stats", [])
            has_match = False
            for stat_id in stats:
                if any(p.lower() in stat_id.lower() for p in stat_patterns):
                    has_match = True
                    break

            if not has_match:
                continue

            # Filtrar por item_tag
            if item_tag:
                can_spawn = False
                for sw in mod_data.get("spawn_weights", []):
                    if sw.get("tag") == item_tag and sw.get("weight", 0) > 0:
                        can_spawn = True
                        break
                if not can_spawn:
                    continue

            matching_mods.append(mod_id)

        return matching_mods


# Constantes para mods conhecidos do nicho es_influence_shield
REPOE_MOD_IDS = {
    # Spell Suppression Suffix (ChanceToSuppressSpells)
    "spell_suppression": {
        "mod_ids": [
            "ChanceToSuppressSpells2",  # of Snuffing (T2)
            "ChanceToSuppressSpells3",  # of Revoking (T3)
            "ChanceToSuppressSpells4",  # of Abjuration (T4)
        ],
        "item_tags": [
            "dex_armour",
            "dex_int_armour",
            "str_dex_armour",
            "str_dex_int_armour",
        ],
        "generation_type": "suffix",
        "groups": ["ChanceToSuppressSpells"],
    },
    # Energy Shield % Prefix (LocalIncreasedEnergyShieldPercent)
    "energy_shield_percent": {
        "mod_ids": [
            "LocalIncreasedEnergyShieldPercent8",  # Unfaltering (T1)
            "LocalIncreasedEnergyShieldPercent7_",  # Unassailable (T2)
            "LocalIncreasedEnergyShieldPercent6",  # Indomitable (T3)
            "LocalIncreasedEnergyShieldPercent5",  # Dauntless (T4)
        ],
        "item_tags": ["shield"],
        "generation_type": "prefix",
        "groups": ["DefencesPercent"],
    },
}

DENSE_FOSSIL_POSITIVE_TAG = "defences"  # Dense Fossil aumenta peso de mods de defesa


if __name__ == "__main__":
    parser = RePoeParser()
    parser.build_local_db(force_download=True)

    # Testar mod de Spell Suppression
    print("\n=== Testando Spell Suppression ===")
    for mod_id in REPOE_MOD_IDS["spell_suppression"]["mod_ids"]:
        data = parser.get_mod_data(mod_id)
        if data:
            print(
                f"{mod_id}: groups={data.get('groups')}, spawn_weights={[sw for sw in data.get('spawn_weights', []) if sw.get('weight', 0) > 0][:3]}"
            )

    # Testar mod de ES%
    print("\n=== Testando ES% Prefix ===")
    for mod_id in REPOE_MOD_IDS["energy_shield_percent"]["mod_ids"][:1]:
        data = parser.get_mod_data(mod_id)
        if data:
            print(
                f"{mod_id}: groups={data.get('groups')}, spawn_weights={data.get('spawn_weights')}"
            )

    # Testar cálculo de pool para shield
    print("\n=== Pool Total para Shield ===")
    total_prefix = parser.get_total_spawn_weight_by_tag(
        "shield", generation_type="prefix"
    )
    total_suffix = parser.get_total_spawn_weight_by_tag(
        "shield", generation_type="suffix"
    )
    print(f"Total prefix weight: {total_prefix}")
    print(f"Total suffix weight: {total_suffix}")
