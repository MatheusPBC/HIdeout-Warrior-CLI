import os
import json
import requests
from typing import Dict, Any, List, Optional

class RePoeParser:
    """
    Motor de Pesos do RePoE (Path of Exile Repository of Data).
    Fase 1: Data Layer.
    Baixa os mod weights (pesos) estritos do jogo base e converte-os em
    um dicionário cacheado localmente para consultas $O(1)$ assíncronas ultra-rápidas.
    """
    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.mods_url = "https://raw.githubusercontent.com/brather1ng/RePoE/master/RePoE/data/mods.json"
        # Opcionalidade para base_items
        self.base_items_url = "https://raw.githubusercontent.com/brather1ng/RePoE/master/RePoE/data/base_items.json"
        
        self.parsed_weights_path = os.path.join(self.data_dir, "parsed_weights.json")
        self.db: Dict[str, Any] = {}
        
        os.makedirs(self.data_dir, exist_ok=True)
        self._load_local_db()

    def _load_local_db(self):
        """Carrega o banco JSON local de O(1) de pesos se ele existir."""
        if os.path.exists(self.parsed_weights_path):
            try:
                with open(self.parsed_weights_path, 'r', encoding='utf-8') as f:
                    self.db = json.load(f)
            except json.JSONDecodeError:
                self.db = {}

    def _download_raw_data(self, url: str) -> Dict[str, Any]:
        """Faz o download de um DB do repositório RePoE."""
        print(f"📥 [RePoeParser] Baixando dados de: {url} (pode demorar alguns segundos)...")
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"❌ [RePoeParser] Erro fatal de rede ao baixar RePoe: {e}")
            return {}

    def build_local_db(self, force_download: bool = False):
        """
        Constrói o DB local otimizado $O(1)$.
        Itera sobre todos os mods globais e limpa objetos inúteis, guardando apenas mods
        úteis de crafting com tier, tag, mod_id.
        """
        if not force_download and os.path.exists(self.parsed_weights_path):
            print("✅ [RePoeParser] Tabela de pesos O(1) já estruturada. Usando cache local de acesso.")
            return

        raw_mods = self._download_raw_data(self.mods_url)
        if not raw_mods:
            print("❌ [RePoeParser] Falha na construção: Dados brutos vazios.")
            return

        print("⚙️  [RePoeParser] Compactando mais de 100 mil registros do Path of Exile em uma HashTable...")
        parsed_db = {}

        for mod_id, mod_data in raw_mods.items():
            # Pegando as spawn_weights limpas do generation pool do JSON
            generation_weights = mod_data.get("generation_weights", [])
            clean_weights = []
            
            for gw in generation_weights:
                clean_weights.append({
                    "tag": gw.get("tag"),
                    "weight": gw.get("weight")
                })
            
            # Formata a key-value otimizada do afixo
            parsed_db[mod_id] = {
                "mod_id": mod_id,
                # Alguns afixos explícitos do jogo não tem property nomeada de "tier", pegamos label do nome.
                "tier": mod_data.get("name", "Unknown"), 
                "weights": clean_weights,
                # O spawn constraint de tags ex (mana, speed, attack)
                "tags": mod_data.get("tags", []), 
                "mod_group": mod_data.get("group", "Unknown")
            }

        with open(self.parsed_weights_path, 'w', encoding='utf-8') as f:
            json.dump(parsed_db, f, indent=2)
            
        self.db = parsed_db
        print(f"📦 [RePoeParser] Compressão concluída! {len(self.db)} afixos chaveados para cálculos Probabilísticos.")

    def get_weight(self, mod_id: str) -> Optional[List[Dict[str, Any]]]:
        """
        Retorna em $O(1)$ todos os weights e spawn-tags de um determinado Afixo.
        """
        if not self.db:
            print("⚠️ [RePoeParser] Banco de dados em memória vazio. Rodando auto-build...")
            self.build_local_db()
            
        return self.db.get(mod_id, {}).get("weights")

    def get_total_weight_by_tag(self, tag: str, base_type: str = "") -> int:
        """
        Calcula a somatória de peso O(N pequeno) de todos os afixos que carregam
        uma certa tag na geração. (Essencial para calcular 'Dilution' do pool).
        """
        if not self.db:
            self._load_local_db()
            
        total_weight = 0
        for mod_id, mod_data in self.db.items():
            for w in mod_data.get("weights", []):
                # Caso uma das tags de geração do mod combine com a nossa de request.
                if w.get("tag") == tag or tag in mod_data.get("tags", []):
                    total_weight += w.get("weight", 0)
        return total_weight

if __name__ == "__main__":
    parser = RePoeParser()
    parser.build_local_db(force_download=True)
    
    sample_mod = "MovementVelocity1"
    weights = parser.get_weight(sample_mod)
    print(f"Exemplo - Weights do Affix \"{sample_mod}\": {weights}")
