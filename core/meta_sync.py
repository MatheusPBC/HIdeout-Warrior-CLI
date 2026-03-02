import os
import json
import time
import requests
from typing import Dict

class PoeNinjaScraper:
    """
    Módulo C: Meta-Sync.
    Integração com a API do poe.ninja para elencar a economia atual da liga,
    provendo O(1) reads do cache local de tempo de vida 1-Hora para a motor de pathfinding.
    """

    CACHE_FILE = "data/market_prices.json"
    CACHE_EXPIRATION_SECONDS = 3600 # 1 Hora

    def __init__(self, league: str = "Mirage"):
        self.league = league
        self.headers = {
            "User-Agent": "HideoutWarrior-CLI/1.0",
            "Accept": "application/json"
        }
        
    def _is_cache_valid(self) -> bool:
        """Verifica se o arquivo de cache existe e tem menos de 1 hora de vida."""
        if not os.path.exists(self.CACHE_FILE):
            return False
            
        file_mtime = os.path.getmtime(self.CACHE_FILE)
        current_time = time.time()
        
        return (current_time - file_mtime) < self.CACHE_EXPIRATION_SECONDS

    def _fetch_endpoint(self, url: str, is_currency: bool = False) -> Dict[str, float]:
        """
        Faz o GET para um endpoint do poe.ninja e extrai o nome do item 
        e seu valor equivalente em Chaos.
        """
        prices = {}
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            if response.status_code == 200:
                data = response.json()
                lines = data.get("lines", [])
                
                for item in lines:
                    if is_currency:
                        name = item.get("currencyTypeName")
                        # Em currency, chaos equivalent vem direto em receive/pay ou chaosEquivalent
                        price = item.get("chaosEquivalent", 0.0) 
                    else:
                        name = item.get("name")
                        price = item.get("chaosValue", 0.0)
                        
                    if name and price > 0:
                        prices[name] = float(price)
            else:
                print(f"[META-SYNC] Falha HTTP {response.status_code} na URL: {url}")
                
        except Exception as e:
            print(f"[META-SYNC] Erro consultando poe.ninja: {e}")
            
        return prices

    def sync_market_data(self) -> bool:
        """
        Sincroniza Currency, Essences e Fossils do poe.ninja.
        Se o cache for válido, skipa a Request. Senão, recarrega e salva no cache JSON.
        """
        if self._is_cache_valid():
            print("[META-SYNC] Cache local está atualizado (< 1 hora). Sincronização pulada.")
            return True
            
        print("[META-SYNC] Cache expirado ou não encontrado. Baixando dados do mercado (poe.ninja)...")
        
        # Endpoints
        currency_url = f"https://poe.ninja/api/data/currencyoverview?league={self.league}&type=Currency"
        essence_url = f"https://poe.ninja/api/data/itemoverview?league={self.league}&type=Essence"
        fossil_url = f"https://poe.ninja/api/data/itemoverview?league={self.league}&type=Fossil"
        
        consolidated_market: Dict[str, float] = {}
        
        # Faz os fetches
        currency_data = self._fetch_endpoint(currency_url, is_currency=True)
        essence_data = self._fetch_endpoint(essence_url, is_currency=False)
        fossil_data = self._fetch_endpoint(fossil_url, is_currency=False)
        
        # Merges dicts via update()
        consolidated_market.update(currency_data)
        consolidated_market.update(essence_data)
        consolidated_market.update(fossil_data)
        
        # Salva em Disco
        try:
            os.makedirs(os.path.dirname(self.CACHE_FILE), exist_ok=True)
            with open(self.CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(consolidated_market, f, indent=4)
            print(f"[META-SYNC] Sucesso! Foram salvos {len(consolidated_market)} itens em {self.CACHE_FILE}.")
            return True
        except IOError as e:
            print(f"[META-SYNC] Erro escrevendo cache no disco: {e}")
            return False

def get_price(item_name: str) -> float:
    """
    Função pública global Helper (Complexidade O(1)) 
    para leitura ágil do cache por outras classes da aplicação.
    """
    cache_path = PoeNinjaScraper.CACHE_FILE
    if not os.path.exists(cache_path):
        return 0.0
        
    try:
        with open(cache_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data.get(item_name, 0.0)
    except Exception:
        return 0.0
