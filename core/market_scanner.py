from typing import List, Dict, Optional
import math

from core.api_integrator import MarketAPIClient
from core.graph_engine import ItemState
from core.ml_oracle import PricePredictor

class OnDemandScanner:
    """
    Fase 7: Scanner de Mercado Sob Demanda.
    Permite busca ativa via filtros na GGG Trade API e avalia automaticamente o "EV" 
    do item utilizando o Oráculo de Machine Learning.
    """
    def __init__(self, league: str = "Standard"):
        self.api_client = MarketAPIClient(league=league)
        self.oracle = PricePredictor()
        
        # Puxamos as taxas atuais do ninja para conversões Chaos <-> Divine na Arbitragem
        self.currency_rates = self.api_client.sync_ninja_economy()

    def build_trade_query(self, item_class: str = "", ilvl_min: int = 1, rarity: str = "rare", is_influenced: bool = False) -> dict:
        """
        Constrói o payload em tempo-real para o POST /api/trade/search
        """
        query: dict = {
            "query": {
                "status": {"option": "online"},
                "filters": {
                    "trade_filters": {
                        "filters": {
                            "price": {"min": 1} # Item precisa de Buyout Listado
                        }
                    },
                    "type_filters": {
                        "filters": {
                            "rarity": {"option": rarity}
                        }
                    },
                    "misc_filters": {
                        "filters": {
                            "ilvl": {"min": ilvl_min}
                        }
                    }
                }
            },
            "sort": {"price": "asc"}
        }

        # Match de tipo opcional
        if item_class:
             query["query"]["type"] = item_class

        if is_influenced:
             # Filtro psuedo de influencer, qualquer influencer. 
             query["query"]["filters"]["misc_filters"]["filters"]["influence"] = {"option": "true"}

        return query

    def parse_api_to_state(self, item_json: dict) -> Optional[ItemState]:
        """
        Transforma o payload JSON do servidor no nosso Hashable 'ItemState' nativo do A* Graph.
        """
        item_data = item_json.get("item", {})
        if not item_data:
            return None
            
        base_type = item_data.get("baseType", "Unknown Base")
        ilvl = item_data.get("ilvl", 1)
        
        # Simplificação: No PoE JSON explicit mods formatados já contêm o nome.
        # Precisamos parseá-los para a representação do Graph.
        raw_mods = item_data.get("explicitMods", [])
        prefixes = set()
        suffixes = set()
        
        # Distinção entre prefixo e sufixo é falha na API crua sem cross-reference,
        # vamos usar o total de affixes para feature eng do XGBoost de forma cega para este MVP.
        # Numa DAG real buscaríamos no RePoe se cada mod listado é P ou S.
        for i, mod in enumerate(raw_mods):
            if i % 2 == 0:
                prefixes.add(mod)
            else:
                suffixes.add(mod)
                
        is_fractured = True if item_data.get("fractured", False) or item_data.get("influences", {}) else False
        
        return ItemState(
            base_type=base_type,
            ilvl=ilvl,
            prefixes=frozenset(prefixes),
            suffixes=frozenset(suffixes),
            is_fractured=is_fractured
        )

    def extract_price_chaos(self, listing_json: dict) -> float:
        """
        Converte o preço de listagem do Item da conta do utilizador para a unidade padrão Chaos Orb.
        """
        price_info = listing_json.get("price", {})
        currency = price_info.get("currency", "")
        amount = float(price_info.get("amount", 0.0))
        
        if currency == "chaos":
            return amount
            
        # Padrão GGG Trade para Poe.Ninja (Semântica)
        ninja_key_map = {
            "divine": "Divine Orb",
            "exalted": "Exalted Orb",
            "mirror": "Mirror of Kalandra",
            "alch": "Orb of Alchemy"
        }
        
        ninja_key = ninja_key_map.get(currency, currency.title() + " Orb")
        if ninja_key in self.currency_rates:
             return amount * self.currency_rates[ninja_key]
             
        # Fallbacks em caso de indisponibilidade ninja
        if currency == "divine": return amount * 125.0
        
        return amount

    def run_scan(self, item_class: str = "", ilvl_min: int = 1, rarity: str = "rare", max_items: int = 30) -> List[Dict]:
        """
        Executa uma pesquisa on-demand. Avalia listagens com a IA, e retorna Arbitragens Livres (Profit > 0).
        Retorna: Lista de Dicts [{base, listed_price, ml_value, profit, whisper}] ordenada por Profit DESC.
        """
        query = self.build_trade_query(item_class, ilvl_min, rarity, False)
        
        query_id, result_ids = self.api_client.search_items(query)
        if not query_id or not result_ids:
            return []

        # Paginação controlada e respeitosa
        process_limit = min(max_items, len(result_ids))
        target_ids = result_ids[:process_limit]
        
        evaluated_items = []
        batch_size = 10
        
        for i in range(0, len(target_ids), batch_size):
             batch = target_ids[i:i+batch_size]
             details = self.api_client.fetch_item_details(batch, query_id)
             
             for item_json in details:
                 listing = item_json.get("listing", {})
                 whisper = listing.get("whisper", "")
                 if not whisper: continue
                 
                 listed_price_chaos = self.extract_price_chaos(listing)
                 
                 state = self.parse_api_to_state(item_json)
                 if not state: continue
                 
                 # THE MAGIC: Evaluates ML prediction
                 ml_value = self.oracle.predict_value(state)
                 profit = ml_value - listed_price_chaos
                 
                 evaluated_items.append({
                     "base_type": state.base_type,
                     "ilvl": state.ilvl,
                     "listed_price": round(listed_price_chaos, 1),
                     "ml_value": round(ml_value, 1),
                     "profit": round(profit, 1),
                     "whisper": whisper
                 })

        # Sort: Margens mais lucrosas no TOP
        evaluated_items.sort(key=lambda x: x["profit"], reverse=True)
        return evaluated_items
