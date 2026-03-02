import time
import requests
from typing import List, Dict, Tuple, Any

class GGGTradeAPI:
    """
    Módulo A: API Oficial da Grinding Gear Games (Trade).
    Gerencia consultas ao Path of Exile Trade respeitando rigidamente as políticas de Rate Limit e ToS.
    """
    def __init__(self, league: str = "Mirage", user_agent: str = "HideoutWarrior_CLI/1.0 (Contact: seu_email@example.com)"):
        self.league = league
        self.base_url = "https://www.pathofexile.com/api/trade"
        self.headers = {
            "User-Agent": user_agent,
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def _handle_rate_limits(self, response: requests.Response):
        """
        Lê os headers de rate limit (x-rate-limit-ip, x-rate-limit-ip-state) da API da GGG.
        Se a requisição retornar HTTP 429 (Too Many Requests), bloqueia a execução com time.sleep
        pelo tempo especificado no cabeçalho Retry-After.
        """
        # Exibe as políticas para debug se necessário
        # rate_limit_policy = response.headers.get("x-rate-limit-ip")
        # rate_limit_state = response.headers.get("x-rate-limit-ip-state")
        
        if response.status_code == 429:
            # Pela diretriz da GGG, a punição fica no Retry-After
            retry_after = response.headers.get("Retry-After", 60)
            try:
                wait_seconds = int(retry_after)
            except ValueError:
                wait_seconds = 60
                
            print(f"[API] HTTP 429: Rate Limit atingido! Pausando execução obrigatoriamente por {wait_seconds} segundos...")
            time.sleep(wait_seconds)
            return

    def search_items(self, query_json: dict) -> Tuple[str, List[str]]:
        """
        Filtra os itens na liga especificada através de um payload JSON.
        Faz um POST para /search/{league}.
        Retorna uma Tupla contendo o 'query_id' e uma lista de 'item_ids'.
        """
        url = f"{self.base_url}/search/{self.league}"
        
        # Delay de Civilidade (1s extra para evitar spams rápidos num loop)
        time.sleep(1)
        
        try:
            response = self.session.post(url, json=query_json, timeout=15)
            self._handle_rate_limits(response)
            
            if response.status_code == 200:
                data = response.json()
                query_id = data.get("id", "")
                result_ids = data.get("result", [])
                return query_id, result_ids
            else:
                print(f"[API] Falha no Search (HTTP {response.status_code}): {response.text}")
                return "", []
        except requests.exceptions.RequestException as e:
            print(f"[API] Erro de Rede na rota de Search: {e}")
            return "", []

    def fetch_item_details(self, item_ids: List[str], query_id: str) -> List[Dict[str, Any]]:
        """
        Resgata (Fetch) os dados dos itens formatados na busca via GET.
        A API só permite buscar no máximo 10 itens por vez. 
        Retorna a lista de chaves de metadados do mercado (preço, afixos explícitos).
        """
        if not item_ids:
            return []
            
        # Bloqueio de Segurança para o Limite da GGG
        if len(item_ids) > 10:
            print(f"[API-WARN] A API apenas aceita fetch de até 10 IDs. Truncando requisição.")
            item_ids = item_ids[:10]
            
        ids_string = ",".join(item_ids)
        url = f"{self.base_url}/fetch/{ids_string}?query={query_id}"
        
        # Delay de Civilidade
        time.sleep(1)
        
        try:
            response = self.session.get(url, timeout=15)
            self._handle_rate_limits(response)
            
            if response.status_code == 200:
                data = response.json()
                return data.get("result", [])
            else:
                print(f"[API] Falha no Fetch (HTTP {response.status_code}): {response.text}")
                return []
        except requests.exceptions.RequestException as e:
            print(f"[API] Erro de Rede na rota de Fetch: {e}")
            return []


if __name__ == "__main__":
    print("--- Teste de Integração da API da GGG ---")
    
    # Payload padrão para buscar "Tabula Rasa" (Item Unique comum)
    test_query = {
        "query": {
            "status": {"option": "online"},
            "name": "Tabula Rasa",
            "type": "Simple Robe"
        },
        "sort": {"price": "asc"}
    }
    
    # Vamos usar Standard para o teste caso a liga Mirage não esteja viva na data do snippet
    api = GGGTradeAPI(league="Standard")
    
    print("\n[Passo 1] Realizando Search (POST) de Tabula Rasa Online...")
    query_id, item_hashes = api.search_items(test_query)
    
    if query_id and item_hashes:
        print(f"Sucesso!\nQuery_ID: {query_id}")
        print(f"Total Encontrado: {len(item_hashes)}. Exibindo top 2 hashes de Trade: {item_hashes[:2]}")
        
        print("\n[Passo 2] Realizando Fetch (GET) dos Dados Físicos (Top 2)...")
        items_data = api.fetch_item_details(item_hashes[:2], query_id)
        
        for idx, item_res in enumerate(items_data):
            # A Estrutura do Json do Path of Exile Trade:
            # item_res["item"] guarda stats, mods, links
            # item_res["listing"] guarda valor de mercado e conta do Player
            item_info = item_res.get("item", {})
            listing = item_res.get("listing", {})
            price = listing.get("price", {})
            
            name = item_info.get("name", "Unknown Name")
            seller = listing.get("account", {}).get("lastCharacterName", "Unknown Seller")
            cost = f"{price.get('amount', 0)} {price.get('currency', '?')}"
            
            print(f"({idx+1}) Item: {name} | Seller (Whisper): {seller} | Preço Market: {cost}")
            print(f"    - Explícitos: {item_info.get('explicitMods', [])}")
    else:
        print("Falha na busca ou itens indisponíveis. Cheque conectividade ou validade dos filtros.")
