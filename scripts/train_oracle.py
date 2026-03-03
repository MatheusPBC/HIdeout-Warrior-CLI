import sys
import os
import time
from typing import List, Dict

# Adicionar a raiz ao PYTHONPATH para os imports do core funcionarem localmente
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn
import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error

from core.api_integrator import MarketAPIClient
from core.graph_engine import ItemState

def parse_trade_item_to_features(item_data: dict, currency_rates: dict) -> dict:
    """
    Recebe o JSON de um item gerado pela GGG Trade API e extrai as Features Vetorizadas
    esperadas pelo modelo de XGBoost.
    """
    listing = item_data.get("listing", {})
    price_info = listing.get("price", {})
    currency = price_info.get("currency", "")
    amount = price_info.get("amount", 0.0)
    
    # Conversão Universal de Divisas -> Chaos Orb baseada no poe.ninja
    price_chaos = amount
    if currency != "chaos":
        # Correção Semântica das Tags da GGG vs Poe.Ninja
        ninja_key_map = {
            "divine": "Divine Orb",
            "exalted": "Exalted Orb",
            "mirror": "Mirror of Kalandra",
            "alch": "Orb of Alchemy"
        }
        ninja_key = ninja_key_map.get(currency, currency.title() + " Orb")
        
        if ninja_key in currency_rates:
            price_chaos = amount * currency_rates[ninja_key]
        elif currency == "divine":
            # Hardcoded Fallback para o Standard caso ninja falhe temporariamente
            price_chaos = amount * 125.0 
    
    item = item_data.get("item", {})
    ilvl = item.get("ilvl", 1)
    
    # Influência Lógica
    influences = item.get("influences", {})
    is_influenced = 1 if influences else 0
    is_fractured = 1 if item.get("fractured", False) else 0
    feature_influence = max(is_influenced, is_fractured)
    
    # Parsing de Mods - Heurística Base para o Treino
    mods = item.get("explicitMods", [])
    tier_life = 0
    tier_speed = 0
    total_affixes = len(mods)
    
    for mod in mods:
        mod_lower = mod.lower()
        if "maximum life" in mod_lower:
            tier_life = 1 if "to maximum" in mod_lower else 2
        if "speed" in mod_lower:
            tier_speed = 1 if "increased" in mod_lower else 2
            
    open_affixes = max(0, 6 - total_affixes)
    
    return {
        "is_influenced": feature_influence,
        "ilvl": ilvl,
        "tier_life": tier_life,
        "tier_speed": tier_speed,
        "open_affixes": open_affixes,
        "price_chaos": round(price_chaos, 1)
    }

def fetch_training_data(target_bases: List[str], items_per_base: int = 500) -> pd.DataFrame:
    """
    Faz consultas Live GGG Trade API com rate limit respeitado e extrai os itens pro DataSet.
    """
    client = MarketAPIClient(league="Standard")
    currency_rates = client.sync_ninja_economy()
    
    dataset = []
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TextColumn("({task.completed}/{task.total} iter)"),
    ) as progress:
    
        overall_task = progress.add_task("[cyan]Comunicação com API GGG...", total=len(target_bases) * (items_per_base // 10))
        
        for base_type in target_bases:
            progress.update(overall_task, description=f"[cyan]A Sacar Mercado: {base_type}...")
            
            query = {
                "query": {
                    "status": {"option": "online"},
                    "type": base_type,
                    "filters": {
                        "trade_filters": {
                            "filters": {
                                "price": {"min": 1} # Item precisa de ter buyout
                            }
                        }
                    }
                },
                "sort": {"price": "asc"}
            }
            
            # Buscar os Metadados / Hash IDs do Filtro
            query_id, result_ids = client.search_items(query)
            if not query_id or not result_ids:
                progress.console.print(f"[yellow]⚠️ Sem liquidez atual para {base_type}.")
                continue
                
            # Limitar a paginação para N elementos
            result_ids = result_ids[:items_per_base]
            
            # Request Batching GET -> Puxar blocos de 10 em 10 IDs exatos
            batch_size = 10
            for i in range(0, len(result_ids), batch_size):
                batch_ids = result_ids[i:i+batch_size]
                details = client.fetch_item_details(batch_ids, query_id)
                
                for item_json in details:
                    # Filtra preços corruptos / trocas (ex: WTB)
                    if not item_json.get("listing", {}).get("price", {}).get("amount"):
                        continue
                        
                    features = parse_trade_item_to_features(item_json, currency_rates)
                    dataset.append(features)
                    
                progress.advance(overall_task, advance=1)
                
    return pd.DataFrame(dataset)

def train_xgboost_oracle():
    print("🚀 [Fase 6.1] Iniciando Treino do XGBoost com Dados Reais da Trade API...")
    target_bases = ["Imbued Wand", "Spine Bow", "Titanium Spirit Shield", "Vaal Regalia", "Hubris Circlet", "Sadist Garb"]
    
    # O GGG Rate Limit aciona demorados limites (timeout 60s) em largas extrações.
    # Puxaremos 500 de cada base x 6 = 3000 itens (respeitando a requisição de 2k-5k itens).
    df = fetch_training_data(target_bases, items_per_base=500)
    
    if len(df) < 50:
         print("❌ Dados insuficientes extraídos (menos que 50). Verifique o GGG Ban IP.")
         sys.exit(1)
         
    print(f"\n📊 Extracção Completa! Dados Válidos Encontrados: {len(df)} listagens.")
    
    # ML Pipeline
    X = df.drop("price_chaos", axis=1)
    y = df["price_chaos"]
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    print("🧠 Injetando Matrizes no XGBoost Regressor...")
    dtrain = xgb.DMatrix(X_train, label=y_train)
    dtest = xgb.DMatrix(X_test, label=y_test)
    
    params = {
        'max_depth': 5,
        'eta': 0.05,
        'objective': 'reg:squarederror',
        'eval_metric': 'rmse'
    }
    
    evals = [(dtrain, 'train'), (dtest, 'eval')]
    
    # Early Stopping Preemptivo para generalizar melhor
    model = xgb.train(params, dtrain, num_rounds=150, evals=evals, early_stopping_rounds=15, verbose_eval=50)
    
    # Avaliação de Erro Real (RMSE)
    preds = model.predict(dtest)
    rmse = np.sqrt(mean_squared_error(y_test, preds))
    
    print(f"\n🎯 [MÉTRICA] Root Mean Square Error (RMSE): {rmse:.2f} Chaos")
    print("↳ Interpretação: Na média, a IA erra a previsão de preços por este valor em Chaos. O mercado é selvagem e volátil, mas a IA guiará o A*!")
    
    # Gravando .xgb final
    os.makedirs("data", exist_ok=True)
    model_path = os.path.join("data", "price_oracle.xgb")
    model.save_model(model_path)
    print(f"✅ [SUCESSO] Cérebro atualizado e injetado com Big Data verdadeiro em {model_path}!")

if __name__ == "__main__":
    train_xgboost_oracle()
