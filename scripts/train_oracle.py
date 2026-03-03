import sys
import os
import random
try:
    import pandas as pd
    import xgboost as xgb
    from sklearn.model_selection import train_test_split
except ImportError:
    print("❌ Erro: Algumas dependências de Machine Learning (pandas, scikit-learn, xgboost) não estão instaladas.")
    print("Por favor, execute: pip install -r requirements.txt")
    sys.exit(1)

def generate_mock_dataset(num_samples: int = 5000) -> pd.DataFrame:
    """
    Simula uma grande extração de dados da API do poe.ninja para Items de Path of Exile.
    
    Características vetorizadas:
    - is_influenced: 0 ou 1
    - tier_life: 0 a 8 (onde 1 é o melhor, 0 significa que não tem)
    - tier_speed: 0 a 8
    - open_affixes: 0 a 6
    - item_level: 1 a 100
    
    A nossa simulação de "Target Variable" (y) será o 'price_chaos' gerado a partir de lógicas.
    """
    print(f"[{time.strftime('%H:%M:%S')}] Gerando um Dataset Sintético com {num_samples} itens...")
    
    data = []
    for _ in range(num_samples):
        is_influenced = random.choices([0, 1], weights=[0.8, 0.2])[0]
        ilvl = random.randint(60, 86)
        tier_life = random.choices([0, 1, 2, 3, 4, 5, 6, 7], weights=[0.5, 0.05, 0.05, 0.1, 0.1, 0.1, 0.05, 0.05])[0]
        tier_speed = random.choices([0, 1, 2, 3, 4, 5], weights=[0.6, 0.05, 0.05, 0.1, 0.1, 0.1])[0]
        open_affixes = random.randint(0, 6)
        
        # Simulação Lógica do Mercado (Como os jogadores valorizam isso em Chaos)
        # Bases High level valem um pouco mais
        price = max(1.0, (ilvl - 80) * 2) if ilvl > 80 else 1.0
        
        # Influência multiplica valor base
        if is_influenced:
             price *= 1.5
             
        # Tiers de Life (Tier 1 soma +100 chaos, Tier 4 +10 chaos)
        if tier_life == 1: price += 150.0
        elif tier_life == 2: price += 80.0
        elif tier_life in [3, 4]: price += 20.0
             
        # Sinergia Matadora (T1 Life + T1 Speed vale fortuna)
        if tier_life == 1 and tier_speed == 1:
            price += 1500.0  # Jackpot Mod! Synergy Overcharge.
        elif tier_speed == 1:
            price += 80.0
            
        # Ter espaço para craftar algo é bom, mas itens full vazios não prestam
        if open_affixes == 1: price += 30.0
        
        # Inserindo algum ruído Gaussiano pra simular oscilação de mercado humano
        price = max(1.0, price * random.uniform(0.85, 1.15))
        
        data.append({
            "is_influenced": is_influenced,
            "ilvl": ilvl,
            "tier_life": tier_life,
            "tier_speed": tier_speed,
            "open_affixes": open_affixes,
            "price_chaos": round(price, 1)
        })
        
    return pd.DataFrame(data)


def train_xgboost_oracle():
    import time
    
    # 1. Dataset Generation
    df = generate_mock_dataset(num_samples=10000)
    
    print(f"[{time.strftime('%H:%M:%S')}] Split de Treinamento e Teste (80/20)...")
    X = df.drop("price_chaos", axis=1)
    y = df["price_chaos"]
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # 2. Setup XGBoost
    print(f"[{time.strftime('%H:%M:%S')}] Compilando Modelo Regressivo XGBoost...")
    # Converter para tipo DMatrix matricial de alta performance (Memória C)
    dtrain = xgb.DMatrix(X_train, label=y_train)
    dtest = xgb.DMatrix(X_test, label=y_test)
    
    # Hiperparâmetros amigáveis para previsão de finanças/economia gameficada
    params = {
        'max_depth': 5,            # Árvores nem tão rasas nem tão perigosas
        'eta': 0.1,                # Learning rate
        'objective': 'reg:squarederror', # Queremos prever o número absoluto de Chaos
        'eval_metric': 'rmse'      # Root Mean Square Error
    }
    
    evals = [(dtrain, 'train'), (dtest, 'eval')]
    
    # 3. Training Loop
    print(f"[{time.strftime('%H:%M:%S')}] Iniciando Boosting nas Árvores de Decisão...")
    num_rounds = 100
    model = xgb.train(params, dtrain, num_rounds, evals, early_stopping_rounds=10, verbose_eval=10)
    
    # 4. Save Artifact
    os.makedirs("data", exist_ok=True)
    model_path = os.path.join("data", "price_oracle.xgb")
    model.save_model(model_path)
    print(f"\n✅ [SUCESSO] Oráculo de Inteligência Artificial Treinado!")
    print(f"💾 Modelo Gravado fisicamente em: {model_path}")
    print("O Módulo do GraphEngine carregará este cérebro automaticamente se existir.")


if __name__ == "__main__":
    train_xgboost_oracle()
