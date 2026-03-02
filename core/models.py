from pydantic import BaseModel, Field
from typing import List, Optional

class ItemMeta(BaseModel):
    base_type: str = Field(..., description="Nome exato da base do item (ex: 'Omen Wand')")
    item_class: str = Field(..., description="Classe do item (ex: 'Wand', 'Belt')")
    min_ilvl: int = Field(default=1, description="Item level mínimo exigido")
    influence: List[str] = Field(default_factory=list, description="Tipos de influência (ex: 'Shaper', 'Elder')")

class AffixTarget(BaseModel):
    trade_api_id: str = Field(..., description="ID exato usado pela API oficial de Trade (pseudo ou explicit)")
    description: str = Field(default="", description="Descrição textual do mod para logs/CLI")
    min_tier: int = Field(default=1, description="O Tier mínimo aceitável (1 é o melhor, geralmente)")
    is_fractured_acceptable: bool = Field(default=False, description="Se for True, a base pode ser comprada com o afixo fraturado")
    weight: int = Field(default=0, description="Peso heurístico dinâmico atribuído ao afixo (1-100)")

class TargetStats(BaseModel):
    prefixes: List[AffixTarget] = Field(default_factory=list)
    suffixes: List[AffixTarget] = Field(default_factory=list)

class Constraints(BaseModel):
    open_prefixes_required: int = Field(default=0, description="Requisito de afixos de Prefixo livres ao final do craft (ex: pra craftar vida na bancada)")
    open_suffixes_required: int = Field(default=0, description="Requisito de afixos de Sufixo livres")
    max_crafting_budget_divines: float = Field(default=float('inf'), description="Orçamento máximo (EV) em Divines antes do Motor Grafo abandonar o craft")

class CraftingTargetSchema(BaseModel):
    """
    Data Contract mestre que define o "Estado Final" (Nó de Destino) 
    para o Módulo B (Graph Engine) e os pesos para o Módulo A (Hospital Snipe).
    """
    item_meta: ItemMeta
    target_stats: TargetStats
    constraints: Constraints
