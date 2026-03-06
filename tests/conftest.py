import pytest
from typing import Dict, List, Any


@pytest.fixture
def mock_leagues_response() -> List[str]:
    return [
        "Standard",
        "Hardcore",
        "Mirage",
        "Sanctum",
        "Affliction",
        "Hardcore Affliction",
        "Ruthless",
    ]


@pytest.fixture
def mock_trade_search_response() -> Dict[str, Any]:
    return {
        "id": "abc123",
        "result": [
            "item_id_1",
            "item_id_2",
            "item_id_3",
            "item_id_4",
            "item_id_5",
        ],
        "total": 5,
        "exact_remain": 0,
    }


@pytest.fixture
def mock_item_detail() -> Dict[str, Any]:
    return {
        "listing": {
            "price": {
                "type": "price",
                "amount": 10.0,
                "currency": "chaos",
            },
            "account": {
                "name": "SellerAccount",
                "online": True,
            },
            "whisper": "@SellerAccount Hi, I would like to buy your Tabula Rasa for 10 chaos",
            "indexed": "2024-01-15T10:30:00Z",
        },
        "item": {
            "id": "item_id_1",
            "baseType": "Tabula Rasa",
            "ilvl": 1,
            "rarity": "unique",
            "explicitMods": [
                "Life: +50",
                "Mana: +20",
            ],
            "implicitMods": [],
            "corrupted": False,
            "fractured": False,
            "influences": {},
        },
    }


@pytest.fixture
def mock_ninja_currency_response() -> Dict[str, float]:
    return {
        "Chaos Orb": 1.0,
        "Divine Orb": 150.0,
        "Exalted Orb": 85.0,
        "Orb of Alchemy": 0.1,
        "Orb of Alteration": 0.02,
        "Orb of Chance": 0.05,
        "Orb of Scouring": 0.03,
        "Orb of Regret": 0.2,
        "Chromatic Orb": 0.01,
        "Jeweller's Orb": 0.02,
        "Mirror of Kalandra": 50000.0,
    }
