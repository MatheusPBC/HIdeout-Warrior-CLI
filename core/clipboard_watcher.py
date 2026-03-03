import time
import re
import threading
import pyperclip
from typing import Callable, Optional
from core.graph_engine import ItemState

class ClipboardScanner:
    """
    Os 'olhos' da CLI (Módulo D / Fase 4).
    Roda em background monitorando o pyperclip para capturas de 'Ctrl+C' vindas
    direto de dentro do Path of Exile. 
    Parsea o texto cru gerado pelo jogo em um objeto ItemState computável.
    """
    def __init__(self, callback: Callable[[ItemState], None]):
        self.callback = callback
        self._running = False
        self._last_content = ""

    def start(self):
        """Inicia a thread em background para monitorar o clipboard sem travar o event loop do UI."""
        if self._running:
            return
            
        self._running = True
        self._last_content = pyperclip.paste()
        self._thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._thread.start()

    def stop(self):
        """Mata o daemon de scan."""
        self._running = False

    def _monitor_loop(self):
        while self._running:
            try:
                current_content = pyperclip.paste()
                if current_content != self._last_content:
                    self._last_content = current_content
                    # Dispara o parsing se o texto parece vir do Path of Exile
                    if self._is_poe_item(current_content):
                        state = self._parse_poe_text(current_content)
                        if state:
                            self.callback(state)
            except Exception:
                pass # Erros de sistema de clipboard (bloqueios do S.O) não devem crashar a thread.
                
            time.sleep(0.5)

    def _is_poe_item(self, text: str) -> bool:
        """Heurística simples pra saber se o Ctrl+C foi num item no Path of Exile."""
        return "Item Class:" in text and "Rarity:" in text

    def _parse_poe_text(self, text: str) -> Optional[ItemState]:
        """
        Regex Parser Engine. Converte a parede de texto do PoE em Estrutura O(1).
        
        Exemplo do Header do PoE:
        Item Class: Wands
        Rarity: Rare
        Behemoth Cry
        Omen Wand
        --------
        Item Level: 85
        --------
        """
        try:
            lines = [line.strip() for line in text.splitlines() if line.strip()]
            
            base_type = "Unknown"
            ilvl = 0
            is_fractured = False
            
            # Parsing Básico via Regex Line-by-Line
            for i, line in enumerate(lines):
                if line.startswith("Item Level:"):
                    ilvl_match = re.search(r'\d+', line)
                    if ilvl_match: ilvl = int(ilvl_match.group(0))
                # Base Type geralmente fica após a Raridade e o Nome Mágico/Raro.
                # A lógica robusta para isso requereria checar uma lista nativa de bases do DB.
                # Para simplificação local de strings separadas por '--------'
                
            # Extração de Nome da Base Bruta
            # Regra Simplificada: Se a linha 1 é Item Class, linha 2 Raridade
            rarity_line = next((l for l in lines if l.startswith("Rarity:")), None)
            rarity = rarity_line.split(":")[1].strip() if rarity_line else "Normal"
            
            # O nome da base do item geralmente fica localizado na última string antes do primeiro separator '--------'
            separator_idx = lines.index("--------") if "--------" in lines else 4
            if rarity in ["Rare", "Magic"]:
                # Pula Item Class, Rarity, Nome Fantasia => Base = Index 3 ou 2
                base_str = lines[separator_idx - 1]
                if base_str not in ["Unidentified"]:
                    base_type = base_str
            else:
                 # Normal items usually have the base type at index 2
                 base_type = lines[2] if len(lines) > 2 else "Unknown Base"

            # Parse de Afixos
            prefixes = set()
            suffixes = set()
            
            # Regex do In-Game avançado com (Holding ALT)
            # Prefix Modifier "Thirsty" (Tier: 1) — Mana
            # Suffix Modifier "of the Student" (Tier: 5) — Attributes
            
            for line in lines:
                if "Prefix Modifier" in line:
                    mod_name_match = re.search(r'"([^"]*)"', line)
                    if mod_name_match:
                         # Adotamos um ID simplificado ou mock para conectar com o GraphEngine.
                         prefixes.add(mod_name_match.group(1))
                elif "Suffix Modifier" in line:
                    mod_name_match = re.search(r'"([^"]*)"', line)
                    if mod_name_match:
                         suffixes.add(mod_name_match.group(1))
                
                if "Fractured Item" in line:
                    is_fractured = True

            # Fallback se o usuário não copiou usando "ALT" press in-game
            # A extração de mods num Ctrl+C cru exigiria Reverse-Weight Mapping.
            # O PoE Ninja Trade e players sempre usam Advanced Mod Descriptions habilitadas.

            return ItemState(
                base_type=base_type,
                ilvl=ilvl,
                prefixes=frozenset(prefixes),
                suffixes=frozenset(suffixes),
                is_fractured=is_fractured
            )
            
        except Exception as e:
            # Em caso de Ctlr+C muito cru ou item Legacy mal formatado.
            return None
