import pyperclip
import time
import pprint

class RogOracle:
    """
    Módulo D: Rog Oracle
    Monitora ativamente a área de transferência aguardando entradas in-game do NPC Rog e calcula a melhor ação.
    """
    def __init__(self):
        self.last_clipboard = ""

    def start_monitoring(self):
        """
        Inicia o Daemon de monitoramento bloqueante.
        Deve ser parado via Ctrl+C no terminal.
        """
        print("[ROG-ORACLE] Daemon Iniciado. Copie um item no Path of Exile (Ctrl+C) para analisar.")
        print("[ROG-ORACLE] Pressione Ctrl+C duas vezes para encerrar.")
        
        try:
            while True:
                current_clipboard = pyperclip.paste()
                if current_clipboard != self.last_clipboard:
                    self.last_clipboard = current_clipboard
                    self._handle_clipboard_change(current_clipboard)
                time.sleep(0.5)  # Polling a cada meio segundo
        except KeyboardInterrupt:
            print("\n[ROG-ORACLE] Daemon encerrado pelo usuário.")

    def _handle_clipboard_change(self, text: str):
        # Validação ultra-básica se o texto do clipboard parece um item do PoE
        if "Item Class:" in text and "Rarity:" in text:
            parsed_data = self.parse_item_text(text)
            self.calculate_best_craft_option(parsed_data)

    def parse_item_text(self, clipboard_text: str) -> dict:
        """
        Faz o parse do formato gigantesco de texto gerado pelo Ctrl+C do PoE.
        """
        lines = clipboard_text.splitlines()
        item_data = {
            "name": "Unknown",
            "ilvl": 0,
            "affixes": []
        }
        
        for i, line in enumerate(lines):
            if line.startswith("Item Level:"):
                try:
                    item_data["ilvl"] = int(line.split(":")[1].strip())
                except ValueError:
                    pass
            elif line.startswith("Rarity:"):
                # O nome geralmente vem nas linhas logo depois de Rarity
                if i + 1 < len(lines):
                    item_data["name"] = lines[i+1].strip()
            
            # Detecção de afixos do Rog geralmente baseada em keywords ou tiers textuais
            if "(implicit)" in line:
                item_data["affixes"].append(f"Implicit: {line.replace('(implicit)', '').strip()}")
            elif "(Tier " in line or "(craft)" in line or "(fractured)" in line:
                # Simula uma extração rasa
                item_data["affixes"].append(line.strip())
                
        # Se os afixos explícitos estiverem soltos mas lidos pela API de mods, 
        # esse parsing precisaria de um banco de dados de Regex. Aqui montamos estruturalmente.
        return item_data

    def calculate_best_craft_option(self, item_data: dict):
        """
        Avalia se a base é boa e pondera as ações do Rog.
        """
        print("\n" + "="*50)
        print(f"[ROG-ORACLE] Novo item detectado no clipboard:")
        print(f"  -> Base/Nome: {item_data['name']}")
        print(f"  -> Item Level: {item_data['ilvl']}")
        print(f"  -> Explicit/Implicits Encontrados: {len(item_data['affixes'])}")
        
        if item_data['ilvl'] >= 84:
            print("[ROG-ORACLE] -> VEREDUTO TÁTICO: Base Excelente (iLvl >= 84).")
            print("[ROG-ORACLE] -> AÇÃO SUGERIDA: Focar em Upgrades de Tier e Reroll de Sufixos.")
        else:
            print("[ROG-ORACLE] -> VEREDUTO TÁTICO: Base Medíocre.")
            print("[ROG-ORACLE] -> AÇÃO SUGERIDA: Pular este item (Skip) ou pegar recompensa genérica.")
            
        print("="*50 + "\n")
