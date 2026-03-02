import pyperclip

class Broker:
    \"\"\"
    Módulo D: The Broker
    Formata strings de 'Direct Whisper' e injeta na área de transferência para colagem rápida no jogo.
    \"\"\"
    def __init__(self):
        pass

    def format_whisper(self, seller_name: str, item_name: str, listing_price: str, stash_tab: str, left: int, top: int) -> str:
        return f"@{{seller_name}} Hi, I would like to buy your {{item_name}} listed for {{listing_price}} in Mirage (stash tab \\\"{{stash_tab}}\\\"; position: left {{left}}, top {{top}})"

    def inject_to_clipboard(self, message: str):
        pyperclip.copy(message)
