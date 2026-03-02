import argparse
import sys

def scan_mode(args):
    print(f"[SCAN MODE] Varredura no mercado...")
    print(f" -> Categoria: {args.category}")
    print(f" -> Arquivo de Pesos: {args.weights}")

def snipe_mode(args):
    print(f"[SNIPE MODE] Busca cirúrgica ativa...")
    print(f" -> Item alvo: {args.item}")
    print(f" -> Teto de Preço: {args.max_price}")

def rog_assist_mode(args):
    print(f"[ROG-ASSIST MODE] Iniciando daemon de monitoramento do clipboard para o NPC Rog...")
    print(f" -> Aguardando atualização do clipboard (Ctrl+C in-game)...")

def main():
    parser = argparse.ArgumentParser(
        description="Hideout Warrior - Path of Exile 3.28 Market & Crafting CLI",
        prog="hideout_warrior"
    )

    subparsers = parser.add_subparsers(dest="command", help="Comandos disponíveis")
    subparsers.required = True

    # ---------- SCAN COMMAND ----------
    scan_parser = subparsers.add_parser("scan", help="Varredura geral buscando itens com alto score (PoB) e baixo preço.")
    scan_parser.add_argument("--category", required=True, help="Tipo/Categoria do item (ex: weapon, armour).")
    scan_parser.add_argument("--weights", required=True, help="Arquivo XML do Path of Building com os pesos dos afixos.")

    # ---------- SNIPE COMMAND ----------
    snipe_parser = subparsers.add_parser("snipe", help="Busca cirúrgica por um item único ou base com filtros específicos.")
    snipe_parser.add_argument("--item", required=True, help="Nome do item a ser buscado.")
    snipe_parser.add_argument("--max-price", required=True, help="Valor máximo para filtragem (teto).")

    # ---------- ROG-ASSIST COMMAND ----------
    rog_parser = subparsers.add_parser("rog-assist", help="Inicia o daemon de monitoramento do clipboard para modo crafting.")

    args = parser.parse_args()

    if args.command == "scan":
        scan_mode(args)
    elif args.command == "snipe":
        snipe_mode(args)
    elif args.command == "rog-assist":
        rog_assist_mode(args)

if __name__ == "__main__":
    main()
