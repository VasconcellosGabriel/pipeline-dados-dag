"""Gera os arquivos CSV de origem, simulando a extração de um sistema de vendas.

Os dados são sintéticos e determinísticos (semente fixa). Eles já vêm com os
problemas típicos de uma origem real (espaços sobrando, caixa misturada, linhas
reenviadas e alguns itens inválidos) para que o pipeline tenha o que tratar.

    py gerar_dados.py                  # cenário normal   -> dados/raw
    py gerar_dados.py --cenario falha  # origem quebrada  -> dados/raw_falha
"""
from __future__ import annotations

import argparse
import csv
import random
import unicodedata
from datetime import date, timedelta
from pathlib import Path

RAIZ = Path(__file__).resolve().parent

NOMES = ["Ana", "Bruno", "Carla", "Diego", "Eduarda", "Felipe", "Gabriela", "Heitor", "Isabela", "João",
         "Larissa", "Marcos", "Natália", "Otávio", "Paula", "Rafael", "Sofia", "Tiago", "Vitória", "Yuri"]
SOBRENOMES = ["Silva", "Souza", "Oliveira", "Santos", "Pereira", "Lima", "Costa", "Ribeiro", "Almeida", "Carvalho"]
CIDADES = [("Vitória", "ES"), ("Vila Velha", "ES"), ("Serra", "ES"), ("Cariacica", "ES"), ("Guarapari", "ES"),
           ("Rio de Janeiro", "RJ"), ("Belo Horizonte", "MG"), ("São Paulo", "SP")]
PRODUTOS = [
    ("Notebook 14 polegadas", "Informática", 4299.00),
    ("Monitor 27 polegadas", "Informática", 1599.00),
    ("Teclado mecânico", "Informática", 389.90),
    ("Mouse sem fio", "Informática", 129.90),
    ("Smartphone 128 GB", "Telefonia", 2499.00),
    ("Fone Bluetooth", "Telefonia", 299.90),
    ("Carregador rápido 30W", "Telefonia", 119.90),
    ("Capa protetora", "Telefonia", 49.90),
    ("Cafeteira elétrica", "Casa", 349.00),
    ("Air fryer 4L", "Casa", 499.00),
    ("Aspirador robô", "Casa", 1299.00),
    ("Luminária LED", "Casa", 89.90),
    ("Cadeira de escritório", "Escritório", 899.00),
    ("Mesa regulável", "Escritório", 1899.00),
    ("Organizador de cabos", "Escritório", 39.90),
]
INICIO, FIM = date(2026, 9, 1), date(2026, 10, 25)
N_CLIENTES, N_PEDIDOS = 60, 350
STATUS_SUJOS = {"entregue": ["entregue", "Entregue", " ENTREGUE"], "pago": ["pago", "PAGO "], "cancelado": ["cancelado"]}


def sem_acento(texto: str) -> str:
    return unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode()


def gerar(cenario: str, semente: int = 42) -> dict[str, list[dict]]:
    rnd = random.Random(semente)

    clientes = []
    for i in range(1, N_CLIENTES + 1):
        nome = f"{rnd.choice(NOMES)} {rnd.choice(SOBRENOMES)}"
        cidade, uf = rnd.choice(CIDADES)
        clientes.append({
            "id_cliente": i,
            "nome": nome,
            "email": f"{sem_acento(nome).lower().replace(' ', '.')}{i}@exemplo.com",
            "cidade": cidade,
            "uf": uf.lower() if rnd.random() < 0.1 else uf,
            "data_cadastro": (date(2025, 1, 1) + timedelta(days=rnd.randint(0, 540))).isoformat(),
        })

    produtos = [
        {"id_produto": i, "nome": nome, "categoria": categoria + (" " if i % 5 == 0 else ""), "preco_unitario": f"{preco:.2f}"}
        for i, (nome, categoria, preco) in enumerate(PRODUTOS, start=1)
    ]

    pedidos, itens = [], []
    for id_pedido in range(1, N_PEDIDOS + 1):
        status = rnd.choices(["entregue", "pago", "cancelado"], weights=[65, 25, 10])[0]
        pedidos.append({
            "id_pedido": id_pedido,
            "id_cliente": rnd.randint(1, N_CLIENTES),
            "data_pedido": (INICIO + timedelta(days=rnd.randint(0, (FIM - INICIO).days))).isoformat(),
            "status": rnd.choice(STATUS_SUJOS[status]),
        })
        for _ in range(rnd.choice([1, 1, 2, 2, 3, 4])):
            id_produto = rnd.randint(1, len(PRODUTOS))
            itens.append({
                "id_item": len(itens) + 1,
                "id_pedido": id_pedido,
                "id_produto": id_produto,
                "quantidade": rnd.choice([1, 1, 1, 2, 2, 3]),
                "preco_unitario": f"{PRODUTOS[id_produto - 1][2]:.2f}",
                "desconto": rnd.choice(["", "0", "0", "0.05", "0.10"]),
            })

    if cenario == "falha":
        # Pedidos apontando para clientes que não existem e uma data impossível.
        for p in rnd.sample(pedidos, 5):
            p["id_cliente"] = 9999
        pedidos[10]["data_pedido"] = "2026-13-40"
        # Um lote de itens com erro de sinal na quantidade: estoura o limite de quarentena.
        for it in rnd.sample(itens, len(itens) // 8):
            it["quantidade"] = -1

    # Problemas "normais" de origem, que o staging precisa absorver.
    for c in rnd.sample(clientes, 6):
        c["email"] = f"  {c['email'].upper()} "
    for c in rnd.sample(clientes[:N_CLIENTES], 3):  # clientes reenviados com cadastro atualizado
        atualizado = date.fromisoformat(c["data_cadastro"]) + timedelta(days=30)
        clientes.append({**c, "cidade": c["cidade"] + " ", "data_cadastro": atualizado.isoformat()})
    pedidos += [dict(p) for p in rnd.sample(pedidos, 8)]  # extração reenviou alguns pedidos
    for it in rnd.sample(itens, 4):
        it["quantidade"] = 0
    rnd.choice(itens)["id_produto"] = 999

    return {"clientes": clientes, "produtos": produtos, "pedidos": pedidos, "itens_pedido": itens}


def salvar(tabelas: dict[str, list[dict]], destino: Path) -> None:
    destino.mkdir(parents=True, exist_ok=True)
    for nome, linhas in tabelas.items():
        with (destino / f"{nome}.csv").open("w", encoding="utf-8", newline="") as f:
            escritor = csv.DictWriter(f, fieldnames=list(linhas[0]))
            escritor.writeheader()
            escritor.writerows(linhas)
        print(f"{destino.name}/{nome}.csv: {len(linhas)} linhas")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--cenario", choices=["normal", "falha"], default="normal")
    args = parser.parse_args()
    pasta = "raw" if args.cenario == "normal" else "raw_falha"
    salvar(gerar(args.cenario), RAIZ / "dados" / pasta)


if __name__ == "__main__":
    main()
