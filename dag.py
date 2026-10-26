"""Orquestrador de transformações SQL organizado como um DAG.

Cada arquivo .sql da pasta pipeline/ é uma tarefa (um nó do grafo). O cabeçalho
do arquivo declara os metadados e as dependências (as arestas):

    -- @tipo: sql                      (extract | sql | check)
    -- @camada: staging
    -- @depende_de: raw_pedidos, stg_clientes
    -- @descricao: O que a etapa faz.

O orquestrador lê as tarefas, valida o grafo (dependências existentes, linhagem
coerente com o SQL e ausência de ciclos), calcula a ordem topológica e executa
cada etapa no SQLite, registrando tudo em meta_execucoes. Se uma etapa falha,
tudo o que depende dela é bloqueado; ramos independentes continuam.

    py dag.py                          # executa o pipeline completo
    py dag.py --validar                # só valida e mostra a ordem
    py dag.py --a-partir-de fct_vendas # reexecuta a tarefa e seus descendentes
    py dag.py --dados dados/raw_falha  # cenário com origem quebrada
"""
from __future__ import annotations

import argparse
import bisect
import csv
import re
import sqlite3
import time
from collections import deque
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

RAIZ = Path(__file__).resolve().parent
TIPOS = ("extract", "sql", "check")
CABECALHO = re.compile(r"^--\s*@(\w+)\s*:\s*(.*)$")


class ErroDAG(Exception):
    """Problema estrutural no grafo: dependência inexistente, ciclo ou linhagem incoerente."""


class FalhaQualidade(Exception):
    """Uma verificação de qualidade encontrou violações."""

    def __init__(self, violacoes: list[tuple]):
        self.violacoes = violacoes
        amostra = "; ".join(" | ".join(map(str, v)) for v in violacoes[:3])
        resto = f" (+{len(violacoes) - 3} outras)" if len(violacoes) > 3 else ""
        super().__init__(f"{len(violacoes)} violação(ões): {amostra}{resto}")


@dataclass
class Tarefa:
    nome: str
    tipo: str
    camada: str
    descricao: str
    depende_de: list[str]
    sql: str
    fonte: str | None = None
    tentativas: int = 1

    @property
    def produz_tabela(self) -> bool:
        return self.tipo != "check"


def ler_tarefa(arquivo: Path) -> Tarefa:
    meta: dict[str, str] = {}
    corpo: list[str] = []
    for linha in arquivo.read_text(encoding="utf-8").splitlines():
        m = CABECALHO.match(linha.strip())
        if m:
            meta[m.group(1)] = m.group(2).strip()
        else:
            corpo.append(linha)

    tipo = meta.get("tipo", "sql")
    if tipo not in TIPOS:
        raise ErroDAG(f"{arquivo.name}: tipo '{tipo}' inválido (use {', '.join(TIPOS)})")
    if tipo == "extract" and "fonte" not in meta:
        raise ErroDAG(f"{arquivo.name}: tarefas extract precisam declarar @fonte")
    return Tarefa(
        nome=arquivo.stem,
        tipo=tipo,
        camada=meta.get("camada", "-"),
        descricao=meta.get("descricao", ""),
        depende_de=[d.strip() for d in meta.get("depende_de", "").split(",") if d.strip()],
        sql="\n".join(corpo).strip(),
        fonte=meta.get("fonte"),
        tentativas=int(meta.get("tentativas", 1)),
    )


def carregar_pipeline(pasta: Path) -> DAG:
    arquivos = sorted(pasta.glob("*.sql"))
    if not arquivos:
        raise ErroDAG(f"nenhuma tarefa .sql em {pasta}")
    return DAG([ler_tarefa(a) for a in arquivos])


def sem_comentarios(sql: str) -> str:
    return re.sub(r"--[^\n]*", "", sql)


class DAG:
    def __init__(self, tarefas: list[Tarefa]):
        self.tarefas: dict[str, Tarefa] = {}
        for t in tarefas:
            if t.nome in self.tarefas:
                raise ErroDAG(f"tarefa duplicada: {t.nome}")
            self.tarefas[t.nome] = t
        self._validar_dependencias()
        self._validar_linhagem()
        self.sucessores = {n: [] for n in self.tarefas}
        for t in self.tarefas.values():
            for d in t.depende_de:
                self.sucessores[d].append(t.nome)
        self.ordem = self._ordenar()

    @property
    def arestas(self) -> list[tuple[str, str]]:
        return [(d, n) for n in self.ordem for d in self.tarefas[n].depende_de]

    def _validar_dependencias(self) -> None:
        for t in self.tarefas.values():
            for d in t.depende_de:
                if d == t.nome:
                    raise ErroDAG(f"{t.nome} depende de si mesma")
                if d not in self.tarefas:
                    raise ErroDAG(f"{t.nome} depende de '{d}', que não existe")

    def _validar_linhagem(self) -> None:
        """Toda tabela lida pelo SQL precisa estar declarada em @depende_de.

        Sem isso o orquestrador poderia rodar uma etapa antes de a tabela que ela
        lê estar pronta. O contrário é permitido: depender de um check sem ler
        nada dele é uma dependência de controle, um "portão" de qualidade.
        """
        tabelas = {n for n, t in self.tarefas.items() if t.produz_tabela}
        for t in self.tarefas.values():
            sql = sem_comentarios(t.sql)
            lidas = {n for n in tabelas - {t.nome} if re.search(rf"\b{n}\b", sql)}
            faltando = sorted(lidas - set(t.depende_de))
            if faltando:
                raise ErroDAG(f"{t.nome} lê {', '.join(faltando)} sem declarar em @depende_de")

    def _ordenar(self) -> list[str]:
        """Ordenação topológica (algoritmo de Kahn). Se sobrar nó, há ciclo."""
        pendentes = {n: len(t.depende_de) for n, t in self.tarefas.items()}
        prontas = sorted(n for n, k in pendentes.items() if k == 0)
        ordem = []
        while prontas:
            n = prontas.pop(0)
            ordem.append(n)
            for s in self.sucessores[n]:
                pendentes[s] -= 1
                if pendentes[s] == 0:
                    bisect.insort(prontas, s)
        if len(ordem) < len(self.tarefas):
            ciclo = self._achar_ciclo(set(self.tarefas) - set(ordem))
            raise ErroDAG("ciclo detectado (cada tarefa depende da seguinte): " + " -> ".join(ciclo))
        return ordem

    def _achar_ciclo(self, restantes: set[str]) -> list[str]:
        # Todo nó que sobrou no Kahn ainda tem uma dependência que também sobrou,
        # então seguir as dependências a partir de qualquer um deles leva a um ciclo.
        caminho: list[str] = []
        n = min(restantes)
        while n not in caminho:
            caminho.append(n)
            n = next(d for d in self.tarefas[n].depende_de if d in restantes)
        return caminho[caminho.index(n):] + [n]

    def niveis(self) -> dict[str, int]:
        """Nível = maior distância até uma fonte. Tarefas do mesmo nível não dependem entre si."""
        nivel: dict[str, int] = {}
        for n in self.ordem:
            nivel[n] = 1 + max((nivel[d] for d in self.tarefas[n].depende_de), default=-1)
        return nivel

    def ondas(self) -> list[list[str]]:
        grupos: dict[int, list[str]] = {}
        for n, k in self.niveis().items():
            grupos.setdefault(k, []).append(n)
        return [grupos[k] for k in sorted(grupos)]

    def descendentes(self, nome: str) -> set[str]:
        vistos: set[str] = set()
        fila = deque([nome])
        while fila:
            for s in self.sucessores[fila.popleft()]:
                if s not in vistos:
                    vistos.add(s)
                    fila.append(s)
        return vistos

    def mermaid(self) -> str:
        linhas = ["flowchart LR"]
        for n in self.ordem:
            linhas.append(f"    {n}{{{{{n}}}}}" if self.tarefas[n].tipo == "check" else f"    {n}[{n}]")
        linhas += [f"    {d} --> {n}" for d, n in self.arestas]
        return "\n".join(linhas)


# ---------------------------------------------------------------- execução

SQL_META = """
CREATE TABLE IF NOT EXISTS meta_execucoes (
    id_execucao TEXT, tarefa TEXT, tipo TEXT, status TEXT, tentativas INTEGER,
    inicio TEXT, fim TEXT, duracao_ms INTEGER, linhas INTEGER, mensagem TEXT
);
CREATE TABLE IF NOT EXISTS meta_violacoes (
    id_execucao TEXT, tarefa TEXT, regra TEXT, detalhe TEXT
);
"""


@dataclass
class Resultado:
    tarefa: str
    status: str  # ok | falhou | bloqueada
    tentativas: int = 0
    inicio: str = ""
    fim: str = ""
    duracao_ms: int = 0
    linhas: int | None = None
    mensagem: str = ""
    violacoes: list[tuple] = field(default_factory=list)


def _rodar_extract(conn: sqlite3.Connection, tarefa: Tarefa, pasta_dados: Path) -> int:
    arquivo = pasta_dados / tarefa.fonte
    with arquivo.open(encoding="utf-8", newline="") as f:
        leitor = csv.DictReader(f)
        registros = list(leitor)
        cabecalho = leitor.fieldnames or []
    try:
        conn.executescript("BEGIN;\n" + tarefa.sql)
        colunas = [c[1] for c in conn.execute(f"PRAGMA table_info({tarefa.nome})")]
        contrato = [c for c in colunas if not c.startswith("_")]
        ausentes = [c for c in contrato if c not in cabecalho]
        if ausentes:
            raise ValueError(f"{arquivo.name} não tem as colunas do contrato: {', '.join(ausentes)}")
        destino = contrato + ["_arquivo"]
        conn.executemany(
            f"INSERT INTO {tarefa.nome} ({', '.join(destino)}) VALUES ({', '.join('?' * len(destino))})",
            ([r[c] for c in contrato] + [arquivo.name] for r in registros),
        )
        conn.execute("COMMIT")
    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise
    return len(registros)


def _rodar_sql(conn: sqlite3.Connection, tarefa: Tarefa, _pasta_dados: Path) -> int:
    # Cada etapa roda numa transação: ou a tabela nova fica pronta, ou a anterior continua intacta.
    try:
        conn.executescript("BEGIN;\n" + tarefa.sql.rstrip().rstrip(";") + ";\nCOMMIT;")
    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise
    return conn.execute(f"SELECT COUNT(*) FROM {tarefa.nome}").fetchone()[0]


def _rodar_check(conn: sqlite3.Connection, tarefa: Tarefa, _pasta_dados: Path) -> int:
    violacoes = conn.execute(tarefa.sql).fetchall()
    if violacoes:
        raise FalhaQualidade(violacoes)
    return 0


RODAR = {"extract": _rodar_extract, "sql": _rodar_sql, "check": _rodar_check}


def _agora() -> str:
    return datetime.now().isoformat(sep=" ", timespec="milliseconds")


def _executar_tarefa(conn: sqlite3.Connection, tarefa: Tarefa, pasta_dados: Path) -> Resultado:
    r = Resultado(tarefa.nome, "falhou", inicio=_agora())
    t0 = time.perf_counter()
    for tentativa in range(1, tarefa.tentativas + 1):
        r.tentativas = tentativa
        try:
            r.linhas = RODAR[tarefa.tipo](conn, tarefa, pasta_dados)
            r.status, r.mensagem = "ok", ""
            break
        except FalhaQualidade as e:
            # Dado inválido não melhora com nova tentativa: falha na hora.
            r.mensagem, r.violacoes = str(e), e.violacoes
            break
        except (sqlite3.Error, OSError, ValueError) as e:
            r.mensagem = f"{type(e).__name__}: {e}"
    r.duracao_ms = round((time.perf_counter() - t0) * 1000)
    r.fim = _agora()
    return r


def _registrar(conn: sqlite3.Connection, id_execucao: str, tarefa: Tarefa, r: Resultado) -> None:
    conn.execute(
        "INSERT INTO meta_execucoes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (id_execucao, r.tarefa, tarefa.tipo, r.status, r.tentativas, r.inicio, r.fim, r.duracao_ms, r.linhas, r.mensagem),
    )
    conn.executemany(
        "INSERT INTO meta_violacoes VALUES (?, ?, ?, ?)",
        [(id_execucao, r.tarefa, *map(str, v[:2])) for v in r.violacoes],
    )


def _linha_log(r: Resultado) -> str:
    marca = {"ok": "[ok]       ", "falhou": "[FALHOU]   ", "bloqueada": "[bloqueada]"}[r.status]
    linhas = f"{r.linhas:>5} linhas" if r.linhas is not None and r.status == "ok" else " " * 12
    texto = f"  {marca} {r.tarefa:<26} {r.duracao_ms:>5} ms {linhas}"
    return f"{texto}  {r.mensagem}" if r.mensagem else texto


def executar(dag: DAG, banco: Path | str, pasta_dados: Path, a_partir_de: str | None = None,
             log=print) -> tuple[str, dict[str, Resultado]]:
    """Executa as tarefas em ordem topológica. Devolve o id da execução e o resultado de cada tarefa."""
    if isinstance(banco, Path):
        banco.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(banco, isolation_level=None)
    conn.executescript(SQL_META)
    id_execucao = datetime.now().strftime("%Y%m%d-%H%M%S-%f")
    selecao = set(dag.ordem) if a_partir_de is None else {a_partir_de} | dag.descendentes(a_partir_de)

    resultados: dict[str, Resultado] = {}
    try:
        for nome in dag.ordem:
            if nome not in selecao:
                continue
            tarefa = dag.tarefas[nome]
            travada_por = [d for d in tarefa.depende_de if d in resultados and resultados[d].status != "ok"]
            if travada_por:
                r = Resultado(nome, "bloqueada", inicio=_agora(), fim=_agora(),
                              mensagem="aguardava " + ", ".join(travada_por))
            else:
                r = _executar_tarefa(conn, tarefa, pasta_dados)
            resultados[nome] = r
            _registrar(conn, id_execucao, tarefa, r)
            log(_linha_log(r))
    finally:
        conn.close()
    return id_execucao, resultados


# ---------------------------------------------------------------- linha de comando

def main(argv: list[str] | None = None) -> int:
    from visualizacao import gerar_html

    parser = argparse.ArgumentParser(description="Executa o pipeline de transformações SQL organizado como DAG.")
    parser.add_argument("--pipeline", type=Path, default=RAIZ / "pipeline", help="pasta com as tarefas .sql")
    parser.add_argument("--dados", type=Path, default=RAIZ / "dados" / "raw", help="pasta com os CSVs de origem")
    parser.add_argument("--banco", type=Path, default=RAIZ / "saida" / "analytics.db", help="arquivo SQLite de destino")
    parser.add_argument("--html", type=Path, default=RAIZ / "saida" / "dag.html", help="visualização gerada")
    parser.add_argument("--validar", action="store_true", help="só valida o grafo e mostra a ordem de execução")
    parser.add_argument("--a-partir-de", metavar="TAREFA", help="reexecuta a tarefa e tudo o que depende dela")
    parser.add_argument("--mermaid", action="store_true", help="imprime o grafo em sintaxe Mermaid e sai")
    args = parser.parse_args(argv)

    try:
        dag = carregar_pipeline(args.pipeline)
    except ErroDAG as e:
        print(f"DAG inválido: {e}")
        return 2
    if args.mermaid:
        print(dag.mermaid())
        return 0
    if args.a_partir_de and args.a_partir_de not in dag.tarefas:
        print(f"tarefa desconhecida: {args.a_partir_de}")
        return 2

    print(f"DAG válido: {len(dag.tarefas)} tarefas, {len(dag.arestas)} dependências, sem ciclos.")
    print("Ondas de execução (tarefas da mesma onda são independentes entre si):")
    for i, onda in enumerate(dag.ondas()):
        print(f"  {i}: {', '.join(onda)}")

    if args.validar:
        gerar_html(dag, None, args.html)
        print(f"\nVisualização: {args.html}")
        return 0

    print(f"\nExecutando com dados de {args.dados} -> {args.banco}")
    id_execucao, resultados = executar(dag, args.banco, args.dados, args.a_partir_de)
    gerar_html(dag, resultados, args.html, id_execucao)

    contagem = {s: sum(r.status == s for r in resultados.values()) for s in ("ok", "falhou", "bloqueada")}
    total_ms = sum(r.duracao_ms for r in resultados.values())
    print(f"\nExecução {id_execucao}: {contagem['ok']} ok, {contagem['falhou']} falharam, "
          f"{contagem['bloqueada']} bloqueadas ({total_ms} ms)")
    if resultados.get("pub_kpis") and resultados["pub_kpis"].status == "ok":
        with closing(sqlite3.connect(args.banco)) as conn:
            cur = conn.execute("SELECT * FROM pub_kpis")
            for coluna, valor in zip((c[0] for c in cur.description), cur.fetchone()):
                print(f"  {coluna:<22} {valor}")
    elif "pub_kpis" in resultados:
        print("  Indicadores NÃO publicados nesta execução; pub_kpis mantém a última versão válida, se houver.")
        print("  Detalhes: SELECT * FROM meta_violacoes WHERE id_execucao = '" + id_execucao + "'")
    print(f"Visualização: {args.html}")
    return 0 if contagem["ok"] == len(resultados) else 1


if __name__ == "__main__":
    raise SystemExit(main())
