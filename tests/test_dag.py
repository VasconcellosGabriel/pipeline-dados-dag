import sqlite3
import sys
from contextlib import closing
import tempfile
import unittest
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))

from dag import DAG, ErroDAG, Tarefa, carregar_pipeline, executar  # noqa: E402


def tarefa(nome, depende_de=(), sql=None, tipo="sql"):
    return Tarefa(nome=nome, tipo=tipo, camada="-", descricao="", depende_de=list(depende_de),
                  sql=sql or f"DROP TABLE IF EXISTS {nome}; CREATE TABLE {nome} AS SELECT 1 AS x;")


def silencioso(_linha):
    pass


class TestEstrutura(unittest.TestCase):
    def test_pipeline_real_respeita_dependencias(self):
        dag = carregar_pipeline(RAIZ / "pipeline")
        posicao = {n: i for i, n in enumerate(dag.ordem)}
        for origem, destino in dag.arestas:
            self.assertLess(posicao[origem], posicao[destino], f"{origem} deveria rodar antes de {destino}")
        self.assertEqual(dag.ordem[-1], "pub_kpis")

    def test_ciclo_e_rejeitado(self):
        with self.assertRaisesRegex(ErroDAG, "ciclo"):
            DAG([
                tarefa("a", ["c"], "SELECT 1"),
                tarefa("b", ["a"], "SELECT 1"),
                tarefa("c", ["b"], "SELECT 1"),
            ])

    def test_dependencia_inexistente(self):
        with self.assertRaisesRegex(ErroDAG, "não existe"):
            DAG([tarefa("a", ["fantasma"])])

    def test_linhagem_nao_declarada(self):
        with self.assertRaisesRegex(ErroDAG, "b lê a sem declarar"):
            DAG([tarefa("a"), tarefa("b", [], "CREATE TABLE b AS SELECT * FROM a")])

    def test_ondas_agrupam_tarefas_independentes(self):
        dag = DAG([tarefa("a"), tarefa("b"), tarefa("c", ["a", "b"], "CREATE TABLE c AS SELECT * FROM a, b")])
        self.assertEqual(dag.ondas(), [["a", "b"], ["c"]])


class TestExecucao(unittest.TestCase):
    def test_falha_bloqueia_apenas_descendentes(self):
        dag = DAG([
            tarefa("a"),
            tarefa("chk", ["a"], "SELECT 'regra', 'detalhe' FROM a", tipo="check"),
            tarefa("b", ["a", "chk"], "DROP TABLE IF EXISTS b; CREATE TABLE b AS SELECT * FROM a;"),
            tarefa("c", ["a"], "DROP TABLE IF EXISTS c; CREATE TABLE c AS SELECT * FROM a;"),
        ])
        _, r = executar(dag, ":memory:", Path("."), log=silencioso)
        self.assertEqual(r["chk"].status, "falhou")
        self.assertEqual(r["b"].status, "bloqueada")
        self.assertEqual(r["c"].status, "ok")

    def test_erro_de_sql_desfaz_a_etapa(self):
        with tempfile.TemporaryDirectory() as tmp:
            banco = Path(tmp) / "t.db"
            ok = DAG([tarefa("a")])
            executar(ok, banco, Path("."), log=silencioso)
            quebrada = DAG([tarefa("a", [], "DROP TABLE IF EXISTS a; CREATE TABLE a AS SELECT * FROM nao_existe;")])
            _, r = executar(quebrada, banco, Path("."), log=silencioso)
            self.assertEqual(r["a"].status, "falhou")
            with closing(sqlite3.connect(banco)) as conn:  # o DROP foi revertido: a versão anterior continua lá
                self.assertEqual(conn.execute("SELECT x FROM a").fetchone(), (1,))

    def test_pipeline_real_e_idempotente(self):
        dag = carregar_pipeline(RAIZ / "pipeline")
        with tempfile.TemporaryDirectory() as tmp:
            banco = Path(tmp) / "t.db"
            contagens = []
            for _ in range(2):
                _, r = executar(dag, banco, RAIZ / "dados" / "raw", log=silencioso)
                self.assertTrue(all(x.status == "ok" for x in r.values()))
                with closing(sqlite3.connect(banco)) as conn:
                    contagens.append(conn.execute(
                        "SELECT (SELECT COUNT(*) FROM fct_vendas), (SELECT receita_liquida FROM pub_kpis)").fetchone())
            self.assertEqual(contagens[0], contagens[1])

    def test_cenario_de_falha_nao_publica(self):
        dag = carregar_pipeline(RAIZ / "pipeline")
        _, r = executar(dag, ":memory:", RAIZ / "dados" / "raw_falha", log=silencioso)
        self.assertEqual(r["chk_pedidos"].status, "falhou")
        self.assertEqual(r["chk_itens_pedido"].status, "falhou")
        self.assertEqual(r["pub_kpis"].status, "bloqueada")
        self.assertEqual(r["dim_clientes"].status, "ok")

    def test_reexecucao_parcial(self):
        dag = carregar_pipeline(RAIZ / "pipeline")
        with tempfile.TemporaryDirectory() as tmp:
            banco = Path(tmp) / "t.db"
            executar(dag, banco, RAIZ / "dados" / "raw", log=silencioso)
            _, r = executar(dag, banco, RAIZ / "dados" / "raw", a_partir_de="fct_vendas", log=silencioso)
            self.assertEqual(set(r), {"fct_vendas"} | dag.descendentes("fct_vendas"))
            self.assertTrue(all(x.status == "ok" for x in r.values()))


if __name__ == "__main__":
    unittest.main()
