-- @tipo: sql
-- @camada: publicacao
-- @depende_de: chk_publicacao, fct_vendas, mart_faturamento_diario, mart_clientes_rfm
-- @descricao: Publica os indicadores do painel de vendas. É a última etapa e só acontece se a reconciliação passar.
DROP TABLE IF EXISTS pub_kpis;
CREATE TABLE pub_kpis AS
SELECT
    (SELECT MIN(data_pedido) FROM fct_vendas)               AS periodo_inicio,
    (SELECT MAX(data_pedido) FROM fct_vendas)               AS periodo_fim,
    (SELECT ROUND(SUM(receita_liquida), 2) FROM fct_vendas) AS receita_liquida,
    (SELECT COUNT(DISTINCT id_pedido) FROM fct_vendas)      AS pedidos,
    (SELECT ROUND(SUM(receita_liquida) / COUNT(DISTINCT id_pedido), 2) FROM fct_vendas) AS ticket_medio,
    (SELECT COUNT(*) FROM mart_clientes_rfm)                AS clientes_compradores,
    (SELECT data_pedido FROM mart_faturamento_diario ORDER BY receita_liquida DESC LIMIT 1) AS melhor_dia,
    datetime('now', 'localtime')                            AS publicado_em;
