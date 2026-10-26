-- @tipo: sql
-- @camada: mart
-- @depende_de: fct_vendas
-- @descricao: Faturamento por dia: pedidos, unidades, receita bruta e líquida e ticket médio.
DROP TABLE IF EXISTS mart_faturamento_diario;
CREATE TABLE mart_faturamento_diario AS
SELECT
    data_pedido,
    COUNT(DISTINCT id_pedido)                                  AS pedidos,
    SUM(quantidade)                                            AS unidades,
    ROUND(SUM(receita_bruta), 2)                               AS receita_bruta,
    ROUND(SUM(receita_liquida), 2)                             AS receita_liquida,
    ROUND(SUM(receita_liquida) / COUNT(DISTINCT id_pedido), 2) AS ticket_medio
FROM fct_vendas
GROUP BY data_pedido
ORDER BY data_pedido;
