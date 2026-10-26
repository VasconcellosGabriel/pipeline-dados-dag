-- @tipo: sql
-- @camada: mart
-- @depende_de: fct_vendas, dim_produtos
-- @descricao: Receita e unidades por categoria de produto, com a participação de cada uma no total.
DROP TABLE IF EXISTS mart_vendas_categoria;
CREATE TABLE mart_vendas_categoria AS
SELECT
    d.categoria,
    COUNT(DISTINCT f.id_pedido)      AS pedidos,
    SUM(f.quantidade)                AS unidades,
    ROUND(SUM(f.receita_liquida), 2) AS receita_liquida,
    ROUND(100.0 * SUM(f.receita_liquida) / (SELECT SUM(receita_liquida) FROM fct_vendas), 2) AS participacao_pct
FROM fct_vendas f
JOIN dim_produtos d ON d.id_produto = f.id_produto
GROUP BY d.categoria
ORDER BY receita_liquida DESC;
