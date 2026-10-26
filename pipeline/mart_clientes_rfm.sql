-- @tipo: sql
-- @camada: mart
-- @depende_de: fct_vendas, dim_clientes
-- @descricao: Segmentação RFM (recência, frequência e valor) de cada cliente comprador, usando a última data com vendas como referência.
DROP TABLE IF EXISTS mart_clientes_rfm;
CREATE TABLE mart_clientes_rfm AS
WITH referencia AS (
    SELECT MAX(data_pedido) AS data_ref FROM fct_vendas
),
por_cliente AS (
    SELECT
        id_cliente,
        MAX(data_pedido)               AS ultima_compra,
        COUNT(DISTINCT id_pedido)      AS frequencia,
        ROUND(SUM(receita_liquida), 2) AS valor
    FROM fct_vendas
    GROUP BY id_cliente
),
com_recencia AS (
    SELECT c.*, CAST(julianday(r.data_ref) - julianday(c.ultima_compra) AS INTEGER) AS recencia_dias
    FROM por_cliente c
    CROSS JOIN referencia r
)
SELECT
    c.id_cliente,
    d.nome,
    d.cidade,
    d.uf,
    c.ultima_compra,
    c.recencia_dias,
    c.frequencia,
    c.valor,
    CASE
        WHEN c.recencia_dias <= 20 AND c.frequencia >= 6 THEN 'fiel'
        WHEN c.recencia_dias <= 20                       THEN 'ativo'
        WHEN c.frequencia >= 4                           THEN 'em risco'
        ELSE 'inativo'
    END AS segmento
FROM com_recencia c
LEFT JOIN dim_clientes d ON d.id_cliente = c.id_cliente
ORDER BY c.valor DESC;
