-- @tipo: sql
-- @camada: core
-- @depende_de: stg_itens_pedido, stg_pedidos, chk_pedidos, chk_itens_pedido
-- @descricao: Fato de vendas no grão de item, só de pedidos pagos ou entregues, com receita bruta e líquida. Só roda se os dois checks passarem.
DROP TABLE IF EXISTS fct_vendas;
CREATE TABLE fct_vendas AS
SELECT
    i.id_item,
    i.id_pedido,
    p.id_cliente,
    i.id_produto,
    p.data_pedido,
    p.status,
    i.quantidade,
    i.preco_unitario,
    i.desconto,
    ROUND(i.quantidade * i.preco_unitario, 2)                    AS receita_bruta,
    ROUND(i.quantidade * i.preco_unitario * (1 - i.desconto), 2) AS receita_liquida
FROM stg_itens_pedido i
JOIN stg_pedidos p ON p.id_pedido = i.id_pedido
WHERE p.status IN ('pago', 'entregue');
