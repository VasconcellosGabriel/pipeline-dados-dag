-- @tipo: sql
-- @camada: staging
-- @depende_de: int_itens_classificados
-- @descricao: Itens aprovados na classificação, prontos para compor o fato de vendas.
DROP TABLE IF EXISTS stg_itens_pedido;
CREATE TABLE stg_itens_pedido AS
SELECT id_item, id_pedido, id_produto, quantidade, preco_unitario, desconto
FROM int_itens_classificados
WHERE motivo_rejeicao IS NULL;
