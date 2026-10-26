-- @tipo: sql
-- @camada: staging
-- @depende_de: raw_itens_pedido, stg_pedidos, stg_produtos
-- @descricao: Tipa todos os itens e classifica cada um: motivo_rejeicao fica NULL para itens válidos ou explica por que o item não pode seguir.
DROP TABLE IF EXISTS int_itens_classificados;
CREATE TABLE int_itens_classificados AS
WITH tipado AS (
    SELECT
        CAST(TRIM(id_item) AS INTEGER)     AS id_item,
        CAST(TRIM(id_pedido) AS INTEGER)   AS id_pedido,
        CAST(TRIM(id_produto) AS INTEGER)  AS id_produto,
        CAST(TRIM(quantidade) AS INTEGER)  AS quantidade,
        CAST(TRIM(preco_unitario) AS REAL) AS preco_unitario,
        CAST(COALESCE(NULLIF(TRIM(desconto), ''), '0') AS REAL) AS desconto
    FROM raw_itens_pedido
)
SELECT
    t.*,
    CASE
        WHEN t.quantidade <= 0                THEN 'quantidade não positiva'
        WHEN t.preco_unitario < 0             THEN 'preço negativo'
        WHEN t.desconto NOT BETWEEN 0 AND 0.5 THEN 'desconto fora do intervalo'
        WHEN p.id_pedido IS NULL              THEN 'pedido inexistente'
        WHEN pr.id_produto IS NULL            THEN 'produto inexistente'
    END AS motivo_rejeicao
FROM tipado t
LEFT JOIN stg_pedidos  p  ON p.id_pedido   = t.id_pedido
LEFT JOIN stg_produtos pr ON pr.id_produto = t.id_produto;
