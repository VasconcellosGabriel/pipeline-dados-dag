-- @tipo: sql
-- @camada: staging
-- @depende_de: raw_pedidos
-- @descricao: Tipa os pedidos (datas inválidas viram NULL), normaliza o status e remove as linhas reenviadas pela extração.
DROP TABLE IF EXISTS stg_pedidos;
CREATE TABLE stg_pedidos AS
WITH tipado AS (
    SELECT
        CAST(TRIM(id_pedido) AS INTEGER)  AS id_pedido,
        CAST(TRIM(id_cliente) AS INTEGER) AS id_cliente,
        DATE(TRIM(data_pedido))           AS data_pedido,
        LOWER(TRIM(status))               AS status,
        ROW_NUMBER() OVER (PARTITION BY TRIM(id_pedido) ORDER BY rowid DESC) AS ordem
    FROM raw_pedidos
    WHERE TRIM(COALESCE(id_pedido, '')) <> ''
)
SELECT id_pedido, id_cliente, data_pedido, status
FROM tipado
WHERE ordem = 1;
