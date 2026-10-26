-- @tipo: sql
-- @camada: staging
-- @depende_de: raw_produtos
-- @descricao: Tipa o catálogo (preço como REAL), padroniza a categoria e garante um registro por id_produto.
DROP TABLE IF EXISTS stg_produtos;
CREATE TABLE stg_produtos AS
WITH tipado AS (
    SELECT
        CAST(TRIM(id_produto) AS INTEGER)  AS id_produto,
        TRIM(nome)                         AS nome,
        TRIM(categoria)                    AS categoria,
        CAST(TRIM(preco_unitario) AS REAL) AS preco_tabela,
        ROW_NUMBER() OVER (PARTITION BY TRIM(id_produto) ORDER BY rowid DESC) AS ordem
    FROM raw_produtos
    WHERE TRIM(COALESCE(id_produto, '')) <> ''
)
SELECT id_produto, nome, categoria, preco_tabela
FROM tipado
WHERE ordem = 1;
