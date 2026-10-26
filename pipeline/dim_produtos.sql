-- @tipo: sql
-- @camada: core
-- @depende_de: stg_produtos
-- @descricao: Dimensão de produtos com categoria e preço de tabela.
DROP TABLE IF EXISTS dim_produtos;
CREATE TABLE dim_produtos AS
SELECT id_produto, nome, categoria, preco_tabela
FROM stg_produtos;
