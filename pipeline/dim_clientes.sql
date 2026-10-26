-- @tipo: sql
-- @camada: core
-- @depende_de: stg_clientes
-- @descricao: Dimensão de clientes para análise. O e-mail (dado pessoal) fica de fora, porque quem consome indicadores não precisa dele.
DROP TABLE IF EXISTS dim_clientes;
CREATE TABLE dim_clientes AS
SELECT id_cliente, nome, cidade, uf, data_cadastro
FROM stg_clientes;
