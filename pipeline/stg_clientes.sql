-- @tipo: sql
-- @camada: staging
-- @depende_de: raw_clientes
-- @descricao: Tipa e padroniza clientes (trim, e-mail minúsculo, UF maiúscula) e mantém apenas o cadastro mais recente de cada id_cliente.
DROP TABLE IF EXISTS stg_clientes;
CREATE TABLE stg_clientes AS
WITH tipado AS (
    SELECT
        CAST(TRIM(id_cliente) AS INTEGER) AS id_cliente,
        TRIM(nome)                        AS nome,
        LOWER(TRIM(email))                AS email,
        TRIM(cidade)                      AS cidade,
        UPPER(TRIM(uf))                   AS uf,
        DATE(TRIM(data_cadastro))         AS data_cadastro,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(id_cliente)
            ORDER BY DATE(TRIM(data_cadastro)) DESC
        ) AS ordem
    FROM raw_clientes
    WHERE TRIM(COALESCE(id_cliente, '')) <> ''
)
SELECT id_cliente, nome, email, cidade, uf, data_cadastro
FROM tipado
WHERE ordem = 1;
