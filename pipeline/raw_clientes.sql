-- @tipo: extract
-- @camada: raw
-- @fonte: clientes.csv
-- @tentativas: 2
-- @descricao: Ingere o cadastro de clientes exatamente como veio da origem (tudo TEXT), sem limpeza. As colunas sem "_" formam o contrato com o arquivo de origem.
DROP TABLE IF EXISTS raw_clientes;
CREATE TABLE raw_clientes (
    id_cliente    TEXT,
    nome          TEXT,
    email         TEXT,
    cidade        TEXT,
    uf            TEXT,
    data_cadastro TEXT,
    _arquivo      TEXT,
    _carregado_em TEXT DEFAULT (datetime('now', 'localtime'))
);
