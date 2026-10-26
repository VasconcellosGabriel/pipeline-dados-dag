-- @tipo: extract
-- @camada: raw
-- @fonte: produtos.csv
-- @tentativas: 2
-- @descricao: Ingere o catálogo de produtos como veio da origem, sem limpeza.
DROP TABLE IF EXISTS raw_produtos;
CREATE TABLE raw_produtos (
    id_produto     TEXT,
    nome           TEXT,
    categoria      TEXT,
    preco_unitario TEXT,
    _arquivo       TEXT,
    _carregado_em  TEXT DEFAULT (datetime('now', 'localtime'))
);
