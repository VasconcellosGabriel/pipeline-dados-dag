-- @tipo: extract
-- @camada: raw
-- @fonte: pedidos.csv
-- @tentativas: 2
-- @descricao: Ingere os cabeçalhos de pedido como vieram da origem, incluindo as linhas reenviadas.
DROP TABLE IF EXISTS raw_pedidos;
CREATE TABLE raw_pedidos (
    id_pedido     TEXT,
    id_cliente    TEXT,
    data_pedido   TEXT,
    status        TEXT,
    _arquivo      TEXT,
    _carregado_em TEXT DEFAULT (datetime('now', 'localtime'))
);
