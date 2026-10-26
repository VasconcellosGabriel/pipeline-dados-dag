-- @tipo: extract
-- @camada: raw
-- @fonte: itens_pedido.csv
-- @tentativas: 2
-- @descricao: Ingere os itens de cada pedido (produto, quantidade, preço praticado e desconto) como vieram da origem.
DROP TABLE IF EXISTS raw_itens_pedido;
CREATE TABLE raw_itens_pedido (
    id_item        TEXT,
    id_pedido      TEXT,
    id_produto     TEXT,
    quantidade     TEXT,
    preco_unitario TEXT,
    desconto       TEXT,
    _arquivo       TEXT,
    _carregado_em  TEXT DEFAULT (datetime('now', 'localtime'))
);
