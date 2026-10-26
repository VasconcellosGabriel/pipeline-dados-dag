-- @tipo: sql
-- @camada: staging
-- @depende_de: int_itens_classificados
-- @descricao: Quarentena: itens rejeitados ficam guardados com o motivo para a equipe investigar, em vez de serem descartados em silêncio.
DROP TABLE IF EXISTS qua_itens_pedido;
CREATE TABLE qua_itens_pedido AS
SELECT *, datetime('now', 'localtime') AS quarentenado_em
FROM int_itens_classificados
WHERE motivo_rejeicao IS NOT NULL;
