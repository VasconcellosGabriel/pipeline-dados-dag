-- @tipo: check
-- @camada: qualidade
-- @depende_de: stg_pedidos, stg_clientes
-- @descricao: Contrato dos pedidos: data válida, status dentro do domínio e cliente existente. Qualquer violação bloqueia o fato de vendas.
SELECT 'data_pedido inválida' AS regra, 'pedido ' || id_pedido AS detalhe
FROM stg_pedidos
WHERE data_pedido IS NULL
UNION ALL
SELECT 'status fora do domínio', 'pedido ' || id_pedido || ': ' || COALESCE(status, 'NULL')
FROM stg_pedidos
WHERE status IS NULL OR status NOT IN ('pago', 'entregue', 'cancelado')
UNION ALL
SELECT 'cliente inexistente', 'pedido ' || p.id_pedido || ' -> cliente ' || p.id_cliente
FROM stg_pedidos p
LEFT JOIN stg_clientes c ON c.id_cliente = p.id_cliente
WHERE c.id_cliente IS NULL;
