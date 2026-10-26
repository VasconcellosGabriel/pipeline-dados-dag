-- @tipo: check
-- @camada: qualidade
-- @depende_de: fct_vendas, mart_faturamento_diario, mart_vendas_categoria, mart_clientes_rfm
-- @descricao: Reconciliação antes de publicar: cada mart precisa somar a mesma receita do fato, sem mart vazio nem receita negativa.
SELECT 'mart vazio' AS regra, 'mart_faturamento_diario não tem linhas' AS detalhe
WHERE (SELECT COUNT(*) FROM mart_faturamento_diario) = 0
UNION ALL
SELECT 'faturamento diário não reconcilia', printf('%.2f vs %.2f no fato', d.total, f.total)
FROM (SELECT SUM(receita_liquida) AS total FROM mart_faturamento_diario) d,
     (SELECT SUM(receita_liquida) AS total FROM fct_vendas) f
WHERE ABS(d.total - f.total) > 0.05
UNION ALL
SELECT 'vendas por categoria não reconcilia', printf('%.2f vs %.2f no fato', c.total, f.total)
FROM (SELECT SUM(receita_liquida) AS total FROM mart_vendas_categoria) c,
     (SELECT SUM(receita_liquida) AS total FROM fct_vendas) f
WHERE ABS(c.total - f.total) > 0.05
UNION ALL
SELECT 'RFM não reconcilia', printf('%.2f vs %.2f no fato', r.total, f.total)
FROM (SELECT SUM(valor) AS total FROM mart_clientes_rfm) r,
     (SELECT SUM(receita_liquida) AS total FROM fct_vendas) f
WHERE ABS(r.total - f.total) > 0.05
UNION ALL
SELECT 'receita negativa', 'dia ' || COALESCE(data_pedido, 'NULL')
FROM mart_faturamento_diario
WHERE receita_liquida < 0 OR data_pedido IS NULL;
