-- @tipo: check
-- @camada: qualidade
-- @depende_de: stg_itens_pedido, qua_itens_pedido, int_itens_classificados
-- @descricao: Aceita até 5% dos itens em quarentena; acima disso a origem está com problema e o fato não deve ser gerado. Também garante id_item único.
SELECT 'taxa de quarentena acima de 5%' AS regra,
       printf('%.1f%% dos itens (%d de %d)', 100.0 * q.n / t.n, q.n, t.n) AS detalhe
FROM (SELECT COUNT(*) AS n FROM qua_itens_pedido) q,
     (SELECT COUNT(*) AS n FROM int_itens_classificados) t
WHERE t.n = 0 OR 1.0 * q.n / t.n > 0.05
UNION ALL
SELECT 'id_item duplicado', 'item ' || id_item || ' aparece ' || COUNT(*) || ' vezes'
FROM stg_itens_pedido
GROUP BY id_item
HAVING COUNT(*) > 1;
