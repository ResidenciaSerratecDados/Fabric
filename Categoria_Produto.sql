SELECT * FROM dbo.categoria;

--selecionar produtos com preços acima da média.
SELECT * FROM dbo.produto WHERE VALOR_UNITARIO > (SELECT AVG(VALOR_UNITARIO) FROM dbo.produto);

--inner join produto categoria
SELECT
    p.NOME AS produto,
    p.CUSTO_MEDIO AS custo_medio,
    p.VALOR_UNITARIO AS valor_unitario,
    c.NOME AS categoria
FROM 
    produto p
INNER JOIN 
    categoria c ON p.ID_CATEGORIA = c.IDCATEGORIA;