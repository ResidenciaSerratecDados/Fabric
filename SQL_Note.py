#!/usr/bin/env python
# coding: utf-8

# ## SQL_Note
# 
# New notebook

# In[70]:


pip install plotly


# In[71]:


import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pyspark.sql import SparkSession


# In[72]:


categoria = spark.sql("SELECT * FROM LakeHouse_Murilo.categoria LIMIT 1000")
display(categoria)


# In[73]:


produto = spark.sql("SELECT * FROM LakeHouse_Murilo.produto LIMIT 1000")
display(produto)


# In[74]:


df_categoria_pandas = categoria.toPandas()
df_produto_pandas = produto.toPandas()


# In[75]:


df_categoria_pandas.head()


# In[76]:


df_produto_pandas.head()


# In[77]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# SELECT
#     p.NOME AS produto,
#     p.CUSTO_MEDIO AS custo_medio,
#     p.VALOR_UNITARIO AS valor_unitario,
#     c.NOME AS categoria
# FROM 
#     produto p
# INNER JOIN 
#     categoria c ON p.ID_CATEGORIA = c.IDCATEGORIA;


# In[78]:


# Realize o merge (join) com base nas colunas relacionais
df_join = pd.merge(
    left=df_produto_pandas,
    right=df_categoria_pandas,
    how='inner',
    left_on='ID_CATEGORIA',  # Coluna do produto
    right_on='IDCATEGORIA'  # Coluna da categoria
)

# Renomeie as colunas para melhor legibilidade (opcional)
df_join.rename(columns={
    'NOME_x': 'Produto',
    'NOME_y': 'Categoria',
    'CUSTO_MEDIO': 'Custo Médio',
    'VALOR_UNITARIO': 'Valor Unitário'
}, inplace=True)

# Exiba os primeiros 300 registros
display(df_join[['Produto', 'Categoria', 'Custo Médio', 'Valor Unitário']].head(300))


# In[79]:


# Realize o merge (join) com base nas colunas relacionais
df_join2 = pd.merge(
    left=df_produto_pandas,
    right=df_categoria_pandas,
    how='inner',
    left_on='ID_CATEGORIA',  # Coluna do produto
    right_on='IDCATEGORIA',  # Coluna da categoria
    suffixes=('_prod', '_cat')
)

# Exiba os primeiros 300 registros
df_join2.head(300)


# In[80]:


join = {
    'Produto': df_produto_pandas['NOME'],
    'Categoria': df_categoria_pandas['NOME'],
    'Custo Médio': df_produto_pandas['CUSTO_MEDIO'],
    'Valor Unitário': df_produto_pandas['VALOR_UNITARIO']
}
join_view = pd.DataFrame(join)
join_view.head(300)


# In[81]:


# Criar gráfico de dispersão
fig = px.scatter(
    df_join,
    x='Categoria',             # Eixo X
    y='Valor Unitário',          # Eixo Y
    color='Categoria',           # Cor por categoria
    size='Valor Unitário',          # Tamanho dos pontos conforme custo médio
    title='Preços dos Produtos por Categoria',
    labels={
        'Categoria': 'Categorias',
        'Valor Unitário': 'Preço(R$)'
    },
    hover_name='Produto'         # Mostra o nome do produto ao passar o mouse
)

# Exibir o gráfico interativo
fig.show()


# In[89]:


df_join.to_csv('Categorias.csv', sep=';')


# In[90]:


#1 Criar DataFrame Spark
spark = SparkSession.builder.getOrCreate()
df_spark = spark.createDataFrame(df_join)

caminho_da_tabela = "Tables/Dispersao.csv"
df_spark.write.mode("overwrite").option("header", "true").csv(caminho_da_tabela)


# 
