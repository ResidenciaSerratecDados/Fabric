#!/usr/bin/env python
# coding: utf-8

# ## SQL_Note
# 
# New notebook

# In[1]:


pip install plotly


# In[2]:


import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pyspark.sql import SparkSession


# In[3]:


categoria = spark.sql("SELECT * FROM LakeHouse_Murilo.categoria LIMIT 1000")
display(categoria)


# In[4]:


produto = spark.sql("SELECT * FROM LakeHouse_Murilo.produto LIMIT 1000")
display(produto)


# In[5]:


df_categoria_pandas = categoria.toPandas()
df_produto_pandas = produto.toPandas()


# In[6]:


df_categoria_pandas.head()


# In[7]:


df_produto_pandas.head()


# In[8]:


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


# In[9]:


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


# In[10]:


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


# In[11]:


join = {
    'Produto': df_produto_pandas['NOME'],
    'Categoria': df_categoria_pandas['NOME'],
    'Custo Médio': df_produto_pandas['CUSTO_MEDIO'],
    'Valor Unitário': df_produto_pandas['VALOR_UNITARIO']
}
join_view = pd.DataFrame(join)
join_view.head(300)


# In[12]:


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


# In[24]:


type(df_join)


# In[83]:


# Inicia uma sessão Spark (se ainda não tiver uma)
spark = SparkSession.builder.getOrCreate()

# Converte o pandas DataFrame para Spark DataFrame
spark_df_join = spark.createDataFrame(df_join)

# Agora você pode usar .write
spark_df_join.write.format("csv").option("header", "true").mode("overwrite").save("Tables/Categorias_Produtos.csv")


# In[79]:


spark.sql("CREATE TABLE IF NOT EXISTS Categorias_Produtos USING DELTA LOCATION 'Files/Categorias_Produtos'")


# In[74]:


df_join = produto.alias("p") \
    .join(
        categoria.alias("c"),
        on=produto.ID_CATEGORIA == categoria.IDCATEGORIA,
        how="inner"
    ) \
    .select(
        produto.NOME.alias("produto"),
        produto.CUSTO_MEDIO.alias("custo_medio"),
        produto.VALOR_UNITARIO.alias("valor_unitario"),
        categoria.NOME.alias("categoria")
    )

# Mostrar resultados
df_join.show(300)


# 
