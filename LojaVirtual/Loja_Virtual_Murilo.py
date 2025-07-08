#!/usr/bin/env python
# coding: utf-8

# ## Loja_Virtual_Murilo
# 
# New notebook

# In[2]:


import pandas as pd
from pyspark.sql import SparkSession
import pyspark.pandas as ps
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
import numpy as np
import re


# In[19]:


livros = spark.sql("SELECT * FROM LH_Aula07.books_data_clean")
display(livros)


# In[20]:


salarios = spark.sql("SELECT * FROM LH_Aula07.salaries")
display(salarios)


# In[21]:


compras = spark.sql("SELECT * FROM LH_Aula07.shopping_trends")
display(compras)


# In[22]:


spotify = spark.sql("SELECT * FROM LH_Aula07.spotify_history")
display(spotify)


# In[3]:


# Cria ou reutiliza uma sessão Spark (ambiente de execução do PySpark)
spark = SparkSession.builder.getOrCreate()

# Lista com os caminhos dos arquivos CSV localizados no Lakehouse
file_paths = [
    "Files/Books_Data_Clean.csv",
    "Files/Salaries.csv",
    "Files/shopping_trends.csv",
    "Files/spotify_history.csv"
]

# Função para limpar os nomes das colunas de um DataFrame
def clean_column_names(df):
    for col in df.columns:
        new_col = col.strip().lower().replace(" ", "_")  # Remove espaços, põe em minúsculo e troca espaço por "_"
        new_col = re.sub(r'[^\w_]', '', new_col)  # Remove caracteres especiais (mantém letras, números e "_")
        df = df.withColumnRenamed(col, new_col)  # Renomeia a coluna
    return df  # Retorna o DataFrame com os nomes limpos

# Dicionário para armazenar os DataFrames limpos
dfs = {}

# Loop para ler, tratar e salvar cada CSV como Delta Table
for path in file_paths:
    name = path.split("/")[-1].replace(".csv", "")  # Usa o nome do arquivo como nome da tabela
    df = spark.read.option("header", True).csv(path)  # Lê o CSV com cabeçalho
    df_clean = clean_column_names(df)  # Aplica a limpeza dos nomes das colunas
    dfs[name] = df_clean  # Armazena o DataFrame limpo no dicionário
    df_clean.show(5)  # Mostra as 5 primeiras linhas do DataFrame limpo

    # Salva o DataFrame como Delta Table no Lakehouse
    df_clean.write.format("delta")\
        .mode("overwrite")\
        .option("overwriteSchema", "true")\
        .saveAsTable(name)  # Salva a tabela com o nome do arquivo original-

