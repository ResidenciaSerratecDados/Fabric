#!/usr/bin/env python
# coding: utf-8

# ## B3_NoteBook
# 
# New notebook

# In[52]:


get_ipython().system('pip install yahooquery')


# In[53]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, to_date, month, year, when, lag, avg
from pyspark.sql.window import Window
import pandas as pd
import numpy as np


# In[54]:


from yahooquery import Ticker


# In[55]:


from pyspark.sql.functions import round


# In[56]:


#Carregar tabela CSV
df_tickers = spark.read.option("header", True).csv('Files/acoes_listadas_b3.csv')
display(df_tickers)


# In[57]:


#Selecionar itens da linhas da 1ª coluna com 3
df_tickers = df_tickers.filter(col('Ticker').endswith('3'))
display(df_tickers)


# In[58]:


#Concatenar itens das linhas da 1ª coluna com .SA
df_tickers = df_tickers.withColumn('Ticker', expr("concat(Ticker, '.SA')"))
display(df_tickers)


# In[59]:


lista_tickers = [row['Ticker'] for row in df_tickers.collect()]


# In[60]:


lista_tickers_resumido = ['PETR3.SA', 'VALE3.SA', 'TAEE3.SA']


# In[61]:


tickers = Ticker(lista_tickers)


# In[62]:


df_cotacoes = tickers.history(period='30d', adj_timezone=False)


# In[63]:


df_cotacoes = df_cotacoes.reset_index()
display(df_cotacoes)


# In[64]:


#Conversão para DataHora
df_cotacoes['date'] = pd.to_datetime(df_cotacoes['date'], utc=True)


# In[65]:


#Fuso horário de São Paulo (-3:00)
df_cotacoes['date'] = df_cotacoes['date'].dt.tz_convert('America/Sao_Paulo')


# In[66]:


#converter para Data (dd/mm/aaaa)
df_cotacoes['date'] = df_cotacoes['date'].dt.date


# In[67]:


#Transformação do DataFrame Pandas em Spark
df_cotacoes_spark = spark.createDataFrame(df_cotacoes)

#Renomeação de Colunas
df_cotacoes_spark = df_cotacoes_spark.withColumnRenamed('date', 'Data') \
   .withColumnRenamed('open', 'Abertura') \
   .withColumnRenamed('high', 'Alta') \
   .withColumnRenamed('low', 'Baixa') \
   .withColumnRenamed('close', 'Fechamento') \
   .withColumnRenamed('volume', 'Volume') \
   .withColumnRenamed('adjclose', 'FechamentoAjustado') \
   .withColumnRenamed('symbol', 'Ticker') \
   .withColumnRenamed('dividends', 'Dividendos')

df_cotacoes_spark = df_cotacoes_spark \
    .withColumn('Abertura', round('Abertura', 2)) \
    .withColumn('Alta', round('Alta', 2)) \
    .withColumn('Baixa', round('Baixa', 2)) \
    .withColumn('Fechamento', round('Fechamento', 2)) \
    .withColumn('Volume', round('Volume', 2)) \
    .withColumn('FechamentoAjustado', round('FechamentoAjustado', 2)) \
    .withColumn('Dividendos', round('Dividendos', 2))

display(df_cotacoes_spark)


# In[68]:


#Cálculo da Variação de Fechamento
df_cotacoes_spark = df_cotacoes_spark.sort(['Ticker', 'Data'])
display(df_cotacoes_spark)


# In[69]:


#Define janela por Ticker Ordenada por Data
windowSpec = Window.partitionBy('Ticker').orderBy('Data')


# In[70]:


#Criação da coluna de 1 dia antes do fechamento
df_cotacoes_spark = df_cotacoes_spark.withColumn("Fechamento_Anterior", lag("Fechamento").over(windowSpec))
display(df_cotacoes_spark)


# In[71]:


# Cria coluna com a variação percentual do fechamento em relação ao dia anterior
df_cotacoes_spark = df_cotacoes_spark.withColumn("Fechamento_Variacao", 
    (col("Fechamento") - col("Fechamento_Anterior")) / col("Fechamento_Anterior"))

df_cotacoes_spark = df_cotacoes_spark.withColumn(
    'Fechamento_Variacao',
    round(col('Fechamento_Variacao') * 100, 2)
)
display(df_cotacoes_spark)


# In[72]:


# Converte o dicionário de perfil dos ativos (como setor, indústria etc.) em DataFrame pandas
df_setores = pd.DataFrame(tickers.asset_profile)

# Transpõe o DataFrame: linhas viram colunas e colunas viram linhas (tickers passam a ser índice)
df_setores = df_setores.T

display(df_setores)


# In[73]:


# Seleciona apenas as colunas de setor e indústria e reseta o índice (ticker vira coluna)
df_setores = df_setores[['sectorDisp', 'industryDisp']].reset_index()
display(df_setores)


# In[74]:


# Renomeia a coluna 'index' para 'Ticker' (após o reset_index)
df_setores = df_setores.rename(columns = {'index': 'Ticker'})
display(df_setores)


# In[75]:


# Converte o DataFrame Pandas df_setores para um DataFrame Spark
df_setores_spark = spark.createDataFrame(df_setores)
display(df_setores_spark)


# In[76]:


# Junta os dados de cotações com os dados setoriais pelo Ticker, mantendo todas as cotações (left join)
df_cotacoes_spark = df_cotacoes_spark.join(df_setores_spark, on='Ticker', how='left')
display(df_cotacoes_spark)


# In[77]:


# Salva o DataFrame como uma tabela Delta no metastore Spark com schema merge e sobrescrita
df_cotacoes_spark.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .saveAsTable("cotacoes_tickers")

