#!/usr/bin/env python
# coding: utf-8

# ## Note_IBGE_Murilo Copy
# 
# New notebook

# In[66]:


import pandas as pd
from pyspark.sql import SparkSession
import pyspark.pandas as ps
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType


# In[ ]:


df = spark.sql("SELECT * FROM LakeHouse_IBGE_Murilo.petropolis_setores LIMIT 1000")
display(df)


# In[38]:


df_geral = spark.sql("SELECT * FROM LakeHouse_IBGE_Murilo.setoresbasicos")
display(df_geral)


# In[48]:


# Filtrando o DataFrame para incluir apenas os registros de Petrópolis
df_petropolis = df_geral.filter(df_geral["NM_MUN"] == "Petrópolis")

# Exibindo as primeiras linhas do DataFrame filtrado
display(df_petropolis)


# In[61]:


df_petropolis_pd = df_petropolis.toPandas()
df_petropolis_pd.to_csv(
    "abfss://4d610067-8093-41e5-8082-7cad66a1a874@onelake.dfs.fabric.microsoft.com/1c8200e7-59ed-4bf8-914c-fdaca50c5cd7/Files/petropolis_setores.csv",
    index=False,
    encoding="utf-8"
)


# In[63]:


df_pet22 = spark.sql("SELECT * FROM LakeHouse_IBGE_Murilo.petropolis_setores LIMIT 1000")
display(df_pet22)


# In[69]:


#renomear colunas e converter para float as colunas
'''
df_pet22 = df_pet22 \
    .withColumn('v0001', F.round(F.col('Total_Pessoas').cast(FloatType()), 2)) \
    .withColumn('v0002', F.round(F.col('Total_Domicilios').cast(FloatType()), 2)) \
    .withColumn('v0003', F.round(F.col('Domicilios_Particulares').cast(FloatType()), 2)) \
    .withColumn('v0004', F.round(F.col('Domicilios_Coletivos').cast(FloatType()), 2)) \
    .withColumn('v0005', F.round(F.col('Media_Mor_Dom_Par').cast(FloatType()), 2)) \
    .withColumn('v0006', F.round(F.col('%_Mor_Dom_Par').cast(FloatType()), 2)) \
    .withColumn('v0007', F.round(F.col('Tot_Dom_Part_Ocup').cast(FloatType()), 2))

df_pet22.printSchema()
display(df_pet22)
'''


# In[70]:


#Listar as comunidades
fcu_set = df_pet22.select(F.collect_set("NM_FCU").alias("fcu_list")).first()["fcu_list"]
print(fcu_set)


# In[76]:


df_sum = df_pet22.groupBy("NM_FCU") \
    .agg(F.sum("v0001").alias("Residentes")) \
    .filter(F.col("NM_FCU").isNotNull())

df_sorted = df_sum.orderBy(F.col("Residentes").desc())
display(df_sorted)


# In[78]:


#Listar os aglomerados
fcu_aglom = df_pet22.select(F.collect_set("NM_AGLOM").alias("fcu_aglom")).first()["fcu_aglom"]
print(fcu_aglom)


# In[80]:


df_sum2 = df_pet22.groupBy("NM_AGLOM") \
    .agg(F.sum("v0001").alias("Residentes")) \
    .filter(F.col("NM_AGLOM").isNotNull())

df_sorted2 = df_sum2.orderBy(F.col("Residentes").desc())
display(df_sorted2)


# In[81]:


#Listar os DISTRITOS
fcu_dist = df_pet22.select(F.collect_set("NM_DIST").alias("fcu_dist")).first()["fcu_dist"]
print(fcu_dist)


# In[82]:


df_sum3 = df_pet22.groupBy("NM_DIST") \
    .agg(F.sum("v0001").alias("Residentes")) \
    .filter(F.col("NM_DIST").isNotNull())

df_sorted3 = df_sum3.orderBy(F.col("Residentes").desc())
display(df_sorted3)


# In[ ]:


#CÁCULO DA DEMANDA:
#https://pt.slideshare.net/slideshow/clculo-de-demanda/59396535#4
#Ver o potencial para abrir curso de imgles por local

#Criar Pipeline
#Conectar Fabric com GitHub

