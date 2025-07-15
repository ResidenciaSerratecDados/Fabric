#!/usr/bin/env python
# coding: utf-8

# ## Spotify_Decadas
# 
# New notebook

# In[2]:


import pandas as pd
from pyspark.sql import SparkSession
import pyspark.pandas as ps
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
import numpy as np


# In[3]:


df60 = spark.sql("SELECT * FROM LH_Spotify.d60 LIMIT 1000")
display(df60)


# In[ ]:


df70 = spark.sql("SELECT * FROM LH_Spotify.d70 LIMIT 1000")
display(df70)


# In[ ]:


df80 = spark.sql("SELECT * FROM LH_Spotify.d80 LIMIT 1000")
display(df80)


# In[6]:


df90 = spark.sql("SELECT * FROM LH_Spotify.d90 LIMIT 1000")
display(df90)


# In[ ]:


df00 = spark.sql("SELECT * FROM LH_Spotify.d2000 LIMIT 1000")
display(df00)


# In[ ]:


df10 = spark.sql("SELECT * FROM LH_Spotify.d2010 LIMIT 1000")
display(df10)


# In[ ]:


d1960 = df60.toPandas()
d1970 = df70.toPandas()
d1980 = df80.toPandas()
d1990 = df90.toPandas()
d2000 = df00.toPandas()
d2010 = df10.toPandas()


# In[ ]:


df_decadas=pd.concat([d1960,d1970,d1980,d1990,d2000,d2010])
display(df_decadas.head())


# In[ ]:


# Inicia uma sessão Spark (se ainda não tiver uma)
spark = SparkSession.builder.getOrCreate()

# Converte o pandas DataFrame para Spark DataFrame
spark_df_join = spark.createDataFrame(df_decadas)

# Agora você pode usar .write
spark_df_join.write.format("csv").option("header", "true").mode("overwrite").save("Tables/60Anos.csv")

