#!/usr/bin/env python
# coding: utf-8

# ## Loja_Virtual_Murilo
# 
# New notebook

# In[18]:


import pandas as pd
from pyspark.sql import SparkSession
import pyspark.pandas as ps
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
import numpy as np


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

