#!/usr/bin/env python
# coding: utf-8

# In[69]:


import numpy as np
import pandas as pd
from datetime import datetime
import requests

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)



# In[70]:


df1 = pd.read_excel("https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/cuadro_130t.xls")
df2 = pd.read_excel("https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/cuadro_104t.xls")
df3 = pd.read_excel("https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/cuadro_105t.xls")


# In[71]:


df1 = df1.T.iloc[1:]
df1.columns = df1.iloc[0]
df1 = df1.drop(df1.index[0])
df1.index = pd.date_range(start='1/1/1997', periods=len(df1), freq = "QS")
df1.index.name = "Date"
df1 = df1[df1.columns.dropna()]
df1 = df1.iloc[: , :-1]
df1.columns = df1.columns + " - Precios Corrientes"


# In[72]:


df2 = df2.T.iloc[1:]
df2.columns = df2.iloc[0]
df2 = df2.drop(df2.index[0])
df2.index = pd.date_range(start='1/1/2005', periods=len(df2), freq = "QS")
df2.index.name = "Date"
df2 = df2[df2.columns.dropna()]
df2 = df2.iloc[: , :-2]
df2.columns = df2.columns + " - Precios Constantes de 2005"


# In[73]:


df3 = df3.T.iloc[1:]
df3.columns = df3.iloc[0]
df3 = df3.drop(df3.index[0])
df3.index = pd.date_range(start='1/1/2005', periods=len(df3), freq = "QS")
df3.index.name = "Date"
df3 = df3[df3.columns.dropna()]
df3 = df3.iloc[: , :-1]
df3.columns = df3.columns + " - Índice de volúmen físico base trimestre promedio 2005=100"


# In[74]:


df_final = df1.merge(df2, right_index=True, left_index=True, how = "left").merge(df3, right_index=True, left_index=True)


# In[75]:


df_final["country"] = "Uruguay"

alphacast.datasets.dataset(7906).upload_data_from_df(df_final, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)


df4 = pd.read_excel("https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/cuadro_106t.xls")
df5 = pd.read_excel("https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/cuadro_107t.xls")
df6 = pd.read_excel("https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/cuadro_133t.xls")


# In[77]:


df4 = df4.T.iloc[1:]
df4.columns = df4.iloc[0]
df4 = df4.drop(df4.index[0])
df4.index = pd.date_range(start='1/1/2005', periods=len(df4), freq = "QS")
df4.index.name = "Date"
df4 = df4[df4.columns.dropna()]
df4 = df4.iloc[: , :-5]
df4.columns = df4.columns + " - Precios Corrientes"


df5 = df5.T.iloc[1:]
df5.columns = df5.iloc[0]
df5 = df5.drop(df5.index[0])
df5.index = pd.date_range(start='1/1/2005', periods=len(df5), freq = "QS")
df5.index.name = "Date"
df5 = df5[df5.columns.dropna()]
df5 = df5.iloc[: , :-5]
df5.columns = df5.columns + " - Precios Constantes de 2005"


df6 = df6.T.iloc[1:]
df6.columns = df6.iloc[0]
df6 = df6.drop(df6.index[0])
df6.index = pd.date_range(start='1/1/2005', periods=len(df6), freq = "QS")
df6.index.name = "Date"
df6 = df6[df6.columns.dropna()]
df6 = df6.iloc[: , :-4]
df6.columns = df6.columns + " - Índice de volúmen físico base trimestre promedio 2005=100"


df_final2 = df4.merge(df5, right_index=True, left_index=True, how = "left").merge(df6, right_index=True, left_index=True)
df_final2["country"] = "Uruguay"

alphacast.datasets.dataset(7907).upload_data_from_df(df_final, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

