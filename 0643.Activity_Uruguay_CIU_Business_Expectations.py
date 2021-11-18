#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import requests
import datetime
from datetime import datetime
from datetime import date

import io
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)



# In[2]:


df1 = pd.read_excel("http://www.ciu.com.uy/innovaportal/file/15128/2/expectecon.xlsx")
df2 = pd.read_excel("http://www.ciu.com.uy/innovaportal/file/15128/2/expectemp.xlsx")
df3 = pd.read_excel("http://www.ciu.com.uy/innovaportal/file/15128/2/expectexport.xlsx")
df4 = pd.read_excel("http://www.ciu.com.uy/innovaportal/file/15128/2/expectvtaint.xlsx")


# In[3]:


df1 = df1[4:-1]
df1.columns = "Economía - " + df1.iloc[0]
df1 = df1.iloc[1:]
df1["Date"] = pd.to_datetime(df1["Economía - Fecha "], format='%Y-%m-%d %H:%M:%S', errors = "coerce")
df1 = df1.set_index("Date")
del df1["Economía - Fecha "]
df1 = df1[~df1.index.isnull()] 


# In[4]:


df2 = df2[4:-1]
df2.columns = "Empresas - " + df2.iloc[0]
df2 = df2.iloc[1:]
df2["Date"] = pd.to_datetime(df2["Empresas - Fecha "], format='%Y-%m-%d %H:%M:%S', errors = "coerce")
df2 = df2.set_index("Date")
del df2["Empresas - Fecha "]
df2 = df2[~df2.index.isnull()] 


# In[5]:


df3 = df3[4:-1]
df3.columns = "Exportaciones - " + df3.iloc[0]
df3 = df3.iloc[1:]
df3["Date"] = pd.to_datetime(df3["Exportaciones - Fecha "], format='%Y-%m-%d %H:%M:%S', errors = "coerce")
df3 = df3.set_index("Date")
del df3["Exportaciones - Fecha "]
df3 = df3[~df3.index.isnull()] 


# In[6]:


df4 = df4[3:-1]
df4.columns = "Ventas Internas - " + df4.iloc[0]
df4 = df4.iloc[1:]
df4["Date"] = pd.to_datetime(df4["Ventas Internas - Fecha "], format='%Y-%m-%d %H:%M:%S', errors = "coerce")
df4 = df4.set_index("Date")
del df4["Ventas Internas - Fecha "]
df4 = df4[~df4.index.isnull()] 


# In[7]:


df = df1.merge(df2, right_index = True, left_index=True).merge(df3, right_index = True, left_index=True).merge(df4, right_index = True, left_index=True)
df["country"] = "Uruguay"

alphacast.datasets.dataset(643).upload_data_from_df(df, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)





