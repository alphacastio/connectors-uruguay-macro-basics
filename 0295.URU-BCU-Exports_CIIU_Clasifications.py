#!/usr/bin/env python
# coding: utf-8

# In[5]:


import pandas as pd
from datetime import datetime
import requests
import numpy as np
import re

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)



# In[6]:


url = "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/ComercioExterior_ICB/exp_ciiu_val.xls"
r = requests.get(url, allow_redirects=False)
df = pd.read_excel(r.content, skiprows=7, sheet_name='Expor_CIIU Rev.3 hasta dic2018')


# In[7]:


df = df.dropna(how='all').dropna(how='all',axis=1).dropna(how='all', subset=df.columns[1:])
df = df.drop(['Unnamed: 1',' '], axis=1)
df = df.dropna(how='all', subset=df.columns[1:])

df = df.iloc[1:, :]                

df = df.set_index('Unnamed: 3')

df = df.T

df = df.reset_index()
df = df.rename(columns={'index':'Date'})
df = df.set_index('Date')
df.columns = list(df.columns)


# In[8]:


url1 = "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/ComercioExterior_ICB/exp_ciiu_val.xls"
r1 = requests.get(url1, allow_redirects=False)

df1 = pd.read_excel(r1.content, skiprows=7, sheet_name='Expor_CIIU Rev.3 desde ene2019')

df1 = df1.dropna(how='all').dropna(how='all',axis=1).dropna(how='all', subset=df1.columns[1:])
df1 = df1.drop(['Unnamed: 1',' '], axis=1)
df1 = df1.dropna(how='all', subset=df1.columns[1:])

df1 = df1.iloc[1:, :]                

df1 = df1.set_index('Unnamed: 3')

df1 = df1.T

df1 = df1.reset_index()
df1 = df1.rename(columns={'index':'Date'})

indiceFinal = df1[df1['Date'] == 'Mensual setiembre-21 / setiembre-20'].index[0]

df1 = df1[:indiceFinal]
df1 = df1.set_index('Date')

df1.columns = list(df1.columns)

dfFinal = df.append([df1])
dfFinal


# In[9]:


dfFinal['country'] = 'Uruguay'

alphacast.datasets.dataset(295).upload_data_from_df(dfFinal, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)


# In[ ]:




