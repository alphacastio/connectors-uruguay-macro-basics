#!/usr/bin/env python
# coding: utf-8

# In[32]:


import pandas as pd
import numpy as np
import datetime
import urllib
import time
from urllib.request import urlopen
import requests  

import regex as re
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)



# In[33]:


# path = "..//Input/Conector 648/ITLUP.xlsx"
# df = pd.read_excel(path)
# df = df.rename(columns={'FECHA':'Date'})
# df = df.set_index('Date')
# df = df.sort_index(ascending=True)

path = "..//Input/Conector 648/ITLUP.csv"
df = pd.read_csv(path)
df.index = pd.to_datetime(df["Date"])
del df["Date"]
df


# In[34]:


# Updater

url = "https://web.bevsa.com.uy/CurvasVectorPrecios/CurvasIndices/Historico.aspx?I=ITLUP"
r = requests.get(url, allow_redirects=False)
df2 = pd.read_html(r.content)
df2 = df2[8]

#Correcciones al df
df2["Date"] = pd.to_datetime(df2["FECHA"], errors = 'coerce', format = '%d/%m/%Y').dt.strftime('%Y-%m-%d')
del df2["FECHA"]

df2 = df2.set_index('Date')
df2 = df2.sort_index(ascending=True)

df2.columns = df.columns

df2 = df2/10000

#Chequeo si hubo actualizaciones y luego mergeo con el histórico
if df.index[len(df)-1] == df2.index[len(df2)-1]:
    print("Dataset nuevo igual al anterior. No se actualizará nada.")
else:
        print("Dataset actualizado.")

df3 = pd.concat([df,df2]).drop_duplicates()
df3 = df3[~df3.index.duplicated(keep='last')]
df3.to_csv("..//Input/Conector 648/ITLUP.csv")
df3["country"] = "Uruguay"

alphacast.datasets.dataset(648).upload_data_from_df(df3, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)


