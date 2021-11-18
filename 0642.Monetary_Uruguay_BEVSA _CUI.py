#!/usr/bin/env python
# coding: utf-8

# In[44]:


import pandas as pd
import numpy as np
import datetime
import urllib
import time
from urllib.request import urlopen
import requests  

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)



# In[45]:


#path = "..//Input/Conector 642/cui.xlsx"
#df = pd.read_excel(path)
#df["Date"] = df["FECHA"]
#del df["FECHA"]
#df.index = df["Date"]
#del df["Date"]
#df = df.sort_index(ascending=True)

path = "..//Input/Conector 642/cui.csv"
df = pd.read_csv(path)
df.index = pd.to_datetime(df["Date"])
del df["Date"]
df


# In[46]:


# Updater

url = "https://web.bevsa.com.uy/CurvasVectorPrecios/CurvasIndices/Historico.aspx?I=CUI"
r = requests.get(url, allow_redirects=False)
df2 = pd.read_html(r.content)
df2 = df2[8]
del df2["Unnamed: 17"]
ddmmyyyy = df2["FECHA"]
yyyymmdd = ddmmyyyy.str[6:] + "-" + ddmmyyyy.str[3:5] + "-" + ddmmyyyy.str[:2]
df2["FECHA"] = yyyymmdd
df2["Date"] = df2["FECHA"]
del df2["FECHA"]
df2.index = pd.to_datetime(df2["Date"])
del df2["Date"]
df2 = df2.sort_index(ascending=True)
df2.columns = df.columns
df2 = df2/10000

if df.index[len(df)-1] == df2.index[len(df2)-1]:
    print("Dataset nuevo igual al anterior. No se actualizará nada.")
else:
        print("Dataset actualizado.")

df3 = pd.concat([df,df2]).drop_duplicates()
df3 = df3[~df3.index.duplicated(keep='last')]
df3.to_csv("..//Input/Conector 642/cui.csv")
df3["country"] = "Uruguay"

alphacast.datasets.dataset(642).upload_data_from_df(df3, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)




