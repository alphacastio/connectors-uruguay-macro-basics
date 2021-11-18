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


url = "https://www.ine.gub.uy/c/document_library/get_file?uuid=0357634a-28d7-49e2-ba2c-971f170d0e1d&groupId=10181"
r = requests.get(url ,allow_redirects=False, verify=False)
df = pd.read_excel(r.content, skiprows= 8)
df = df.iloc[:-3 , :]
df["Entity"] = "Uruguay"


# In[46]:


df.columns = ["Date", "Índice Medio de Salarios Nominales - Nivel General", 
              "Índice Medio de Salarios Nominales - Sector Privado", 
              "Índice Medio de Salarios Nominales - Sector Público", 
              "country"]

df['Date'] = pd.to_datetime(df['Date'])
df['Date'] = df['Date'].dt.date
df = df.set_index(['Date'])


alphacast.datasets.dataset(479).upload_data_from_df(df, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)


# In[ ]:




