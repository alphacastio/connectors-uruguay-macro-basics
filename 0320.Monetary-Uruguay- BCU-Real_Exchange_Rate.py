#!/usr/bin/env python
# coding: utf-8

# In[77]:


import pandas as pd
from datetime import datetime
import requests

import re
from urllib.request import urlopen
from lxml import etree
import io
import numpy as np
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)



# In[78]:


url = "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Indice_Cambio_Real/TCRE.xls"
r = requests.get(url, allow_redirects=True, verify=False)
df = pd.read_excel(r.content,skiprows = 1, sheet_name = 'Hoja1', header=[6])
df = df.replace(np.nan, "")
df.columns = df.columns + "  " + df.loc[0]
df.columns = df.columns.str.replace(".1", " - ")
df.columns = df.columns.str.replace(".2", " - ")
df.columns = df.columns.str.replace("Efectivo  Global", "Efectivo - Global")
df = df.replace("", np.nan)


# In[79]:


#Elimino NaNs
df = df.dropna(how='all', subset=df.columns[2:])                
df = df.loc[:, ~(df == '(*)').any()]                
df = df.dropna(how='all')     
df = df.iloc[1:]


# In[80]:


df.columns = df.columns.str.replace("Unnamed: -   ", "Date")
df['Date']= pd.to_datetime(df['Date'])
df = df.set_index('Date')


# In[81]:


df['country'] = 'Uruguay'

alphacast.datasets.dataset(320).upload_data_from_df(df, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)






