#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
import requests
import numpy as np
from datetime import datetime
from urllib.request import urlopen
from lxml import etree
import io
from alphacast import Alphacast
from dotenv import dotenv_values


# In[3]:


#Obtengo la url del file a través de su xpath
url = "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Paginas/Encuesta-Inflacion.aspx"
r = requests.get(url,verify=False)
html = r.content
htmlparser = etree.HTMLParser()
tree = etree.fromstring(html, htmlparser)
xls_address = tree.xpath("//*[@id='ctl00_ctl63_g_c525116a_ced2_4266_8f43_a185987917be_ctl00_documents']/tbody/tr/td[4]/a/@href")[0]


# In[4]:


url = xls_address
r = requests.get(url, allow_redirects=True, verify=False)

#Creo el dataframe y elimino los vacíos
df = pd.read_excel(r.content, skiprows=9, sheet_name= 0)
df = df.dropna(how = 'all', axis = 0).dropna(how = 'all', axis = 1)
df = df.dropna(how='all', subset = df.columns[2:])


# In[5]:


#Replico el nombre en todas las columnas vacías
df.columns = df.columns.astype(str)
df.columns = pd.Series([np.nan if 'Unnamed:' in x else x for x in df.columns.values]).ffill().values.flatten()


# In[6]:


#Traspongo el df para poder armar el multiindice y lo vuelvo al original para armar las columnas con el nombre unificado
df = df.set_index('Fecha encuesta')
df = df.T.reset_index() 
df.rename(columns={df.columns[1]: "Estadística", df.columns[22]: "2005-09-01"}, inplace=True) #Renombro las columnas mal nombradas
df = df.set_index(['index', 'Estadística'])
df = df.T


# In[7]:


#Uno los nombres de las columnas
if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.map(' - '.join)


# In[8]:


#Elimino los numeros de los nombres de las variables
for col in df.columns:
    new_col = col.replace("1.", "").replace('2.','').replace('3.','').replace('4.','').replace('5.','').replace('6.','').replace('7.','').replace('8.','').replace('9.','')
    df = df.rename(columns={col: new_col})


# In[9]:


#Seteo la columna 'Date' como indice
df = df.reset_index()
df['Date'] = pd.to_datetime(df['Fecha encuesta'])
del df['Fecha encuesta']
df = df.set_index("Date")

#Agrego la columna country al dataframe
df['country'] = 'Uruguay'


# In[10]:


API_KEY = dotenv_values(".env").get("ALPHACAST_API_KEY")

alphacast = Alphacast(API_KEY)


# In[11]:


#Cargo la data a Alphacast
alphacast.datasets.dataset(640).upload_data_from_df(df, 
                 deleteMissingFromDB = False, onConflictUpdateDB = True, uploadIndex=True)


# In[ ]:




