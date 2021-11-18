#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import requests
import io
from datetime import datetime
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)



# In[2]:





# In[3]:


url = 'https://www.ine.gub.uy/c/document_library/get_file?uuid=8e08c0dc-acc2-44f7-b302-daa32e0b978b&groupId=10181'
r = requests.get(url, allow_redirects=False, verify=False)

with io.BytesIO(r.content) as datafr:
    df = pd.read_excel(datafr, skiprows=4)

df = df.dropna(how='all', subset= df.columns[1:])


# In[4]:


value = ''
for i in range(0, len(df['Año'])):
    if pd.notna(df['Año'].iloc[i]):
        value = df['Año'].iloc[i]
    else:
        df['Año'].iloc[i] = value


# In[5]:


df['Mes'] = df['Mes'].apply(lambda x: x.upper())


# In[6]:


for i in range(0, len(df['Mes'])):
    if ('/' in df['Mes'].iloc[i]) or (df['Mes'].iloc[i] == 'PROM. ANUAL'):
        df['Mes'].iloc[i] = 'NOT THIS'
    else:
        pass

df = df[df['Mes'] != 'NOT THIS']


# In[7]:


def replace_date_str_to_int(x):
    x= x.replace(' ','')
    if x == 'ENERO':
        x = x.replace('ENERO', '01-01')
    elif x == 'FEBRERO':
        x = x.replace('FEBRERO', '02-01')
    elif x == 'MARZO':
        x = x.replace('MARZO', '03-01')
    elif x == 'ABRIL':
        x = x.replace('ABRIL', '04-01')
    elif x == 'MAYO':
        x = x.replace('MAYO', '05-01')
    elif x == 'JUNIO':
        x = x.replace('JUNIO', '06-01')
    elif x == 'JULIO':
        x = x.replace('JULIO', '07-01')
    elif x == 'AGOSTO':
        x = x.replace('AGOSTO', '08-01')
    elif x == 'SEPTIEMBRE':
        x = x.replace('SEPTIEMBRE', '09-01')
    elif x == 'SETIEMBRE':
        x = x.replace('SETIEMBRE', '09-01')
    elif x == 'OCTUBRE':
        x = x.replace('OCTUBRE', '10-01')
    elif x == 'NOVIEMBRE':
        x = x.replace('NOVIEMBRE', '11-01')
    elif x == 'DICIEMBRE':
        x = x.replace('DICIEMBRE', '12-01')
    return x

df['Mes'] =df['Mes'].apply(lambda x: replace_date_str_to_int(x))
df['Date'] = df['Año'].astype(str) +'-'+ df['Mes'].astype(str)
df['Date'] = df['Date'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d"))
df.Date.unique()


# In[8]:


# df = pd.to_datetime('Date', format='%Y-%m-%d')
df = df.set_index('Date')


# In[9]:


df = df.dropna(axis=1)
df = df[df.columns[2:]]
df['country'] = 'Uruguay'


# In[10]:


df = df.replace("(s)", np.nan)
df.dropna(axis=0, how='all',inplace=True)
df.dropna(axis=1, how='all',inplace=True)

newcols = []
for col in df.columns:
    col = str(col)
    newcol = 'c'+col
    newcols += [newcol]

df.columns = newcols

df["country"] = "Uruguay"

alphacast.datasets.dataset(111).upload_data_from_df(df, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

