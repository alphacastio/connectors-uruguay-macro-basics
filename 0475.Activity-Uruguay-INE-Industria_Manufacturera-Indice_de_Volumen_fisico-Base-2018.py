#!/usr/bin/env python
# coding: utf-8

# In[30]:


import pandas as pd
import numpy as np
import requests
import io
from datetime import datetime

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)



# In[31]:


url = "https://www.ine.gub.uy/c/document_library/get_file?uuid=0eeb66dc-048e-49b7-8517-aaedf2f7075b&groupId=10181"
r = requests.get(url, allow_redirects=False, verify=False)

with io.BytesIO(r.content) as datafr:
    df_clasif = pd.read_excel(datafr, skiprows=2)

df_clasif = df_clasif.dropna(how='all', subset= df_clasif.columns[1:])
#df_clasif = df_clasif[df_clasif["Grupo (1)"]== 0]


# In[32]:


url = "https://www.ine.gub.uy/c/document_library/get_file?uuid=e5ee6e11-601f-45ff-9335-68cd5191fa39&groupId=10181"
r = requests.get(url, allow_redirects=False, verify=False)

with io.BytesIO(r.content) as datafr:
    df = pd.read_excel(datafr, skiprows=4)


# In[33]:


df = df.transpose().reset_index()
df = df.merge(df_clasif[df_clasif["Grupo (1)"]== 0], how="left", left_on="index", right_on="División")
df = df.merge(df_clasif[df_clasif["Agrupación / Clase (2)"]== 0], how="left", left_on="index", right_on="Grupo (1)")
df = df.merge(df_clasif, how="left", left_on="index", right_on="Agrupación / Clase (2)")
df["descripcion"] = df["Denominación"].fillna(df["Denominación_x"]).fillna(df["Denominación_y"]).fillna("Total")
df["index"] = df["index"].astype("str") + " - " + df["descripcion"]
df = df.set_index("index")
df = df.transpose().reset_index()




df = df.dropna(how='all', subset= df.columns[1:])
df["Date"] = pd.to_datetime(df["Año y mes - Total"], errors="coerce")
df = df.set_index('Date')
del df['index']
del df['Año y mes - Total']
del df['Unnamed: 1 - Total']
df = df[df.index.notnull()]


df = df.dropna(axis=1)
df["country"] = "Uruguay"

files = ['https://www.ine.gub.uy/c/document_library/get_file?uuid=da2ffe84-4870-4dd1-83e2-9c1de30eda7d&groupId=10181',
        'https://www.ine.gub.uy/c/document_library/get_file?uuid=238326af-a83c-4b57-981b-51bbf400d216&groupId=10181']

df_ag = pd.DataFrame()
for file in files:
    r = requests.get(file, allow_redirects=False, verify=False)
    with io.BytesIO(r.content) as datafr:
        df_aux = pd.read_excel(datafr, skiprows=4)
    
    df_aux = df_aux.dropna(how='all')
    df_aux = df_aux.dropna(how='all', subset= df_aux.columns[1:])
    df_aux["Date"] = pd.to_datetime(df_aux["Año y mes"], errors="coerce")
    df_aux = df_aux.set_index('Date')
    del df_aux['Año y mes']
    del df_aux['Unnamed: 1']
    
    df_aux = df_aux.iloc[:,[0]]
    df_ag = df_ag.merge(df_aux, how='outer', left_index=True, right_index=True)
    
df_ag = df_ag.rename(columns={'C_x':'Indice de horas trabajadas','C_y':'Indice de precios al productor'})

df = df.merge(df_ag, how='outer', left_index=True, right_index=True)

alphacast.datasets.dataset(475).upload_data_from_df(df, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)



