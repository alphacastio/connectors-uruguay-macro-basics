#!/usr/bin/env python
# coding: utf-8

# In[226]:


import pandas as pd

import numpy as np
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)



# In[227]:


url = "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/ComercioExterior_ICB/imp_gce_val.xls"
df = pd.read_excel(url, sheet_name="Impor GCE desde enero 2019", skiprows= 7)
df.dropna(axis=0, how='all',inplace=True)
df.dropna(axis=1, how='all',inplace=True)


# In[228]:


df["level_1"] = df["Unnamed: 2"]

sublevels = {
    'level_2':["Automotores y otros de transporte", "Duraderos", "Energía eléctrica", "Equipos de transporte", "Intermedios sin petróleo, destilados y energía eléctrica", "Maquinaria y equipos", "Otros de consumo", "Petróleo y destilados ", ],
    'level_3':["Alimentos y bebidas", "Otros insumos intermedios", "Piezas y accesorios de transporte", "Privado", "Público", ],
    'level_4':["Resto", "Suministros industriales básicos", "Suministros industriales elaborados", ],
}

for sublevel in sublevels:
    df[sublevel] = np.nan
    #print(sublevel)
    df.loc[df["level_1"].isin(sublevels[sublevel]), sublevel] = df["level_1"]
    df.loc[df["level_1"].isin(sublevels[sublevel]), "level_1"] = np.nan 


# In[229]:


for x in range(len(sublevels)+1,0,-1):
    print(x)
    df["level_{}_temp".format(x)] = df["level_{}".format(x)].ffill()
    df.loc[df["level_{}".format(x)].notnull(), "level_{}".format(x)] = df["level_{}_temp".format(x)]
    for y in range(1, x):
        df.loc[df["level_{}".format(y)].notnull(), "level_{}_temp".format(x)] = np.nan
        #print(str(x) + " - " + str(y))    


# In[230]:



df["concept"] = df["level_1_temp"]  

for y in range (2,len(sublevels)+2):
    df.loc[df["level_{}_temp".format(y)].notnull(), "concept"] = df["concept"] + " - " + df["level_{}_temp".format(y)]
    #print(y)       


# In[232]:




df = df[df.columns.drop(list(df.filter(regex='level_')))]
df["Unnamed: 2"] = df["concept"].str.title()
df = df.transpose().reset_index()
del df[0]
new_header = df.iloc[1] #grab the first row for the header
df = df[2:] #take the data less the header row
df.columns = new_header #set the header row as the df header

df["Date"] = pd.to_datetime(df["Unnamed: 2"].astype("str").str.split(" ").str[0], errors="coerce")
del df["Unnamed: 2"]
df = df.set_index("Date")
df = df[df.index.notnull()]


# In[234]:


df.dropna(axis=0, how='all',inplace=True)
df.dropna(axis=1, how='all',inplace=True)
df


# In[237]:


df["country"] = "Uruguay"

alphacast.datasets.dataset(343).upload_data_from_df(df, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)


