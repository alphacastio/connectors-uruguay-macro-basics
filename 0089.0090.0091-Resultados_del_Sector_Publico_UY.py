#!/usr/bin/env python
# coding: utf-8

# In[17]:


import pandas as pd
import datetime
import numpy as np
import requests
from urllib.request import urlopen
from lxml import etree
from bs4 import BeautifulSoup

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)



# In[18]:


#Se busca el link en caso que cambie el nombre o la posición del archivo
page = requests.get('https://www.gub.uy/ministerio-economia-finanzas/datos-y-estadisticas/estadisticas')
soup = BeautifulSoup(page.content, 'html.parser')


# In[19]:


link_xls=[]
for link in soup.find_all('a'):
    if 'Resultados del sector público a setiembre de 2021 (.xlsx 1246 KB)' in link.get_text():
        link_xls.append(link.get('href'))
link_xls


# In[20]:


file_xls = requests.get(link_xls[0])
df = pd.read_excel(file_xls.content, sheet_name = 'Sector Público No Monetario', skiprows=3)


# In[21]:


def replacing_numbers(x):
    x= x.replace(' (1)', '')
    x= x.replace(' (2)', '')
    x= x.replace(' (3)', '')
    x= x.replace(' (4)', '')
    x= x.replace(' (5)', '')
    x = x.replace('á', 'a')
    x = x.replace('é', 'e')
    x = x.replace('í', 'i')
    x = x.replace('ó', 'o')
    x = x.replace('ú', 'u')
    x = x.replace("Á", "A")
    x = x.replace("É", "E")
    x = x.replace("Í", "I")
    x = x.replace("Ó", "I")
    x = x.replace("Ú", "U")       
    return x


# In[22]:


#df = pd.read_excel(r.content, sheet_name = 'Sector Público No Monetario', skiprows=3)
df = df.rename(columns={'Unnamed: 0':'Field'})
df = df.dropna(how='all', subset = df.columns[1:])
df['Field'] = df['Field'].apply(lambda x: replacing_numbers(x))
df = df.replace({'      DGI': 'Gobierno Central - DGI', 
           '      IRP': 'Gobierno Central - IRP', 
           '      Comercio Exterior': 'Gobierno Central - Comercio Exterior',
           '      Otros': 'Gobierno Central - Otros',
           '      Remuneraciones': 'EPCGC - Remuneraciones',
           '      Gastos no personales': 'EPCGC - Gastos no personales',
           '      Pasividades': 'EPCGC - Pasividades',
           '      Transferencias': 'EPCGC - Transferencias',
           '      Gobierno Central-BPS': 'Intereses - Gobierno Central-BPS',
           '      Empresas Públicas': 'Intereses - Empresas Públicas',
           '      Intendencias': 'Intereses - Intendencias',
           '      BSE': 'Intereses - BSE'})
df = df.set_index('Field')
df = df.T
df['Field'] = df.index
df = df.rename(columns={'Field':'Date'})
df = df.set_index('Date')

df = df.replace('-.-', np.nan)

for column in df.columns:
#     print(column)
    df[column] = df[column].astype('float')


df['country'] = 'Uruguay'


# In[23]:


df2 = pd.read_excel(file_xls.content, sheet_name = 'Sector Público Consolidado', skiprows=3)
df2 = df2.rename(columns={'Unnamed: 0':'Field'})
df2 = df2.dropna(how='all', subset = df2.columns[1:])

df2 = df2.set_index('Field')
df2 = df2.T
df2['Field'] = df2.index
df2 = df2.rename(columns={'Field':'Date'})
df2 = df2.set_index('Date')

df2 = df2.replace('-.-', np.nan)

for column in df2.columns:
#     print(column)
    df2[column] = df2[column].astype('float')

correct_columns = []
for column in df2.columns:
    if column[-1] == ' ':
        column = column[:-1]
        correct_columns += [column]
    else:
        correct_columns += [column]

df2.columns = correct_columns
df2['country'] = 'Uruguay'
# df2.to_csv('test_90.csv')


# In[24]:


df3 = pd.read_excel(file_xls.content, sheet_name = 'Gobierno Central - BPS', skiprows=3)
df3 = df3.rename(columns={'Unnamed: 0':'Field'})
df3 = df3.dropna(how='all', subset = df3.columns[1:])
df3['Field'] = df3['Field'].apply(lambda x: replacing_numbers(x))

df3["level_1"] = df3["Field"]

sublevels = {
    'level_2':["Comercio Exterior", "D.G.I.", "Loterias", "Venta de energia", "TGN/otros", "FIMTOP", "Aportes de Emp. Pcas.",
               "IRP", "Recursos Libre Disponibilidad", "Recaudacion Bruta", "Ingresos netos FSS Ley Nº 19.590", "Cert. Dev. Impuest. BPS (-)",
              "Otros Ingresos BPS", "Adm. Central", "Org. Docentes", "Retenc/otros", "BPS", "Caja Policial", "Caja Militar", "Suministros",
              "Plan de Emergencia", "Transferencias GC", "Transferencias BPS", "MTOP", "MVOTMA", "Presidencia", "Resto", "Gobierno Central", "BPS - FSS Ley Nº 19.590 (-)"],
    'level_3':["Ingresos Brutos ", "Cert. Dev. Impuest. DGI (-)", "Entes Publicos", "Serv. Deuda", "Otros Organismos", "Rentas Afectadas",
              "Seguro de Enfermedad", "Asignaciones Familiares y otras prestaciones activas", "Seguro de Desempleo", "- I.R.P./ IRPF desde ago-07",
              "- AFAP", "- Otros", "Otros Egresos"],
} 

for sublevel in sublevels:
    df3[sublevel] = np.nan
    print(sublevel)
    df3.loc[df3["level_1"].isin(sublevels[sublevel]), sublevel] = df3["level_1"]
    df3.loc[df3["level_1"].isin(sublevels[sublevel]), "level_1"] = np.nan 

for x in range(len(sublevels)+1,0,-1):
    print(x)
    df3["level_{}_temp".format(x)] = df3["level_{}".format(x)].ffill()
    df3.loc[df3["level_{}".format(x)].notnull(), "level_{}".format(x)] = df3["level_{}_temp".format(x).strip().replace("\\xa0", "")]
    for y in range(1, x):
        df3.loc[df3["level_{}".format(y)].notnull(), "level_{}_temp".format(x)] = np.nan

df3["concept"] = df3["level_1_temp"]
for y in range (2,len(sublevels)+2):
    df3.loc[df3["level_{}_temp".format(y)].notnull(), "concept"] = df3["concept"] + " - " + df3["level_{}_temp".format(y)]

df3 = df3[df3.columns.drop(list(df3.filter(regex='level')))]
df3 = df3.set_index("concept")
df3 = df3.drop("Field",axis=1)

df3 = df3.T
df3['concept'] = df3.index
df3 = df3.rename(columns={'concept':'Date'})
df3 = df3.set_index('Date')

df3 = df3.replace('-.-', np.nan)

for column in df3.columns:
#     print(column)
    df3[column] = df3[column].astype('float')

correct_columns = []
for column in df3.columns:
    if column[-1] == ' ':
        column = column[:-1]
        correct_columns += [column]
    else:
        correct_columns += [column]

df3.columns = correct_columns

df3['country'] = 'Uruguay'        

df3 = df3.loc[:,~df3.columns.duplicated()]


alphacast.datasets.dataset(89).upload_data_from_df(df, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

alphacast.datasets.dataset(90).upload_data_from_df(df2, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

alphacast.datasets.dataset(91).upload_data_from_df(df3, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

