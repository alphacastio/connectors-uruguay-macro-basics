#!/usr/bin/env python
# coding: utf-8

# In[21]:


import pandas as pd
from datetime import datetime
import requests

import regex as re
import numpy as np
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)



# In[22]:


url = 'https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Balanza%20de%20Pagos/dse_bp_m6_arm_scn.xlsx'
r = requests.get(url, allow_redirects=False)
df = pd.read_excel(r.content, skiprows=9, sheet_name='Cuadro Nº 1')

df = df = df.dropna(how='all').dropna(how='all', axis=1)
df = df.dropna(how='all', subset=df.columns[1:])
df


# In[23]:


df["level_1"] = df["Unnamed: 0"]

sublevels = {
    'level_2':["1.A. Bienes y Servicios", "1.B Ingreso Primario (2)", "1.C Ingreso Secundario (3)", "3.1 Inversión directa", "3.2 Inversión de cartera", 
               "3.3 Derivados financieros (distintos de reservas)", "3.4 Otra inversión (4)", "3.5 Activos de Reserva BCU"],
    'level_3':["1.A.a Bienes", "1.A.b Servicios", "\xa0\xa0\xa0\xa0\xa0\xa0Adquisición neta de activos financieros", "\xa0\xa0\xa0\xa0Pasivos netos incurridos", "      Crédito", "      Débito"],
    'level_4':["Crédito", "Débito", "Débito "],
    'level_5':["Mercancías generales (1.A.a.1) ", "Exportaciones netas de bienes en compra venta (1.A.a.2)", "Transportes (1.A.b.3 )", "Viajes (1.A.b.4) ",
              "Otros Servicios (1)", "    Por Sector Institucional", "   Por Categoría Funcional", "Por Instrumento y Sector Institucional"],
    'level_6':["De los cuales:  Soc.  principalmente dedicadas a la compraventa", "Bienes adquiridos en virtud de compraventa (crédito negativo)", "Bienes vendidos en virtud de compraventa",
              "Sector Público", "Sector Privado", " Inversión Directa (1.B.2.1)", " Inversión de Cartera (1.B.2.2)", "Otra Inversión (1.B.2.3 )", "Activos de Reserva (1.B.2.4 )",
               "Participaciones de Capital y en Fondos de Inversión (3.1.1 )", "Instrumentos de Deuda (3.1.2)", "Participaciones de Capital y en Fondos de Inversión (3.2.1) ", "Títulos de Deuda (3.2.2 )",
              "Sector Público (neto)", "Sector Privado (neto)", "Moneda y Depósitos (3.4.2)", "Préstamos (3.4.3)", "Créditos y anticipos comerciales (3.4.5)", "Otras cuentas por cobrar (3.4.6)", "Derechos Especiales de Giro (3.4.7)"],
    'level_7':["De los cuales:  Sociedades principalmente dedicadas a la compraventa", "Banco Central", "Sociedades Captadoras de Depósitos, excepto el BC (5)", "Gobierno General", "Otros Sectores (5)"]
} 

for sublevel in sublevels:
    df[sublevel] = np.nan
    print(sublevel)
    df.loc[df["level_1"].isin(sublevels[sublevel]), sublevel] = df["level_1"]
    df.loc[df["level_1"].isin(sublevels[sublevel]), "level_1"] = np.nan 
df


# In[24]:


for x in range(len(sublevels)+1,0,-1):
    print(x)
    df["level_{}_temp".format(x)] = df["level_{}".format(x)].ffill()
    df.loc[df["level_{}".format(x)].notnull(), "level_{}".format(x)] = df["level_{}_temp".format(x).strip().replace("\\xa0", "")]
    for y in range(1, x):
        df.loc[df["level_{}".format(y)].notnull(), "level_{}_temp".format(x)] = np.nan
df


# In[25]:


df["concept"] = df["level_1_temp"]
for y in range (2,len(sublevels)+2):
    df.loc[df["level_{}_temp".format(y)].notnull(), "concept"] = df["concept"] + " - " + df["level_{}_temp".format(y)]
df


# In[26]:


df = df[df.columns.drop(list(df.filter(regex='level')))]
df = df.set_index("concept")
df = df.drop("Unnamed: 0",axis=1)
df


# In[27]:


df1 = df.transpose()
df1 = df1.reset_index()
pd.set_option("display.max_columns", None)
df1


# In[28]:


def dateWrangler(x):
    x=str(x)
    x= x.replace('*','')
    list_x = x.split('.')
    if list_x[1] == 'I':
        y = list_x[0]+'-01-01'
    elif list_x[1] == 'II':
        y = list_x[0]+'-04-01'
    elif list_x[1] == 'III':
        y = list_x[0]+'-07-01'
    elif list_x[1] == 'IV':
        y = list_x[0]+'-10-01'
    return y
    
df1['Date'] = df1['index'].apply(lambda x: dateWrangler(x))
df1['Date'] = pd.to_datetime(df1['Date'])
#df['index'] = df['index'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
#df = df.rename(columns = {'index': 'Date'})
del df1["index"]
df1 = df1.set_index("Date")
#df.columns = list(df.columns)
df1


# In[29]:


for col in df1.columns:
        new_col = re.sub('[()]', ' ', col).replace("\n","").replace("+","").replace("1.", "").replace("2.", "").replace("3.", "").replace("4.", "").replace(".1", "").replace(".2", "").replace(".3", "").replace(".4", "").replace("A.", "").replace(" a ","").replace(" b ","").replace(" B ","").replace(" C ","").replace(".5", "").replace(".6", "").replace("(*)", "").replace("(a)", "").replace(" 1 ","").replace(" 2 ","").replace(" 3 ","").replace(" 4 ","").replace(" 5 ","").replace(" 6 ","").replace(" 7 ","")
        df1 = df1.rename(columns={col: new_col})  

df1.columns = list(df1.columns)               
df1


# In[30]:


df1['country'] = 'Uruguay'

alphacast.datasets.dataset(287).upload_data_from_df(df1, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)


# In[ ]:




