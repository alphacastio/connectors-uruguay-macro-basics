#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from datetime import datetime
import requests
from alphacast import Alphacast
from dotenv import dotenv_values


# In[2]:


#Armo un diccionario con las urls de los archivos y sus respectivas hojas para el loop
urlDict = {'constantes': ['https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas Nacionales/1. Actividades_K.xlsx',
                         'Valores_K'], 
          'corrientes': ['https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas Nacionales/2. Actividades_C.xlsx',
                         'Valores_C'],
          'IVF':['https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas%20Nacionales/5.%20Desestacionalizado.xlsx',
                'IVF']}


# In[3]:


#Hago un loop sobre las keys y los items del diccionario y voy creando dataframes
dfFinal = pd.DataFrame([])
for key, value in urlDict.items():
    r = requests.get(value[0], allow_redirects=False)
    df= pd.read_excel(r.content, skiprows=7, sheet_name=value[1])
    df = df[df.columns[1:]]
    df = df.dropna(how='all').dropna(how='all', axis=1)
    df = df.set_index('Unnamed: 1')
    df = df.T
    df = df.reset_index()
    
    #Armo una funcion para pasar los trimestres a fechas
    def dateWrangler(x):
        x=str(x)
        x= x.replace('*','')
        list_x = x.split(' ')
        if list_x[0] == 'I':
            y = list_x[1]+'-01-01'
        elif list_x[0] == 'II':
            y = list_x[1]+'-04-01'
        elif list_x[0] == 'III':
            y = list_x[1]+'-07-01'
        elif list_x[0] == 'IV':
            y = list_x[1]+'-10-01'

        return y
    
    #Aplico la función a la columna de fecha y la seteo como indice
    df['index'] = df['index'].apply(lambda x: dateWrangler(x))
    df['index'] = df['index'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
    df = df.rename(columns = {'index': 'Date'})
    df = df.set_index('Date')
    
    #Agrego el sufijo a las variables para que se distingan valores constantes y corrientes
    newCols=[]
    for col in df.columns:
        newCols += [col +' - '+ key]
        
    df.columns = newCols
    
    #Hago un merge de los dataframes con el df vacío que cree al comienzo
    dfFinal = dfFinal.merge(df, how='outer', left_index=True, right_index=True)


# In[4]:


#Agrego la columna country
dfFinal['country'] = 'Uruguay'


# In[5]:


API_KEY = dotenv_values(".env").get("ALPHACAST_API_KEY")

alphacast = Alphacast(API_KEY)


# In[7]:


#Cargo la data al sitio de Alphacast
alphacast.datasets.dataset(284).upload_data_from_df(dfFinal, 
                 deleteMissingFromDB = False, onConflictUpdateDB = True, uploadIndex=True)


# In[ ]:




