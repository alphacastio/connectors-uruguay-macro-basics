#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd
from datetime import datetime
import requests

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)



# In[2]:


#Defino un diccionario con la clasificacion, url y nombre de solapa
urlDict = {'Constantes': ['https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas Nacionales/1. Gasto_K.xlsx',
                         'Valores_K'], 
          'Corrientes': ['https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas Nacionales/2. Gasto_C.xlsx',
                         'Valores_C'],
          'IVF' :['https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Cuentas Nacionales/3. Gasto_IVF.xlsx', 'IVF']}


# In[3]:


#Creo un dataframe vacio
dfFinal = pd.DataFrame([])

#Itero sobre los items del diccionario
for key, value in urlDict.items():
    
    #Hace el request de la url del archivo
    r = requests.get(value[0], allow_redirects=False)
    
    #Convierte a un dataframe y elimina las primeras 7 filas
    df= pd.read_excel(r.content, skiprows=7, sheet_name= value[1], engine='openpyxl')
       
    #Si la primera columna tiene valores con una extension de 4 (es un subnivel) y comienza con P
    #le asigno un Nan, sino copio el valor de la columna 1. Esto sirve como una mascara para el nivel
    df['Nivel'] = [np.nan if (fila.startswith('P') and len(fila) == 4) else df[df.columns[1]][indice] 
               for indice, fila in enumerate(df[df.columns[0]])]
    #Hago un ffilna con metodo forward
    df['Nivel'].fillna(method='ffill', inplace=True)
    
    
    #Si el valor de la columna 1 coincide con el de nivel, mantengo el original, sino concateno
    #Nivel - columna 1
    df[df.columns[1]] = [df[df.columns[1]][indice] if df[df.columns[1]][indice] == df['Nivel'][indice] 
                     else str(df['Nivel'][indice]) + ' - ' + str(df[df.columns[1]][indice])
                     for indice, fila in enumerate(df[df.columns[1]])]

    #Filtro las filas que en base a la columna 0 tengan un largo menor a 5
    df = df[df[df.columns[0]].apply(lambda x: len(x) < 5)]
    
    #Hago drop de la columna 0 y de nivel
    df.drop([df.columns[0], 'Nivel'], axis=1, inplace=True)

    #Hago un drop de NaN en base a columnas
    df.dropna(how='all', axis=1 , inplace=True)
    
    #Seteo como indice la columna de niveles y subniveles
    df.set_index(df.columns[0], inplace=True)

    #Hago un drop si no tienen datos en filas
    #Esta fila no se corre para no eliminar la variación de existencias que solo tiene NaN
    #df.dropna(how='all', axis = 0, inplace=True)

    df = df.T
    df = df.reset_index()
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
    df["index"] = df["index"].str.replace("202I", "2021")
    df['index'] = df['index'].apply(lambda x: dateWrangler(x))
    df['index'] = df['index'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
    df = df.rename(columns = {'index': 'Date'})
    df = df.set_index('Date')

    #Agrego el sufijo
    df = df.add_suffix(' - ' + key)
    #reemplazo el "(-) " adelante de la categoria de impo
    df.columns = df.columns.str.replace('\(.*\) ', '')
    
    #Hago el merge de los dataframes
    dfFinal = dfFinal.merge(df, how='outer', left_index=True, right_index=True)


# In[4]:


dfFinal['country'] = 'Uruguay'

alphacast.datasets.dataset(285).upload_data_from_df(dfFinal, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

