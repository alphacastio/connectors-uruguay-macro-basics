#!/usr/bin/env python
# coding: utf-8

# In[79]:


import requests
import pandas as pd
from bs4 import BeautifulSoup
from functools import reduce

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)



# In[80]:


#Se busca el link en caso que cambie el nombre del archivo
page = requests.get('https://www.bcu.gub.uy/Paginas/Default.aspx')
soup = BeautifulSoup(page.content, 'html.parser')


# In[81]:


link_xls=[]
for link in soup.find_all('a'):
    if 'Deuda Externa del Uruguay (informe completo en formato planilla de cálculo)' in link.get_text():
        link_xls.append('https://www.bcu.gub.uy' + link.get('href'))


# In[82]:


file_xls = requests.get(link_xls[0])
df0 = pd.read_excel(file_xls.content, sheet_name='DEBU', skiprows=4)


# In[83]:


#Elimino la primera columna que contiene astericos
df0.drop(df0.columns[0], axis=1,  inplace=True)


# In[84]:


#Determino cual es la primera fila de cada tabla, tambien se podria hacer con el año 1999
filas_0 = df0[df0[df0.columns[1]] == 'Total'].index


# In[86]:


#Voy separando cada tabla en un dataframe diferente para que sea mas facil limpiarlo

#Deuda Externa Bruta del Uruguay por instrumento
df1 = df0.copy()
df1 = df1.iloc[filas_0[1]:filas_0[2],]

# #Deuda Externa Bruta del Uruguay por plazo y moneda
df2 = df0.copy()
df2 = df2.iloc[filas_0[2]:,]

#Deuda Externa Bruta del Uruguay por deudor
df0 = df0.iloc[:filas_0[1],]


# In[87]:


#####################################################
####Deuda Externa Bruta del Uruguay por deudor
#####################################################

#Para eliminar las columnas innecesarias
df0.dropna(how = 'all', axis = 1, inplace=True)


# In[88]:


#No es la mejor opcion pero para resolver rápido el anidado de los nombres de las columnas, se
#verifica cuantas columnas quedaron y se renombran
if df0.shape[1] == 12:
    df0.columns = ['Date', 'Total', 'Por deudor - SPNM', 'Por deudor - SPNM - Gobierno General', 
                   'Por deudor - SPNM - Gobierno General - Central',
                  'Por deudor - SPNM - Gobierno General - Locales', 'Por deudor - SPNM - Sociedades Públicas', 
                   'Por deudor - SPNM - Sociedades Públicas - No Financieras',
                   'Por deudor - Sector Público - Sociedades Públicas - BCU',
                  'Por deudor - Banca Pública', 'Por deudor - Sector Privado Bancario', 
                   'Por deudor - Sector Privado No Bancario']
else:
    raise('Cambio la estructura')


# In[89]:


#Se eliminan las primeras filas del dataframe que no tienen datos
df0 = df0.iloc[5:,]
df0.dropna(subset=['Total'], inplace=True)


# In[90]:


#####################################################
####Deuda Externa Bruta del Uruguay por instrumento
#####################################################

#Como hay celdas con texto en blanco (no puedo eliminar en base a un dropna)
#Mantenemos las primeras 8 columnas
df1 = df1.iloc[: , :8]
df1.reset_index(drop=True, inplace=True)

#Se eliminan las primeras filas que no tienen datos
df1 = df1.iloc[5:,]

# #Como en el caso de la primera tabla, se renombran las columnas de 
df1.columns = ['Date', 'Total por instrumento', 'Por instrumento - Títulos de Deuda - Títulos', 
               'Por instrumento - Títulos de Deuda - Bonos Brady', 
              'Por instrumento - Préstamos Internacionales', 'Por instrumento - Créditos Comerciales',
               'Por instrumento - Depósitos', 'Por instrumento - Otros']

df1.dropna(subset=['Total por instrumento'], inplace=True)


# In[91]:


#####################################################
####Deuda Externa Bruta del Uruguay por plazo y moneda
#####################################################

df2.dropna(how='all', axis=1, inplace=True)

if df2.shape[1] == 10:
    df2.columns = ['Date', 'Total por plazo y moneda', 'Por plazo y moneda - Por plazo contractual - Hasta 1 año', 
                   'Por plazo y moneda - Por plazo contractual - Más de 1 año', 
                   'Por plazo y moneda - Por moneda - Pesos', 'Por plazo y moneda - Por moneda - Dólares', 
                   'Por plazo y moneda - Por moneda - Yenes', 'Por plazo y moneda - Por moneda - Euros',
                   'Por plazo y moneda - Por moneda - DEG', 'Por plazo y moneda - Por moneda - Otras']
else:
    raise('cambio el archivo')

df2.reset_index(drop=True, inplace=True)

df2 = df2.iloc[6:, ]


# In[93]:


#Se calcula un rango de fechas para cada dataframe en caso que tengan diferente cantidad de datos
fechas0 = pd.date_range(start = '1999-10-01', periods=df0.shape[0], freq='QS')
fechas1 = pd.date_range(start = '1999-10-01', periods=df1.shape[0], freq='QS')
fechas2 = pd.date_range(start = '1999-10-01', periods=df1.shape[0], freq='QS')


# In[94]:


#Reemplazo las fechas en cada dataframe y seteo la columna como indice
df0['Date'] = fechas0
df1['Date'] = fechas1
df2['Date'] = fechas2

df0.set_index('Date', inplace=True)
df1.set_index('Date', inplace=True)
df2.set_index('Date', inplace=True)


# In[95]:


data_frames = [df0, df1, df2]


# In[96]:


#Junto todos los dataframes
df = reduce(lambda  left,right: pd.merge(left,right,left_index=True, right_index=True,
                                            how='outer'), data_frames)


# In[97]:


df = df.drop(['Total por instrumento','Total por plazo y moneda'], axis=1)
df = df.replace('N/D', pd.NA)
df['country'] = 'Uruguay'

alphacast.datasets.dataset(7792).upload_data_from_df(df, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)
