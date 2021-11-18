#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests
import pandas as pd
from bs4 import BeautifulSoup
import msoffcrypto
import os

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)



# In[ ]:


page= requests.get('https://www.dgi.gub.uy/wdgi/page?2,principal,dgi--datos-y-series-estadisticas--serie-de-datos--recaudacion-anual-y-mensual-por-impuesto,O,es,0,')


# In[ ]:


#Como la pagina tiene los '\&quot;' en vez de las comillas dobles, hay que hacer el reemplazo para que 
#pueda construir el arbol de la pagina
soup = BeautifulSoup(page.content, 'html.parser')
soup2 = BeautifulSoup(str(soup).replace('\&quot;', '"')                       .replace('/&gt;', '>')                      .replace('&gt;', '>')                      .replace('&lt;', '<')                      .replace('\/', '/')                      .replace('&amp;', '&'))


# In[ ]:


#Se obtiene el contenido de la tabla y luego se buscan los links
links = soup2.find('table', id='hgxpc007_865_3').find_all('a')


# In[ ]:


#Se itera para sacar el link
for link in links:
    if 'Recaudación por impuesto - Series mensuales' in link.get_text():
        link_xls= 'https://www.dgi.gub.uy/wdgi/'+ link.get('href')


# In[ ]:


#Descargo y guardo el archivo en el servidor
response = requests.get(link_xls)

with open('DGI_Recaudacion_mensual.xls', 'wb') as f:
    f.write(response.content)


# In[ ]:


##No remover, por si llega a cambiar, esto es para el caso cuando el archivo estaba encriptado

#Desencripta el archivo y lo guarda en un nuevo archivo
#file = msoffcrypto.OfficeFile (open('DGI_Recaudacion_mensual.xls', 'rb'))
#file.load_key (password = 'VelvetSweatshop') 
#file.decrypt(open ('DGI_Recaudacion_mensual_decrypted.xls', 'wb'))


# In[ ]:


#Cargo el archivo a un dataframe

#Antes el archivo estaba protegido, por lo que se mantiene esta línea
#La hoja 0 es la de series
#df = pd.read_excel('DGI_Recaudacion_mensual_decrypted.xls', sheet_name=0)

df = pd.read_excel('DGI_Recaudacion_mensual.xls', sheet_name=0)

#elimino los archivos generados
os.remove("DGI_Recaudacion_mensual.xls")
#os.remove("DGI_Recaudacion_mensual_decrypted.xls")


# In[ ]:


#Se eliminan las columnas con todos NaN
df.dropna(axis=1, how='all', inplace=True)
#Se eliminan las filas 
df.dropna(subset=[df.columns[1]], how='all', inplace=True)


# In[ ]:


df.dropna(axis=1, how='all', inplace=True)
df = df.loc[:, ~df.columns.str.contains('^Unnamed')]


# In[ ]:


df = df.iloc[:, 2:]


# In[ ]:


df.rename(columns={df.columns[0]: 'Date'}, inplace=True)
df['Date'] = pd.to_datetime(df['Date'])


# In[ ]:


df.set_index('Date', inplace=True)
df['country'] = 'Uruguay'

alphacast.datasets.dataset(7448).upload_data_from_df(df, 
    deleteMissingFromDB = False, onConflictUpdateDB = True, uploadIndex=True)


