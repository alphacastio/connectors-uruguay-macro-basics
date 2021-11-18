#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
import PyPDF2
from io import BytesIO
import tabula

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)



# In[ ]:


url = 'https://cinve.org.uy/category/publicaciones/informes/'
page_html = requests.get(url).text


# In[ ]:


soup = BeautifulSoup(page_html, 'html.parser')
#Extraigo los links principales (3 con imagenes) y los de las entradas
links_principales = soup.find_all('a', {'class':'td-image-wrap'})
div_links = soup.find_all('div', attrs={'class': 'td_module_flex td_module_flex_1 td_module_wrap td-animation-stack'})


# In[ ]:


#Extraigo los links que figuran con imagen en la parte superior de la pagina
links1 = []
for link in links_principales:
    links1.append(link.get('href'))


# In[ ]:


#Extraigo los links de las entradas en la parte inferior
links2 = []
for link in div_links:
    links2.append(link.a['href'])

#Elimino los ultimos 5 que corresponden a los más vistos
links2 = links2[:-5]


# In[ ]:


#Junto ambas listas
links = links1 + links2


# In[ ]:


# Itero sobre las urls recopiladas
# extraigo los links de cada pagina
# itero sobre los links extraidos para encontrar la leyenda y el patron en el nombre del archivo
# Si lo encuentra, lo agrega a una lista. Se evalua si la lista sigue vacia y si no esta vacia, sale del loop

for link in links:
    response_html = requests.get(link).text
    soup_html = BeautifulSoup(response_html, 'html.parser')
    
    links_html = soup_html.find_all('a')
    
    list_pdf = []
    
    for link_html in links_html:
        if ('Acceder al informe completo' in link_html) and ('/II' in link_html.get('href')):
            list_pdf.append(link_html.get('href'))
    if len(list_pdf) > 0:
        break
    #Bajar la pagina
    #Buscar si hay un link que tenga "Acceder a este informe" y pdf, sino seguir con el proximo link
    #En el pdf descargado hay que ir a la penultima pagina y leer la tabla


# In[ ]:


response_pdf = requests.get(list_pdf[0])

#Hago la lectura del pdf
pdf_cinve = BytesIO(response_pdf.content)
read_cinve = PyPDF2.PdfFileReader(pdf_cinve)
# Cuento la cantidad de paginas. El anexo esta en la penultima pagina
pagina_anexo = read_cinve.getNumPages() - 1


# In[ ]:


#Leo con tabula y lo paso a un dataframe
df = tabula.read_pdf(BytesIO(response_pdf.content), pages = pagina_anexo)[0]

#Se convierte a un array y luego a un dataframe porque la primera fila aparece como el nombre de las columnas
df = pd.DataFrame(np.vstack([df.columns, df]))


# In[ ]:


#Evaluo que se haya leido bien y entonces hago todas las operaciones

if len(df.columns) == 11:
    df.columns = ['Date', 'Bienes Elaborados No Energéticos (1)',  'Servicios No Administrados (2)',
                  'Servicios Administrados (3)', 'Inflación tendencial (1+2+3)', 'Alimentos No Elaborados (4)',
                 'Energéticos (5)', 'Servicios administrados públicos (6)', 'Tabaco (7)',
                 'Inflación residual (4+5+6+7)', 'Inflación (1+2+3+4+5+6+7)']
else:
    raise('Se leyo mal el pdf')
    
dict_meses = {'Ene': '01', 'Feb': '02', 'Mar': '03', 'Abr': '04', 'May':'05', 'Jun':'06',
             'Jul':'07', 'Ago':'08', 'set':'09', 'sep':'09', 'Oct':'10', 'Nov':'11', 'Dic':'12'}
dict_meses2 = {'ene': '01', 'feb': '02', 'mar': '03', 'abr': '04', 'may':'05', 'jun':'06',
             'jul':'07', 'ago':'08', 'Set':'09', 'Sep':'09', 'oct':'10', 'nov':'11', 'dic':'12'}

df['Date'].replace(dict_meses, regex=True, inplace=True)
df['Date'].replace(dict_meses2, regex=True, inplace=True)
df['Date'] = pd.to_datetime(df['Date'], format='%m-%y')


# In[ ]:


df.set_index('Date', inplace=True)
df['country'] = 'Uruguay'

alphacast.datasets.dataset(644).upload_data_from_df(df, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

