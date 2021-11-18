#!/usr/bin/env python
# coding: utf-8

# In[95]:


import pandas as pd
import requests
import io
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)



# In[96]:





# In[97]:


url_activity = 'https://www.ine.gub.uy/c/document_library/get_file?uuid=30bd4550-daf9-433c-b88f-06981f4c139a&groupId=10181'
url_unemploy = 'https://www.ine.gub.uy/c/document_library/get_file?uuid=04cfc08e-4080-41e5-870d-560daaccf347&groupId=10181'
url_employmnt= 'https://www.ine.gub.uy/c/document_library/get_file?uuid=764f1c52-eab3-466e-90c7-3f7ad1d0ef44&groupId=10181'


# In[98]:


urls = [url_activity, url_unemploy, url_employmnt]

datasets=[]
for url in urls:
    r = requests.get(url, allow_redirects=False, verify=False)
    with io.BytesIO(r.content) as datafr:
        df = pd.read_excel(datafr, sheet_name='Mensual', skiprows = 4)

    df = df[['Mes', 'Total Pais']]
    df = df.dropna(how='all')
    datasets += [df]


# In[99]:


df= datasets[0].merge(datasets[1], on= 'Mes').merge(datasets[2], on='Mes')
finalrow = df[df['Mes'] == 'Fuente : Instituto Nacional de Estadística (INE).'].index[0]
finalrow-=1
df = df[:finalrow]
df.columns = ['Date', 'Total Activity', 'Total Unemployment', 'Total Employment']
df['country'] = 'Uruguay'
df = df.loc[:,~df.columns.duplicated()]

df2 = pd.read_html('https://www.ine.gub.uy/web/guest/indicadores?indicadorCategoryId=67534')
df2 = pd.DataFrame(df2[0])
df2.columns = ['Indicador', 'Año', 'Mes', 'Total', 'Hombres', 'Mujeres']
df2["fecha"] = df2["Año"].astype(str) + "-" + df2["Mes"].astype(str)
indice = df2[df2["fecha"] == "2020-Marzo"].index
indice = indice + 1
df2 = df2.sort_index(ascending=False)
df2
df3 = pd.DataFrame(df2["Total"])
df3.index = pd.date_range(start='11/1/2019', periods=20, freq="MS")
df3.index.name = 'Date'
df3.columns= ["Total Unemployment"]
df3["country"] = "Uruguay"
df.index = pd.to_datetime(df["Date"])
del df["Date"]

df_total = df.combine_first(df3)

alphacast.datasets.dataset(113).upload_data_from_df(df_total, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

