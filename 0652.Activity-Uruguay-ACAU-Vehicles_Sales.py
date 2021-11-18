#!/usr/bin/env python
# coding: utf-8

# In[13]:


import pandas as pd

import requests
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)



# In[14]:


url_base = 'http://www.acau.com.uy//panel/estadisticas/'

files = {
#         2006:{
#             'file':'09_45_53ar1.xls',
#             ''
#         },
        2007: {
            'file':'09_46_13ar1.xls',
            'inicio':'        E.-   ACUMULADO  DE  VENTAS  MERCADO TOTAL',
            'final':'TOTAL GENERAL', 
            'col_inicial':'Unnamed: 1',
            'filas_delete':['Unnamed: 14','Unnamed: 15']
        },
        2008: {
            'file':'09_46_27ar1.xls',
            'inicio':'        E.-   ACUMULADO  DE  VENTAS  MERCADO TOTAL',
            'final':'TOTAL GENERAL', 
            'col_inicial':'Unnamed: 1',
            'filas_delete':['Unnamed: 14','Unnamed: 15']
        },
        2009: {
            'file':'09_46_39ar1.xls',
            'inicio':'        E.-   ACUMULADO  DE  VENTAS  MERCADO TOTAL',
            'final':'TOTAL GENERAL', 
            'col_inicial':'Unnamed: 1',
            'filas_delete':['Unnamed: 14','Unnamed: 15']
        },
        2010: {
            'file':'11_32_59ar1.xls',
            'inicio':'        D.-   ACUMULADO  DE  VENTAS  MERCADO TOTAL',
            'final':'TOTAL GENERAL', 
            'col_inicial':'Unnamed: 1',
            'filas_delete':['Unnamed: 14','Unnamed: 15']
        },
        2011: {
            'file':'11_32_52ar1.xls',
            'inicio':'        D.-   ACUMULADO  DE  VENTAS  MERCADO TOTAL',
            'final':'TOTAL GENERAL', 
            'col_inicial':'Unnamed: 1',
            'filas_delete':['Unnamed: 14','Unnamed: 15']
        },
        2012: {
            'file':'11_32_43ar1.xls',
            'inicio':'        D.-   ACUMULADO  DE  VENTAS  MERCADO TOTAL',
            'final':'TOTAL GENERAL', 
            'col_inicial':'Unnamed: 1',
            'filas_delete':['Unnamed: 14','Unnamed: 15']
        },
        2013: {
            'file':'11_32_35ar1.xls',
            'inicio':'        D.-   ACUMULADO  DE  VENTAS  MERCADO TOTAL',
            'final':'TOTAL GENERAL', 
            'col_inicial':'Unnamed: 1',
            'filas_delete':['Unnamed: 14','Unnamed: 15']
        },
        2014: {
            'file':'11_32_24ar1.xls',
            'inicio':'        F.-   ACUMULADO  DE  VENTAS  MERCADO TOTAL',
            'final':'TOTAL GENERAL', 
            'col_inicial':'Unnamed: 0',
            'filas_delete':['Unnamed: 13','Unnamed: 14']
        },
        2015: {
            'file':'11_17_59ar1.xls',
            'inicio':'        G.-   ACUMULADO  DE  VENTAS  MERCADO TOTAL',
            'final':'TOTAL GENERAL', 
            'col_inicial':'Unnamed: 0',
            'filas_delete':['Unnamed: 13','Unnamed: 14']
        },
        2016: {
            'file':'11_17_45ar1.xls',
            'inicio':'        G.-   ACUMULADO  DE  VENTAS  MERCADO TOTAL',
            'final':'TOTAL GENERAL',
            'col_inicial':'Unnamed: 0',
            'filas_delete':['Unnamed: 13','Unnamed: 14']
        },
        2017: {
            'file':'11_17_33ar1.xls',
            'inicio':'        G.-   ACUMULADO  DE  VENTAS  MERCADO TOTAL',
            'final':'TOTAL GENERAL',     
            'col_inicial':'Unnamed: 0',
            'filas_delete':['Unnamed: 13','Unnamed: 14']
        },
        2018: {
            'file':'13_58_55ar1.xlsx',
            'inicio':'G.-   ACUMULADO  DE  VENTAS  MERCADO TOTAL',
            'final':'TOTAL GENERAL',     
            'col_inicial':'Unnamed: 0',
            'filas_delete':['Unnamed: 13','Unnamed: 14']
        },
        2019: {
            'file':'15_25_23ar1.xlsx',
            'inicio':'G.- MERCADO TOTAL',
            'final':'TOTAL GENERAL',     
            'col_inicial':'Unnamed: 1',
            'filas_delete':['Unnamed: 14','Unnamed: 15']
        },
        2020: {
            'file':'15_15_59ar1.xlsx',
            'inicio':'G.- MERCADO TOTAL 2020',
            'final':'TOTAL GENERAL',     
            'col_inicial':'Unnamed: 1',
            'filas_delete':['Unnamed: 14','Unnamed: 15']
        },
        2021: {
            'file':'18_05_35ar1.xlsx',
            'inicio':'G.- MERCADO TOTAL 2021',
            'final':'TOTAL GENERAL',     
            'col_inicial':'Unnamed: 1',
            'filas_delete':['Unnamed: 14','Unnamed: 15']
        }
}    


# In[15]:


#r = requests.get('http://www.acau.com.uy//panel/estadisticas/{}'.format(files[key]['file']), allow_redirects=False, verify=False)
dfFinal = pd.DataFrame()
for key in files.keys():
    r = requests.get('http://www.acau.com.uy//panel/estadisticas/{}'.format(files[key]['file']), allow_redirects=False, verify=False)
    df = pd.read_excel(r.content, sheet_name = 'Sheet1')

    df = df.dropna(how='all',axis=0).dropna(how='all',axis=1)
    df = df.set_index(files[key]['col_inicial'])

    df = df.loc[files[key]['inicio']:files[key]['final']]
    df = df.dropna(how='all', subset= df.columns[1:]).dropna(how='all',axis=1)

    df = df.T
    df = df.drop(files[key]['filas_delete'],axis=0)

    df['Day'] = 1
    df['Year'] = key
    df['Month'] = df['MARCA'].str.lower()
    df['Month'] = df['Month'].replace(
            {
                "ene": "01",
                "feb": "02",
                "mar": "03",
                "abr": "04",
                "may": "05",
                "jun": "06",
                "jul": "07",
                "ago": "08",
                "set": "09",
                "oct": "10",
                "nov": "11",
                "dic": "12",

            }
    )

    df['Date'] = pd.to_datetime(df[["Year", "Month", "Day"]])
    df = df.drop(['Year','Month','Day','MARCA'], axis=1)
    #df = df.drop('MARCA', axis=1)
    
    df = df.rename(columns={
        'SUB-TOTAL AUTOMOVILES':'TOTAL AUTOMOVILES',
        'SUB-TOTAL UTILITARIOS':'TOTAL UTILITARIOS',
        'SUB-TOTAL CAMIONES':'TOTAL CAMIONES',
        'SUB-TOTAL OMNIBUS':'TOTAL OMNIBUS',
        'SUB-TOTAL AUTOM + UTIL.':'TOTAL AUTOM + UTIL.',
        'SUB-TOTAL SUV':'TOTAL SUV',
        'SUB- TOTAL MINIBUSES':'TOTAL MINIBUSES',
        'SUB-TOTAL  AUT + SUV':'TOTAL  AUT + SUV'
    })
    
    df = df.set_index('Date')
    dfFinal = dfFinal.append(df)

dfFinal


# In[16]:


dfFinal['country'] = 'Uruguay'

alphacast.datasets.dataset(652).upload_data_from_df(dfFinal, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

