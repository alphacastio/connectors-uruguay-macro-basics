#!/usr/bin/env python
# coding: utf-8

# In[56]:


import pandas as pd

import numpy as np
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)



# In[57]:





# In[171]:


def fix_date(df):
    df["Date"] = df.index
    df["year"] = pd.to_numeric(df["Date"], errors="coerce", downcast="integer")    
    df["month"] = 12
    df["day"] = 1
    df["Date_1"] = pd.to_datetime(df[["year", "month", "day"]], errors="coerce")           
    del df["day"]
    del df["month"]
    del df["year"]
#     print(df["Date_1"])
    df["Date_2"] = np.nan
    df.loc[df["Date_1"].isnull(), "Date_2"] = df["Date"]
    df["Date_2"] = df["Date_2"].astype("str").str.replace("III", "07")
    df["Date_2"] = df["Date_2"].astype("str").str.replace("II", "04")
    df["Date_2"] = df["Date_2"].astype("str").str.replace("I", "01")
    df["Date_2"] = pd.to_datetime(df["Date_2"], format="%m.%y", errors="coerce")
    df.replace({pd.NaT: np.nan})
    
#     print(df["Date_2"])    
    df["Date"] = df["Date_1"].fillna(df["Date_2"])
#     print(df["Date"])
    del df["Date_1"]
    del df["Date_2"]
    df = df[df["Date"].notnull()].set_index("Date")
    return df


# In[175]:


def cut_data(df, date_field, cut_row_from, cut_row_to):
    if cut_row_to:
        cut_data = df[df[date_field].str.find(cut_row_to)>= 0].index.min()        
        df = df[:cut_data]    
    cut_data = df[df[date_field].str.find(cut_row_from)>= 0].index.min() + 2
    df = df[cut_data:]
    del df[df.columns[0]]
    return df

def load_data(sheet_name):
    url = "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Endeudamiento%20Externo%20Pblico/resdep.xls"
    return pd.read_excel(url, sheet_name=sheet_name)


# In[176]:


def nested_levels(df, column_list):
    df = df.transpose()
    sublevels = []
    for col in column_list:
        sublevels.append(df.columns[col])
    temp_sub = []
    print (sublevels)
    for x in sublevels:
        print(x)
        df["level_{}_temp".format(x)] = df[x].ffill()
        #df.loc[df[x].notnull(), x] = df["level_{}_temp".format(x)]    
        for y in temp_sub:
            df.loc[df[y].notnull(), "level_{}_temp".format(x)] = np.nan
            #print(str(x) + " - " + str(y))    
        temp_sub.append(x)

    df["concept"] = df["level_{}_temp".format(sublevels[0])]  

    for y in sublevels[1:]:
        df.loc[df["level_{}_temp".format(y)].notnull(), "concept"] = df["concept"] + " - " + df["level_{}_temp".format(y)]
    if prefix:
        df["concept"] = prefix + " - " + df["concept"]

    df = df[df[df.columns[60]].notnull()]
    df = df[df.columns.drop(list(df.filter(regex='level_')))]
    df = df[df.columns.drop(sublevels)]
    df.dropna(axis=0, how='all',inplace=True)
    df.dropna(axis=1, how='all',inplace=True)

    df = df.set_index("concept")

    new_header = df.iloc[0] #grab the first row for the header
    df = df[1:] #take the data less the header row
    df.columns = new_header #set the header row as the df header
    df = df.transpose()
    return df



# In[217]:


tables = {
    344:  [
        {
            "from": "1. Deuda Externa Pública Bruta por deudor.",
            "to": "2. Deuda Externa Pública Bruta por acreedor.", 
            "sheet_name": "DEB total 1", 
            "prefix": "Por Deudor",
            "nested_levels": [0, 2, 3]
        }, 
        {
            "from": "2. Deuda Externa Pública Bruta por acreedor.",
            "to": "3. Deuda Externa Pública Bruta por instrumento.", 
            "sheet_name": "DEB total 1", 
            "prefix": "Por Acreedor",
            "nested_levels": [0, 2, 3, 4, 5, 6]
        }
    ],
    345 : [
        {
            "from": "3. Deuda Externa Pública Bruta por instrumento.",
            "to": None, 
            "sheet_name": "DEB total 1", 
            "prefix": "Por Instrumento",
            "nested_levels": [0, 2, 3]
        },
        {
            "from": "4. Deuda Externa Pública Bruta por plazo y moneda. ",
            "to": "Estructura a", 
            "sheet_name": "DE total 2", 
            "prefix": "Por Plazo y Moneda",
            "nested_levels": [0, 3, 4]
        },        
        {
            "from": "5. Deuda Externa Pública Bruta por tipo de tasa de interés.",
            "to": None, 
            "sheet_name": "DE total 2", 
            "prefix": "Por Tipo de Tasa",
            "nested_levels": [0]
        },                
    ], 
    346 : [
        {
            "from": "6. Deuda Externa Bruta del Sector Público no Monetario por acreedor.",
            "to": "7. Deuda Externa Bruta del Sector Público no Monetario por instrumento.", 
            "sheet_name": "DEB SPNM", 
            "prefix": "Por Acreedor",
            "nested_levels": [0, 2, 3, 4, 5, 6]
        },
        {
            "from": "7. Deuda Externa Bruta del Sector Público no Monetario por instrumento.",
            "to": "8. Deuda Externa Bruta del Sector Público no Monetario por plazo y moneda.", 
            "sheet_name": "DEB SPNM", 
            "prefix": "Por Instrumento",
            "nested_levels": [0, 2, 3]
        },
        {
            "from": "8. Deuda Externa Bruta del Sector Público no Monetario por plazo y moneda. ",
            "to": None, 
            "sheet_name": "DEB SPNM", 
            "prefix": "Por Plazo y Moneda",
            "nested_levels": [0, 3, 4]
        },                
    ],
    347 : [
        {
            "from": "9. Deuda Externa Bruta del BCU por acreedor.",
            "to": "10. Deuda Externa Bruta del BCU por instrumento.", 
            "sheet_name": "DEB BCU", 
            "prefix": "Por Acreedor",
            "nested_levels": [0, 2, 3, 4, 5, 6]
        },{
            "from": "10. Deuda Externa Bruta del BCU por instrumento.",
            "to": "11. Deuda Externa Bruta del BCU por plazo y moneda. ", 
            "sheet_name": "DEB BCU", 
            "prefix": "Por Instrumento",
            "nested_levels": [0, 2, 3]
        },    
        {
            "from": "11. Deuda Externa Bruta del BCU por plazo y moneda. ",
            "to": "Estructura a", 
            "sheet_name": "DEB BCU", 
            "prefix": "Por Plazo y Moneda",
            "nested_levels": [0, 3, 4]
        },                
    ]    
    }


# df_merge = pd.DataFrame()
# for table in tables[347]:
#     to_str = table["to"]
#     from_str = table["from"]
#     sheet_name = table["sheet_name"]
#     prefix = table["prefix"]
#     nested = table["nested_levels"]
#     df = load_data(sheet_name)
#     df = cut_data(df, df.columns[1], from_str, to_str)
# df.transpose()

# In[220]:



for DataFrame in tables:
#for DataFrame in [347]:
    df_merge = pd.DataFrame()
    print(DataFrame)
    for table in tables[DataFrame]:
        to_str = table["to"]
        from_str = table["from"]
        sheet_name = table["sheet_name"]
        prefix = table["prefix"]
        nested = table["nested_levels"]

        df = load_data(sheet_name)
        df = cut_data(df, df.columns[1], from_str, to_str)
        df = nested_levels(df, nested)
        df = fix_date(df)
        df.dropna(axis=0, how='all',inplace=True)
        df.dropna(axis=1, how='all',inplace=True)
        df_merge = df_merge.merge(df, how="outer", left_index=True, right_index=True)

    df_merge["country"] = "Uruguay"
    df_merge.columns = list(df_merge.columns)
    
    alphacast.datasets.dataset(DataFrame).upload_data_from_df(df_merge, 
        deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)



