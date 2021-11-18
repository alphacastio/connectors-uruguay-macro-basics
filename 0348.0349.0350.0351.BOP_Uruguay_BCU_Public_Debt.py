#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd

import numpy as np
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)



# In[ ]:





# In[3]:


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


# In[4]:


def cut_data(df, date_field, cut_row_from, cut_row_to):
    if cut_row_to:
        cut_data = df[df[date_field].str.find(cut_row_to)>= 0].index.min()        
        df = df[:cut_data]    
    cut_data = df[df[date_field].str.find(cut_row_from)>= 0].index.min() + 2
    df = df[cut_data:]
    del df[df.columns[0]]
    return df

def load_data(sheet_name):
    url = "https://www.bcu.gub.uy/Estadisticas-e-Indicadores/Endeudamiento%20Pblico/resdspg.xls"
    return pd.read_excel(url, sheet_name=sheet_name)


# In[29]:


def nested_levels(df, column_list):
    df = df.transpose()
    #df = df[column_list[0]:]
    #column_list = [x - column_list[0] for x in column_list] 
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



# In[73]:


tables = {
    348:  [
        {
            "from": "1. Deuda Bruta - Sector Público Global por deudor.",
            "to": "2. Deuda Bruta - Sector Público Global por acreedor.", 
            "sheet_name": "SPG1", 
            "prefix": "Por Deudor",
            "nested_levels": [0, 2, 3, 4]
        }, 
        {
            "from": "2. Deuda Bruta - Sector Público Global por acreedor.",
            "to": "3. Deuda Bruta - Sector Público Global por instrumento.", 
            "sheet_name": "SPG1", 
            "prefix": "Por Acreedor",
            "nested_levels": [0, 1, 3, 4, 5]
        }
    ],
    349 : [
        {
            "from": "3. Deuda Bruta - Sector Público Global por instrumento.",
            "to": None, 
            "sheet_name": "SPG1", 
            "prefix": "Por Instrumento",
            "nested_levels": [0, 1, 2, 3]
        },
        {
            "from": "4. Deuda Bruta - Sector Público Global por plazo, moneda y residencia.",
            "to": "Estructura a", 
            "sheet_name": "SPG2", 
            "prefix": "Por Plazo y Moneda",
            "nested_levels": [0, 3, 4]
        },        
        {
            "from": "5. Deuda Bruta - Sector Público Global por tipo de tasa de interés.",
            "to": "6. Deuda Bruta - Sector Público Global por tipo de garantía.", 
            "sheet_name": "SPG2", 
            "prefix": "Por Tipo de Tasa",
            "nested_levels": [0, 1, 2, 3, 4]
        },
        {
            "from": "6. Deuda Bruta - Sector Público Global por tipo de garantía.",
            "to": None,
            "sheet_name": "SPG2", 
            "prefix": "Por Tipo de Garantia",
            "nested_levels": [0, 1, 2, 3,4]
        },        
    ], 
    350 : [
        {
            "from": "7. Deuda Bruta del Sector Público no monetario por acreedor y residencia.",
            "to": "8. Deuda Bruta del Sector Público no monetario por instrumento.", 
            "sheet_name": "SPNM bruta", 
            "prefix": "Por Acreedor",
            "nested_levels": [0, 1, 3, 4, 5]
        },
        {
            "from": "8. Deuda Bruta del Sector Público no monetario por instrumento.",
            "to": "9. Deuda Bruta del Sector Público no monetario por plazo y  moneda.", 
            "sheet_name": "SPNM bruta", 
            "prefix": "Por Instrumento",
            "nested_levels": [0, 1]
        },
        {
            "from": "9. Deuda Bruta del Sector Público no monetario por plazo y  moneda.",
            "to": None, 
            "sheet_name": "SPNM bruta", 
            "prefix": "Por Plazo y Moneda",
            "nested_levels": [0, 1, 2]
        },                
    ],
    351 : [
        {
            "from": "10. Deuda Bruta BCU por acreedor y residencia.",
            "to": "11. Deuda Bruta BCU por instrumento.", 
            "sheet_name": "BCU bruta", 
            "prefix": "Por Acreedor",
            "nested_levels": [0, 1, 3, 4, 5]
        },{
            "from": "11. Deuda Bruta BCU por instrumento.",
            "to": "12. Deuda Bruta BCU por plazo y moneda.", 
            "sheet_name": "BCU bruta", 
            "prefix": "Por Instrumento",
            "nested_levels": [0, 1, 2, 3]
        },    
        {
            "from": "12. Deuda Bruta BCU por plazo y moneda.",
            "to": None, 
            "sheet_name": "BCU bruta", 
            "prefix": "Por Plazo y Moneda",
            "nested_levels": [0, 2, 3]
        },                
    ]    
    }


# In[74]:

# In[77]:



for DataFrame in tables:
#for DataFrame in [351]:
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



# In[ ]:




