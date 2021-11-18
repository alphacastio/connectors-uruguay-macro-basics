#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd

import numpy as np
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)



# In[2]:


def fix_date(df):
    df = df[df.index.notnull()]
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


# In[3]:


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


# In[4]:


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
        df.loc[df["level_{}_temp".format(y)].notnull(), "concept"] = df["concept"] + " - " + df["level_{}_temp".format(y)].astype("str")
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


# In[5]:


tables = {
    597:[
        {
            "from": "13. Activos del Sector Público Global",
            "to": "14. Deuda neta del Sector Público Global", 
            "sheet_name": "Activos Neta", 
            "prefix": "Activos SPG",
            "nested_levels": [0, 1, 4, 6, 7]
        },
        {
            "from": "15. Deuda neta del Sector Público Global",
            "to": None, 
            "sheet_name": "Activos Neta", 
            "prefix": "Deuda neta SPG",
            "nested_levels": [0, 1, 3]
        }
    ]
    }


# In[6]:


#for DataFrame in tables:
for DataFrame in [597]:
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

    df_merge.columns = list(df_merge.columns)      


# In[8]:


df1 = load_data(7)
df1 = cut_data(df1, df1.columns[1], '14. Deuda neta del Sector Público Global', '15. Deuda neta del Sector Público Global')
nested_1 = [0,3,5]
df1 = nested_levels(df1, nested_1)

#Borro esta variable porque se repite con otro cuadro anterior
del df1['Deuda neta SPG - Total']

# Arreglo la variable duplicada
df1 = df1[df1.index.notnull()]
df1 = df1.T
fix_col = df1.columns.to_series()
fix_col.iloc[-2] = 'I.21'
fix_col.iloc[-1] ='II.21'
df1.columns = fix_col
df1 = df1.T

df1 = fix_date(df1)
df1.dropna(axis=0, how='all',inplace=True)
df1.dropna(axis=1, how='all',inplace=True)


df_merge_2 = df_merge.merge(df1, how="outer", left_index=True, right_index=True)

df_merge_2["country"] = "Uruguay"

alphacast.datasets.dataset(597).upload_data_from_df(df_merge_2, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

