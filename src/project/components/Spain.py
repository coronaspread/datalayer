#!/usr/bin/env python
# coding: utf-8

# ## España

# In[1]:


import pandas as pd
import numpy as np
url_esp = "https://covid19.isciii.es/resources/serie_historica_acumulados.csv"


# In[2]:


fmt = "%d/%m/%Y"
#esp_covid = pd.read_csv(url_esp, skipfooter=1)
esp_covid = pd.read_csv(url_esp, encoding = 'cp1252', skipfooter=1)
esp_covid["Fecha"] = pd.to_datetime(esp_covid["Fecha"], format=fmt)


# In[3]:


esp_covid.columns
esp_covid.head(n=5)


# In[5]:


esp_covid.columns

# Get row count of dataframe by finding the length of index labels
numOfRows = len(esp_covid.index)
 
print('Number of Rows in dataframe : ' , numOfRows)


# In[6]:


# format from wide to long

id_vars = ['Fecha', 'CCAA Codigo ISO', 'Casos ']
value_vars = ['Hospitalizados', 'UCI','Fallecidos']
esp_covid_melt = pd.melt(frame=esp_covid, value_vars=value_vars, id_vars=id_vars, var_name="type")
esp_covid_melt.head(n=5)


# In[7]:


# Rename column values to Hospital, Deceased, ICU

esp_covid_melt["type"].replace({"Hospitalizados":"Hospital","Fallecidos":"Deceased", "UCI":"ICU"}, inplace=True)


# In[8]:


esp_covid_melt.head(n=20)


# In[9]:


esp_covid_melt = esp_covid_melt.rename(
    columns = {"Fecha":"date", "CCAA Codigo ISO":"region_code"})


# In[10]:


esp_covid_melt["reporting_date"] = np.nan
esp_covid_melt["region_code_native"] = np.nan
esp_covid_melt["country_iso"] = "esp"
esp_covid_melt["gender"] = np.nan
esp_covid_melt["age"] = np.nan
esp_covid_melt["source"] = "Spain"
esp_covid_melt["latitude"] = np.nan
esp_covid_melt["longitude"] = np.nan
esp_covid_melt["region_large_code"] = np.nan


# In[11]:


esp_prepared = esp_covid_melt[
    ["source","reporting_date","date","country_iso",
     "region_code","region_code_native","region_large_code", "latitude","longitude","gender","age","type","value"]]

esp_prepared.head(n=100)


# In[12]:


# Get row count of dataframe by finding the length of index labels
numOfRows = len(esp_prepared.index)
 
print('Number of Rows in dataframe : ' , numOfRows)


# In[15]:


esp_prepared.to_csv("spain_covid.csv", index=False)

