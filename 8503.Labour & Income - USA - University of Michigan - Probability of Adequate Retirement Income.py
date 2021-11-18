#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests
import pandas as pd
from datetime import date
import io
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)




# In[ ]:


#Defino el periodo de inicio
#Para actualizarlo no se trae todo el historico, sino a partir del año previo
start_year = str(date.today().year - 1)


# In[ ]:


post_data = {'table': '18', 'year': start_year, 'qorm': 'M', 'order': 'asc', 'format': 'Comma-Separated (CSV)'}

csv_file = requests.post('https://data.sca.isr.umich.edu/data-archive/mine.php', data=post_data)


# In[ ]:


#Descarga del csv
df = pd.read_csv(io.BytesIO(csv_file.content), skiprows=1)


# In[ ]:


#Elimino las columnas sin datos
df.dropna(axis=1, how='all', inplace=True)


# In[ ]:


## Creo la columna de Fecha 
df['Date'] = pd.to_datetime(df['Year'].astype(str)+ '-' + df['Month'].astype(str), format='%Y-%m')
#Eliminamos las columnas de mes y año
df.drop(['Month', 'Year'], axis=1, inplace=True)


# In[ ]:


#Seteamos el indice y renombramos la columna
df.set_index('Date', inplace=True)
df.rename(columns={'DK; NA':'DK'}, inplace=True)

#Elimno filas sin datos
df.dropna(axis=0, how='all', inplace=True)
df['country'] = 'USA'


# In[ ]:


# dataset_name = 'Labour & Income - USA - University of Michigan - Probability of Adequate Retirement Income'

# #Para raw data
# dataset = alphacast.datasets.create(dataset_name, 1367, 0)

# alphacast.datasets.dataset(dataset['id']).initialize_columns(dateColumnName = 'Date', 
#             entitiesColumnNames=['country'], dateFormat= '%Y-%m-%d')


# In[ ]:


alphacast.datasets.dataset(8503).upload_data_from_df(df, 
                 deleteMissingFromDB = False, onConflictUpdateDB = True, uploadIndex=True)


# In[ ]:




