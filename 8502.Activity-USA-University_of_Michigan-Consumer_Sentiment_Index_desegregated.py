#!/usr/bin/env python
# coding: utf-8

# In[19]:


import pandas as pd
import numpy as np
import requests

from datetime import datetime
import io
import re
import json
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)




# In[21]:


url = 'https://data.sca.isr.umich.edu/data-archive/mine.php'


# In[22]:


post_data = {
    'Tabla 1':[{"table":'1', "year":'1978', "qorm":'M', "order":'asc', "format":'Tab-Delimited (Excel)'},
               {'Index of Consumer Sentiment'}],
    'Tabla 2':[{"table":'2', "year":'1978', "qorm":'M', "order":'asc', "format":'Tab-Delimited (Excel)'},
               {'Lower Third', 
                'Middle Third',
                'Upper Third'}],
    'Tabla 3':[{"table":'3', "year":'1978', "qorm":'M', "order":'asc', "format":'Tab-Delimited (Excel)'},
               {'Age 18-34', 
                'Age 35-54',
                'Age 55+'}],
    'Tabla 4':[{"table":'4', "year":'1978', "qorm":'M', "order":'asc', "format":'Tab-Delimited (Excel)'},
               {'North East', 
                'North Central',
                'South',
                'West'}],
    'Tabla 5':[{"table":'5', "year":'1978', "qorm":'M', "order":'asc', "format":'Tab-Delimited (Excel)'},
               {'Personal Finance Current', 
                'Personal Finance Expected',
                'Business Condition 12 Months',
                'Business Condition 5 Years',
                'Buying Conditions',
                'Current Index',
                'Expected Index'}]
}


# In[23]:


dfFinal = pd.DataFrame([])
for key, value in post_data.items():
    response = requests.post(url, data= value[0])
    df = pd.read_csv(io.BytesIO(response.content), delimiter = '\t', skiprows = 1)
    
    df = df.dropna(how='all', axis=1)
    df['Day'] = '1'
    df['Date'] = pd.to_datetime(df[['Year','Month','Day']])
    df = df.drop(['Month','Year','Day'], axis = 1)
    df = df.set_index('Date')
    df.columns = value[1]
    dfFinal = dfFinal.merge(df, how='outer', left_index=True, right_index=True)

dfFinal


# In[24]:


url2 = 'http://www.sca.isr.umich.edu/files/tbcics.csv'

df2 = pd.read_csv(url2, delimiter = ',', skiprows = 3)
df2 = df2.dropna(how='all').dropna(how='all', axis =1)
df2['DATE OF SURVEY'] = df2['DATE OF SURVEY'].replace({
    'January':'01',
    'February':'02',
    'March':'03',
    'April':'04',
    'May':'05',
    'June':'06',
    'July':'07',
    'August':'08',
    'September':'09',
    'October':'10',
    'November':'11',
    'December':'12'
})

df2['Day'] = '1'
df2 = df2.rename(columns={'Unnamed: 1':'Year',
                          'DATE OF SURVEY':'Month',
                          'INDEX OF CONSUMER SENTIMENT':'Index of Consumer Sentiment'})

df2['Date'] = pd.to_datetime(df2[['Year','Month','Day']])
df2 = df2.drop(['Month','Year','Day'], axis = 1)
df2 = df2.set_index('Date')
df2 = df2.iloc[-2:]
df2


# In[25]:


if dfFinal.index[len(dfFinal)-1] == df2.index[len(df2)-1]:
    print("Dataset nuevo igual al anterior. No se actualizará nada.")
else:
        print("Dataset actualizado.")

dfFinal = pd.concat([dfFinal,df2]).drop_duplicates()
dfFinal = dfFinal[~dfFinal.index.duplicated(keep='last')]

dfFinal['country'] = 'USA'
dfFinal


# In[26]:


# dataset_name = 'Activity - USA - University of Michigan - Consumer Sentiment Index desagregated'

# ##Para University of Michigan Repo
# dataset = alphacast.datasets.create(dataset_name, 1367, 0)

# alphacast.datasets.dataset(dataset['id']).initialize_columns(dateColumnName = 'Date', 
#             entitiesColumnNames=['country'], dateFormat= '%Y-%m-%d')


# In[27]:


alphacast.datasets.dataset(8502).upload_data_from_df(dfFinal, 
                 deleteMissingFromDB = False, onConflictUpdateDB = True, uploadIndex=True)


# In[ ]:




