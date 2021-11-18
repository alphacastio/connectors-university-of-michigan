#!/usr/bin/env python
# coding: utf-8

# In[1]:


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




# In[3]:


url = 'https://data.sca.isr.umich.edu/data-archive/mine.php'


# In[18]:


post_data = {
    'Tabla 41':[{"table":'41', "year":'1978', "qorm":'M', "order":'asc', "format":'Tab-Delimited (Excel)'},
                ['Buying Conditions - Good time to Buy',
                 'Buying Conditions - Uncertain / Depends',
                 'Buying Conditions - Bad time to Buy',
                 'Buying Conditions - Relative']],
    'Tabla 42':[{"table":'42', "year":'1978', "qorm":'M', "order":'asc', "format":'Tab-Delimited (Excel)'},
               ['Opinions About Buying Conditions - Good Time - Prices Low',
                'Opinions About Buying Conditions - Good Time - Prices will increase',
                'Opinions About Buying Conditions - Good Time - Interest Rates Low',
                'Opinions About Buying Conditions - Good Time - Rising Interest Rates',
                'Opinions About Buying Conditions - Good Time - Good Investment',
                'Opinions About Buying Conditions - Good Time - Times Good',
                'Opinions About Buying Conditions - Bad Time - Prices are High',
                'Opinions About Buying Conditions - Bad Time - Interest Rates High',
                'Opinions About Buying Conditions - Bad Time - Cant Afford',
                'Opinions About Buying Conditions - Bad Time - Uncertain Future',
                'Opinions About Buying Conditions - Bad Time - Bad Investment',
                'Opinions About Buying Conditions - Relative: prices',
                'Opinions About Buying Conditions - Relative: interest rates',
                'Opinions About Buying Conditions - Relative: time',
                'Opinions About Buying Conditions - Relative: investment']],
    'Tabla 43':[{"table":'43', "year":'1978', "qorm":'M', "order":'asc', "format":'Tab-Delimited (Excel)'},
                ['Selling Conditions - Good time to Sell',
                 'Selling Conditions - Uncertain / Depends',
                 'Selling Conditions - Bad time to Sell',
                 'Selling Conditions - Relative']],
    'Tabla 44':[{"table":'44', "year":'1978', "qorm":'M', "order":'asc', "format":'Tab-Delimited (Excel)'},
                ['Opinions About Selling Conditions - Good Time - Prices are High',
                'Opinions About Selling Conditions - Good Time - Prices Wont Go Up',
                'Opinions About Selling Conditions - Good Time - Interest Rates Low',
                'Opinions About Selling Conditions - Good Time - Rising Interest Rates',
                'Opinions About Selling Conditions - Good Time - Times Good',
                'Opinions About Selling Conditions - Good Time - Appreciation',
                'Opinions About Selling Conditions - Bad Time - Prices are Low',
                'Opinions About Selling Conditions - Bad Time - Interest Rates High',
                'Opinions About Selling Conditions - Bad Time - Cant Afford',
                'Opinions About Selling Conditions - Bad Time - Uncertain Future',
                'Opinions About Selling Conditions - Bad Time - Depreciation',
                'Opinions About Selling Conditions - Relative: prices',
                'Opinions About Selling Conditions - Relative: interest rates',
                'Opinions About Selling Conditions - Relative: times',
                'Opinions About Selling Conditions - Relative: investment']]
}


# In[19]:


dfFinal = pd.DataFrame([])
for key, value in post_data.items():
    response = requests.post(url, data = value[0])
    df = pd.read_csv(io.BytesIO(response.content), delimiter = '\t', skiprows = 1)
    
    df = df.dropna(how='all', axis=1)
    df['Day'] = '1'
    df['Date'] = pd.to_datetime(df[['Year','Month','Day']])
    df = df.drop(['Month','Year','Day'], axis = 1)
    df = df.set_index('Date')
    df.columns = value[1]
    dfFinal = dfFinal.merge(df, how='outer', left_index=True, right_index=True)

dfFinal['country'] = 'USA'
dfFinal


# In[20]:


# dataset_name = 'Activity - USA - University of Michigan - Buying and Selling Conditions for Houses'

# ##Para University of Michigan Repo
# dataset = alphacast.datasets.create(dataset_name, 1367, 0)

# alphacast.datasets.dataset(dataset['id']).initialize_columns(dateColumnName = 'Date', 
#             entitiesColumnNames=['country'], dateFormat= '%Y-%m-%d')


# In[21]:


alphacast.datasets.dataset(8528).upload_data_from_df(dfFinal, 
                 deleteMissingFromDB = False, onConflictUpdateDB = True, uploadIndex=True)


# In[ ]:




