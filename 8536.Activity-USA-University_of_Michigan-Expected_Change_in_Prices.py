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


# In[4]:


post_data = {
    'Tabla 32':[{"table":'32', "year":'1978', "qorm":'M', "order":'asc', "format":'Tab-Delimited (Excel)'},
                ['Expectations Next Year - Down',
                'Expectations Next Year - Same',
                'Expectations Next Year - Up by 1-2%',
                'Expectations Next Year - Up by 3-4%',
                'Expectations Next Year - Up by 5%',
                'Expectations Next Year - Up by 6-9%',
                'Expectations Next Year - Up by 10-14%',
                'Expectations Next Year - Up by 15%+',
                'Expectations Next Year - Up DK how much',
                'Expectations Next Year - DK',
                'Expectations Next Year - Mean',
                'Expectations Next Year - Variance',
                'Expectations Next Year - Standard Deviation',
                'Expectations Next Year - 25th Percentile',
                'Expectations Next Year - Median',
                'Expectations Next Year - 75th Percentile',
                'Expectations Next Year - Interquartile Range (75th-25th)']],
    'Tabla 33':[{"table":'33', "year":'1978', "qorm":'M', "order":'asc', "format":'Tab-Delimited (Excel)'},
               ['Expectations Next 5 Years - Down',
                'Expectations Next 5 Years - Same',
               'Expectations Next 5 Years - Up by 1-2%',
               'Expectations Next 5 Years - Up by 3-4%',
               'Expectations Next 5 Years - Up by 5%',
               'Expectations Next 5 Years - Up by 6-9%',
               'Expectations Next 5 Years - Up by 10-14%',
               'Expectations Next 5 Years - Up by 15%+',
               'Expectations Next 5 Years - Up DK how much',
               'Expectations Next 5 Years - DK',
               'Expectations Next 5 Years - Mean',
               'Expectations Next 5 Years - Variance',
               'Expectations Next 5 Years - Standard deviation',
               'Expectations Next 5 Years - 25th Percentile',
               'Expectations Next 5 Years - Median',
               'Expectations Next 5 Years - 75th Percentile',
               'Expectations Next 5 Years - Interquartile Range (75th-25th)']]
}


# In[5]:


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



# In[8]:


url2 = 'http://www.sca.isr.umich.edu/files/tbcpx1px5.csv'

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
                          'NEXT YEAR':'Expectations Next Year - Median',
                          'NEXT 5 YEARS':'Expectations Next 5 Years - Median'})

df2['Date'] = pd.to_datetime(df2[['Year','Month','Day']])
df2 = df2.drop(['Month','Year','Day'], axis = 1)
df2 = df2.set_index('Date')
df2 = df2.iloc[-2:]



# In[9]:


if dfFinal.index[len(dfFinal)-1] == df2.index[len(df2)-1]:
    print("Dataset nuevo igual al anterior. No se actualizará nada.")
else:
        print("Dataset actualizado.")

dfFinal = pd.concat([dfFinal,df2]).drop_duplicates()
dfFinal = dfFinal[~dfFinal.index.duplicated(keep='last')]

dfFinal['country'] = 'USA'



# In[10]:


# dataset_name = 'Activity  - USA - University of Michigan - Expected Change in Prices'

# ##Para University of Michigan Repo
# dataset = alphacast.datasets.create(dataset_name, 1367, 0)

# alphacast.datasets.dataset(dataset['id']).initialize_columns(dateColumnName = 'Date', 
#             entitiesColumnNames=['country'], dateFormat= '%Y-%m-%d')


# In[11]:


alphacast.datasets.dataset(8536).upload_data_from_df(dfFinal, 
                 deleteMissingFromDB = False, onConflictUpdateDB = True, uploadIndex=True)


# In[ ]:




