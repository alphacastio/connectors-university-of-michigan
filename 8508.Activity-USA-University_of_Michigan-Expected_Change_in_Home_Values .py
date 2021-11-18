#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import numpy as np
import requests
import io

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("ALPHACAST_API_KEY")
alphacast = Alphacast(API_KEY)

url = 'https://data.sca.isr.umich.edu/data-archive/mine.php'

post_data = {
    'Tabla 45':[{"table":'45', "year":'1993', "qorm":'M', "order":'asc', "format":'Tab-Delimited (Excel)'},
                {'Past Year - Value Increased',
                 'Past Year - Value Same',
                 'Past Year - Value Decreased',
                 'Past Year - DK',
                 'Past Year - Relative'}],
    'Tabla 46':[{"table":'46', "year":'2007', "qorm":'M', "order":'asc', "format":'Tab-Delimited (Excel)'},
               {'Next Year - Increase',
                'Next Year - Same',
                'Next Year - Decrease',
                'Next Year - DK',
                'Next Year - Median',
                'Next Year - Mean',
                'Next Year - Variance',
                'Next Year - 25th Percentile',
                'Next Year - 75th Percentile',
                'Next Year - Interquartile Rang (75th-25th)'}],
    'Tabla 47':[{"table":'47', "year":'2007', "qorm":'M', "order":'asc', "format":'Tab-Delimited (Excel)'},
               {'Next 5 Years - Increase',
                'Next 5 Years - Same',
                'Next 5 Years - Decrease',
                'Next 5 Years - DK',
                'Next 5 Years - Median',
                'Next 5 Years - Mean',
                'Next 5 Years - Variance',
                'Next 5 Years - 25th Percentile',
                'Next 5 Years - 75th Percentile',
                'Next 5 Years - Interquartile Rang (75th-25th)'}]
}

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

dfFinal['country'] = 'USA'
alphacast.datasets.dataset(8508).upload_data_from_df(dfFinal, 
                 deleteMissingFromDB = False, onConflictUpdateDB = True, uploadIndex=True)




