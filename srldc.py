import requests
import os
import tabula
import pandas as pd
from datetime import datetime, timedelta
import gspread
from google.oauth2 import service_account
from google.oauth2.service_account import Credentials

def read_tables_from_url(url):

    response = requests.get(url)
    with open('temp_srldc.pdf', 'wb') as file:
        file.write(response.content)
    print('type(tables)')
    # Read tables from the PDF file
    tables = tabula.read_pdf('temp_srldc.pdf', pages='all', multiple_tables=True, stream=True, pandas_options={'header': None})
    print(type(tables))
    # Remove the temporary PDF file
    os.remove('temp_srldc.pdf')

    return tables

def preprocess_table(date,states,url):

    tables=read_tables_from_url(url)
    print(type(tables))
    date=date.strftime("%d-%m-%Y")

    temp_table=tables[1][5:]
    temp_table.reset_index(drop=True,inplace=True)
    t=temp_table[0][0].split('PRADESH')
    temp_table[8][0]=t[0]
    temp_table[0][0]='ANDHRA PRADESH'
    temp_table[[2,3,4,5,6]]=temp_table[2].str.split(expand=True)
    index=temp_table.index[temp_table[0]=='Region'].values[0]
    temp_table.drop(index,inplace=True)
    temp_table.reset_index(drop=True,inplace=True)
    temp_table.columns=['State','Thermal','Hydro','Gas/Diesel/Naptha','Wind','Solar','Others','Demand Met','Shortage']

    columns_to_convert = ['Thermal', 'Hydro', 'Gas/Diesel/Naptha', 'Wind', 'Solar', 'Others', 'Demand Met', 'Shortage']##
    temp_table[columns_to_convert] = temp_table[columns_to_convert].apply(lambda x: round(x.astype(float) * (1000/24), 2))##
    temp_table.insert(0,'Date',date)
    for index, row in temp_table.iterrows():
        state = row['State']
        if state not in states.keys():
            states[state] = pd.DataFrame(columns=temp_table.columns)
        states[state] = pd.concat([states[state], pd.DataFrame([row])], ignore_index=True)
    
    
    return states


