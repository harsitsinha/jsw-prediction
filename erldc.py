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
    with open('temp_erldc.pdf', 'wb') as file:
        file.write(response.content)

    # Read tables from the PDF file
    tables = tabula.read_pdf('temp_erldc.pdf', pages='all', multiple_tables=True, stream=True, pandas_options={'header': None})

    # Remove the temporary PDF file
    os.remove('temp_erldc.pdf')

    return tables

def preprocess_table(date,states,url):
    tables=read_tables_from_url(url)
    date=date.strftime("%d-%m-%Y")
    table_ = tables[1]
    size = table_.shape[1]
    column_name = []
    for i in range(size):
        # print(i)
        column_name.append(str(i))
    table_.columns = column_name
    location = (table_['0']).index[table_['0'] == 'BIHAR'][0]
    temp_table = table_[location:]
    index_Rail = temp_table[temp_table['0'] == 'Railways_ER ISTS'].index.to_list()
    if len(index_Rail)!=0:
        print('Railways_ER ISTS')
        temp_table = temp_table.drop(index_Rail[0], axis=0)
        temp_table.reset_index(drop=True,inplace=True)
    DVC = temp_table[temp_table['0'] == 'DVC'].index.to_list()
    if len(DVC)!=0:
        print('DVC')
        temp_table = temp_table.drop(DVC[0], axis=0)
        temp_table.reset_index(drop=True,inplace=True)
    temp_table[['Hydro', 'Gas/Diesel/Naptha', 'Renewable','Others','Total']] = temp_table.iloc[:,2].str.split(' ',n= 5, expand=True)
    Region = temp_table[temp_table['0'] == 'Region'].index.to_list()
    if len(Region)!=0:
        print('REGION')
        temp_table = temp_table.drop(Region[0], axis=0)
        temp_table.reset_index(drop=True,inplace=True)
    temp_table[['Hydro', 'Gas/Diesel/Naptha', 'Renewable','Others','Total']] = temp_table.iloc[:,2].str.split(' ', n=5, expand=True)
    # temp_table[['Demand Met', 'Shortage','Consumption']] = temp_table.iloc[:,6].str.split(' ',3, expand=True)
    temp_table.rename(columns={'1': 'Thermal'}, inplace=True)
    temp_table.rename(columns={'0': 'State'}, inplace=True)
    temp_table.rename(columns={'8': 'Demand Met'}, inplace=True)
    temp_table.rename(columns={'9': 'Shortage'}, inplace=True)
    print(temp_table)
    temp_table['Shortage (+ve)'] = -1*temp_table['Shortage']
    columns_to_convert = ['Thermal','Hydro','Gas/Diesel/Naptha','Renewable','Others','Demand Met','Shortage','Shortage (+ve)']
    temp_table[columns_to_convert] = temp_table[columns_to_convert].apply(pd.to_numeric, errors='coerce').fillna(0.0)
    temp_table[columns_to_convert] = temp_table[columns_to_convert].apply(lambda x: round(x.astype(float) * (1000/24), 2))
    temp_table = temp_table[['State','Thermal','Hydro','Gas/Diesel/Naptha','Renewable','Others','Demand Met','Shortage','Shortage (+ve)']]
    temp_table.insert(0,'Date',date)
    for index, row in temp_table.iterrows():
        state = row['State']
        if state not in states.keys():
            states[state] = pd.DataFrame(columns=temp_table.columns)
        states[state] = pd.concat([states[state], pd.DataFrame([row])], ignore_index=True)


    return states

