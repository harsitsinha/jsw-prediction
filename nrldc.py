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
    with open('temp_nrldc.pdf', 'wb') as file:
        file.write(response.content)

    # Read tables from the PDF file
    tables = tabula.read_pdf('temp_nrldc.pdf', pages='all', multiple_tables=True, stream=True, pandas_options={'header': None})

    # Remove the temporary PDF file
    os.remove('temp_nrldc.pdf')

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
    location = (table_['0']).index[table_['0'] == 'PUNJAB'][0]
    temp_table = table_[location:]
    index_Rail = temp_table[temp_table['0'] == 'RAILWAYS_NR ISTS'].index.to_list()
    if len(index_Rail)!=0:
        temp_table = temp_table.drop(index_Rail[0], axis=0)
        temp_table.reset_index(drop=True,inplace=True)
    Region = temp_table[temp_table['0'] == 'Region'].index.to_list()
    if len(Region)!=0:
        print('REGION')
        temp_table = temp_table.drop(Region[0], axis=0)
        temp_table.reset_index(drop=True,inplace=True)
    temp_table.reset_index(drop=True,inplace=True)
    print(temp_table)
    if temp_table['0'].isnull().values.any():
        # null_indices = (temp_table[temp_table.isnull().any(axis =1)].index)
        nan_indices = temp_table[temp_table['0'].isna()].index.tolist()
        print(nan_indices)
        for i in nan_indices:
            temp = temp_table.iloc[i-1,0]
            temp1 = temp_table.iloc[i+1,0]
            temp_table.iloc[i+1,:] = temp_table.iloc[i,:]
            temp_table.iloc[i+1,0] = str(temp) + ' '+ temp1
        temp_table = temp_table.dropna() 
        temp_table.reset_index(drop=True,inplace=True)
        temp_table.rename(columns={'0': 'State'}, inplace=True)
        temp_table.rename(columns={'8': 'Demand Met'}, inplace=True)
        temp_table.rename(columns={'9': 'Shortage'}, inplace=True)
        temp_table.rename(columns={'1': 'Thermal'}, inplace=True)
        temp_table.rename(columns={'2': 'Hydro'}, inplace=True)
        temp_table[['Gas/Diesel/Naptha', 'Solar','Wind','Others']] = temp_table.iloc[:,3].str.split(' ', n=4, expand=True)
        temp_table = temp_table[['State', 'Thermal','Hydro','Gas/Diesel/Naptha', 'Wind','Solar','Others','Demand Met','Shortage']]
        columns_to_convert = ['Thermal', 'Hydro', 'Gas/Diesel/Naptha', 'Wind', 'Solar', 'Others', 'Demand Met', 'Shortage']##
        temp_table[columns_to_convert] = temp_table[columns_to_convert].apply(lambda x: round(x.astype(float) * (1000/24), 2))##
        temp_table.columns=['State','Thermal','Hydro','Gas/Diesel/Naptha','Wind','Solar','Others','Demand Met','Shortage']

    

        
    else:
        t = temp_table.iloc[7,0].split('PRADESH')
        temp_table.iloc[7,6] = temp_table.iloc[7,6]+' '+t[0]
        temp_table.iloc[7,0]= 'HIMACHAL PRADESH'
        t = temp_table.iloc[9,0].split('Ladakh(UT)')
        temp_table.iloc[9,6] = temp_table.iloc[9,6]+' '+t[0]
        temp_table.iloc[9,0] = 'J&K(UT) & Ladakh(UT)'
        temp_table= temp_table.dropna()
        temp_table[['Thermal', 'Hydro']] = temp_table.iloc[:,1].str.split(' ', n=1, expand=True)
        temp_table[['Gas/Diesel/Naptha', 'Solar','Wind','Others']] = temp_table.iloc[:,2].str.split(' ',n= 4, expand=True)
        temp_table[['Demand Met', 'Shortage','Consumption']] = temp_table.iloc[:,6].str.split(' ',n=3, expand=True)
        temp_table.rename(columns={'0': 'State'}, inplace=True)
        temp_table = temp_table[['State', 'Thermal','Hydro','Gas/Diesel/Naptha', 'Wind','Solar','Others','Demand Met','Shortage']]
        columns_to_convert = ['Thermal', 'Hydro', 'Gas/Diesel/Naptha', 'Wind', 'Solar', 'Others', 'Demand Met', 'Shortage']##
        temp_table[columns_to_convert] = temp_table[columns_to_convert].apply(lambda x: round(x.astype(float) * (1000/24), 2))##
        temp_table.columns=['State','Thermal','Hydro','Gas/Diesel/Naptha','Wind','Solar','Others','Demand Met','Shortage']

    
    temp_table.insert(0,'Date',date)
    for index, row in temp_table.iterrows():
        state = row['State']
        if state not in states.keys():
            states[state] = pd.DataFrame(columns=temp_table.columns)
        states[state] = pd.concat([states[state], pd.DataFrame([row])], ignore_index=True)


    return states



