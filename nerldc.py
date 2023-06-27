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
    with open('temp_nerldc.pdf', 'wb') as file:
        file.write(response.content)

    # Read tables from the PDF file
    tables = tabula.read_pdf('temp_nerldc.pdf', pages='all', multiple_tables=True, stream=True, pandas_options={'header': None})

    # Remove the temporary PDF file
    os.remove('temp_nerldc.pdf')

    return tables

def preprocess_table(date,states,url):
    tables=read_tables_from_url(url)
    date=date.strftime("%d-%m-%Y")
    if tables[1][0][6] == 'ARUNACHAL PRADESH' :
        temp_table = tables[1][6:]
        temp_table.reset_index(drop=True,inplace=True)
        columns_to_delete = [4,5,6,7,8]   # Index positions of columns to delete
        temp_table= temp_table.drop(temp_table.columns[columns_to_delete], axis=1)
        temp_table.columns=['State','Thermal','Hydro','Others','Demand Met','Shortage']
        temp_table[['Hydro','Gas/Diesel/Naptha','Wind','Solar']] = temp_table.Hydro.str.split(expand=True)
        temp_table[['Solar','Wind','Demand Met','Gas/Diesel/Naptha','Others','Shortage']] = temp_table[['Shortage','Demand Met','Wind','Others','Gas/Diesel/Naptha','Solar']]
        temp_table.columns=['State','Thermal','Hydro','Gas/Diesel/Naptha','Wind','Solar','Others','Demand Met','Shortage']
        print(temp_table)
    else :
        temp_table=tables[1][7:]
        temp_table.reset_index(drop=True,inplace=True)
        temp_table[0][0] = 'ARUNACHAL PRADESH'
        temp_table = temp_table.drop(1)
        
    
        #temp_table[[2,3,4,5]]=temp_table[2].str.split(expand=True)
        columns_to_delete = [4,5,6,7,8]   # Index positions of columns to delete
        temp_table= temp_table.drop(temp_table.columns[columns_to_delete], axis=1)
        # print(temp_table)
        #temp_table.columns=['State','Thermal','Hydro','Gas/Diesel/Naptha','Wind','Solar','Others','Demand Met','Shortage']
        temp_table.columns=['State','Thermal','Hydro','Others','Demand Met','Shortage']
        temp_table[['Hydro','Gas/Diesel/Naptha','Wind','Solar']] = temp_table.Hydro.str.split(expand=True)
        temp_table[['Solar','Wind','Demand Met','Gas/Diesel/Naptha','Others','Shortage']] = temp_table[['Shortage','Demand Met','Wind','Others','Gas/Diesel/Naptha','Solar']]
        temp_table.columns=['State','Thermal','Hydro','Gas/Diesel/Naptha','Wind','Solar','Others','Demand Met','Shortage']
    temp_table.drop(temp_table.tail(1).index,inplace=True)
    temp_table.insert(0,'Date',date)
    for index, row in temp_table.iterrows():
        
        state = row['State']
        if state not in states.keys():
            states[state] = pd.DataFrame(columns=temp_table.columns)
        states[state] = pd.concat([states[state], pd.DataFrame([row])], ignore_index=True)
    
    
    return states






