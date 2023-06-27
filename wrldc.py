import requests
import os
import tabula
import pandas as pd
from datetime import datetime, timedelta
import gspread
from google.oauth2 import service_account
from google.oauth2.service_account import Credentials

def read_tables_from_url(url):
    print('h1')
    response = requests.get(url)
    with open('temp_wrldc.pdf', 'wb') as file:
        file.write(response.content)
    print('h1') 
    # Read tables from the PDF file
    tables = tabula.read_pdf('temp_wrldc.pdf', pages='all', multiple_tables=True, stream=True, pandas_options={'header': None})

    # Remove the temporary PDF file
    os.remove('temp_wrldc.pdf')

    return tables

def preprocess_table(date,states,url):
    print('h1')
    tables=read_tables_from_url(url)
    date=date.strftime("%d-%m-%Y")

    # SOLVNG THE ISSUE OF DIFFERENT SPELLINGS OF CHHATTISGARH
    column_data = tables[1].iloc[:, 0].tolist()
    start_index = next((i for i, value in enumerate(column_data) if value in ["CHHATTISGARH", "CHHATISGARH"]), None)
    if start_index is not None:
        temp_table = tables[1].iloc[start_index:]
        temp_table = temp_table.reset_index(drop=True)
        temp_table[0][0] = "CHHATTISGARH"
    
    # SOLVING THE ISSUE OF DIFFERENT SPLITTNG OF MADHYA PRADESH IN DIFFERENT ROWS

    index = temp_table.index[temp_table[0] == "MADHYA"].values[0]
    temp_table.loc[index] = temp_table.loc[index+1]
    temp_table.drop([index+1,index+2],axis=0,inplace=True)
    temp_table = temp_table.reset_index(drop=True)
    temp_table[0][index]="MADHYA PRADESH"

    # EXPANDING THE COLUMNS READ AS SINGLE COLUMN

    temp_table.drop(columns=[3,4,5],axis=1,inplace=True)
    temp_table=temp_table.T.reset_index(drop=True).T
    temp_table[[2,3,4,5]]=temp_table[2].str.split(expand=True)

    # SOLVING PROBLEM OF DADAR AND NAGAR HAVELI AND DAMAN AND DIU COMBINED AS ONE IN RECENT YEARS
    print('h2')
    if "DNHDDPDCL" not in temp_table.iloc[:, 0].values:
        temp_table.loc[1]=temp_table.loc[2]
        temp_table.drop([2,3],axis=0,inplace=True)
        temp_table = temp_table.reset_index(drop=True)
        for i in range(1,temp_table.shape[1]):
            if temp_table[i][1] == "-" or temp_table[i][2] == '-':
                temp_table[i][1]='-'
            else:
                temp_table[i][1]=float(temp_table[i][1])+float(temp_table[i][2])
        temp_table.drop(2,axis=0,inplace=True)
        temp_table=temp_table.reset_index(drop=True)
        temp_table[0][1]="DNHDDPDCL"

    # REMOVING UNNECESSARY ROWS
    index=[]
    index = [x for x, value in enumerate(temp_table.iloc[:, 0].tolist()) if value in ["AMNSIL", "ESIL", "Region"]]
    for i in index:
        temp_table.drop(i,axis=0,inplace=True)
    temp_table.reset_index(drop=True,inplace=True)
    temp_table.columns = ['State','Thermal', 'Hydro', 'Wind', 'Solar', 'Others', 'Shortage', 'Consumption']
    columns_to_convert = [ 'Thermal','Hydro', 'Wind', 'Solar', 'Others', 'Shortage', 'Consumption']##

    temp_table[columns_to_convert] = temp_table[columns_to_convert].apply(pd.to_numeric, errors='coerce').fillna(0.0)
    temp_table[columns_to_convert] = temp_table[columns_to_convert].apply(lambda x: round(x.astype(float) * (1000/24), 2))

    print('hello')
    # print(temp_table)
    print(temp_table)
    temp_table['Demand Met'] = temp_table['Consumption'] - temp_table['Shortage']
    print(temp_table)
    temp_table['Gas/Diesel/Naptha'] = 0.0
    temp_table=temp_table[['State','Thermal','Hydro','Gas/Diesel/Naptha','Wind','Solar','Others','Demand Met','Shortage']]
    # temp_table = temp_table[['State','Thermal','Hydro','Wind','Solar','Others','Shortage']]
    print(temp_table)
    temp_table.insert(0,'Date',date)
    for index, row in temp_table.iterrows():
        state = row['State']
        if state not in states.keys():
            states[state] = pd.DataFrame(columns=temp_table.columns)
        states[state] = pd.concat([states[state], pd.DataFrame([row])], ignore_index=True)
    
    
    return states



