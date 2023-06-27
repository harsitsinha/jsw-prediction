import json
import requests
from datetime import datetime,timedelta

import gspread
from google.oauth2 import service_account
import pandas as pd
import time

def getsheets():
    Credentials_1 = {
        "type": "service_account",
        "project_id": "jswdata-388411",
        "private_key_id": "1b395eae16a4a1ace17348be38566cde67d3403f",
        "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQCptrmVmM+KWB6V\n7NmDjNj0xY1sYHuKradMxOT5pD9IvMwnTqtGIzQM39oyqycmYNZ+U/NkbNL86Ww6\n5CjSA5UPYjko9mCqLr9kdHmBMHr+ZI/duVFMOfHOETMyzhc5Zl+n+zjL7QiX+/Ol\npC1RRCUW0d0f9/5fjOBJHk9fCMLIyCGozWhXBd1DpZ5RXKquhraH1FRTxsjYKIkK\nJJON+ekreDv3VVI34s9boQWVfKDzKNAk4luhb9cFvtpgF5hx/v82rx7qzm9Ewkd9\n+g3EnrYe+aLttynlRmH8t0K4vBKActePDdiLTeSCgak+IBY410BQnk2gsPtxw88P\n9Whr/Z3dAgMBAAECggEAMrg7KmwqykufiGrqbpgvVqebpmIHSeCv+Q925uyhRRm3\nYZ/vK9zld1uHGFFyN9QFBmgMe1zSiWcxQ0BgurL7X0eZkZZgOTwTUvBER8m3WEOx\ngzAZDdDbZRxa0roo0qy3tbostTU3tkREAqLYMEtPUkyR6zXwPZoahX+bdOlnXR2F\nQAQV+OczAViEjqt9/xvo7mNJVtnyRZGIyHZ6WdDDwDjSPp1QmMwsCE4hxBunEhBm\n9hoPeGRck1dHpWFzTLRdck/FiJxnJ946KJS05kzZqtI9RPfLJ8Y2ZNzmuvSrOOzj\nfWxslYzGgEluJcS7EbUAggMt82VJygxg4LHmoRI/OQKBgQDnMFkDztAw47YgWPrk\nhyolFlrX5TEi4i4K6NyeFeDvAbAOvAyFihyx6TBaxGaNMdxuR/PH+NTFVAUqN44W\nMX/JMSX7+MSSdOc4TI6XccYBfv2fx186GLBiJiAAHbplXb5BnDsm6snzPvXJih9w\ngsiGhWFsG4yZsG2xfPUOozPs1wKBgQC77Wsw8VV8//pKJJyv+m0dbNGPHv6DBDpZ\n0e0iTB0WRzWY6gsklwVkTzMhT7H+WWTxCAZ5sc9ComHksYCV9deeSiqWCjGZoC6v\nN+roLmIGeiIf97hRQkgSC2e2/NWgTjqbGCClhiW6qtSyg/72nLnRC9YsRrNvzTp4\n5bOipy9gawKBgQDTjBEYA+nLosKoDOPfs7Mj5rXPsoBo3DCnePdPjNObwJ6ajQak\nN4IUZj4CAI+aidKb8ykmhhIkUsK7I8TXmAnHTzrju3ocTe66PIuKFujJu6tWxnfj\n0G2uEhbcJFfIo6QRo2UZMmUDOUxtU/9606GsPmasJvVcOO3XKHGRKFO+VwKBgQCQ\nqxVwiOgpoRj5iLPCWQM0ureb+N0u2Mtep9doJrXhl5HwFIPdeBDjhoCy7Rn11rV5\nQ5dQiYwnHMBFgEufpbCGH0wnUtOaExC9PEuuzz4RBGZOu7F9hIvcsED4QizmCjLd\ngDZplhpnV+kDP8+/4yi8f+MxFIA/2fpgZVvjI9a53wKBgQDFVMGPxT/s8kcEZkLf\nQ7Myql33SjJ5bKAzvDjJHFvR3bfqL229Rw9t91Ziu70x1jZ5M2n0EtCYlbnbdobF\nmGI4uwsjHUyvBrnFBci4vmZVnocnmgrteld1Junuh/r+RguSi2iIgBxPlQ/8UnEi\n+dA4w9XCAiNXTRuKLf6bXdzwMg==\n-----END PRIVATE KEY-----\n",
        "client_email": "jsw-rldc@jswdata-388411.iam.gserviceaccount.com",
        "client_id": "105061062943500212227",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/jsw-rldc%40jswdata-388411.iam.gserviceaccount.com",
        "universe_domain": "googleapis.com",
        "scopes" : [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        }

    gc_1 = gspread.service_account_from_dict(Credentials_1)
    sheet_nrldc = gc_1.open_by_key("1iUeUAryedTpo1flpSkhydFRBX0_WrZGjb9rHAh4m7AE")
    sheet_srldc = gc_1.open_by_key("1NpCvzZdKxRSxGrVVdegPQWyVchjQ6nBZ5JN930n-Cx0")
    sheet_wrldc = gc_1.open_by_key("1CjPL4lx807a4eWlaQNBx536qtvziRqGMHx0q6B5qS_s")
    sheet_erldc = gc_1.open_by_key("1A1wD6tgjXxQrELjlMd00GaO5Qc3o88enpWUtT6YCEH0")
    sheets = [sheet_nrldc, sheet_srldc, sheet_wrldc, sheet_erldc]
    # sheet_srldc = gc_1.open_by_key("1NpCvzZdKxRSxGrVVdegPQWyVchjQ6nBZ5JN930n-Cx0")
    # sheets = [sheet_srldc]
    return sheets


def fetch_realtime_weather():
    with open('location.json') as json_file:
        data_dict = json.load(json_file)
    Credentials_1 = {
        "type": "service_account",
        "project_id": "jswdata-388411",
        "private_key_id": "1b395eae16a4a1ace17348be38566cde67d3403f",
        "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQCptrmVmM+KWB6V\n7NmDjNj0xY1sYHuKradMxOT5pD9IvMwnTqtGIzQM39oyqycmYNZ+U/NkbNL86Ww6\n5CjSA5UPYjko9mCqLr9kdHmBMHr+ZI/duVFMOfHOETMyzhc5Zl+n+zjL7QiX+/Ol\npC1RRCUW0d0f9/5fjOBJHk9fCMLIyCGozWhXBd1DpZ5RXKquhraH1FRTxsjYKIkK\nJJON+ekreDv3VVI34s9boQWVfKDzKNAk4luhb9cFvtpgF5hx/v82rx7qzm9Ewkd9\n+g3EnrYe+aLttynlRmH8t0K4vBKActePDdiLTeSCgak+IBY410BQnk2gsPtxw88P\n9Whr/Z3dAgMBAAECggEAMrg7KmwqykufiGrqbpgvVqebpmIHSeCv+Q925uyhRRm3\nYZ/vK9zld1uHGFFyN9QFBmgMe1zSiWcxQ0BgurL7X0eZkZZgOTwTUvBER8m3WEOx\ngzAZDdDbZRxa0roo0qy3tbostTU3tkREAqLYMEtPUkyR6zXwPZoahX+bdOlnXR2F\nQAQV+OczAViEjqt9/xvo7mNJVtnyRZGIyHZ6WdDDwDjSPp1QmMwsCE4hxBunEhBm\n9hoPeGRck1dHpWFzTLRdck/FiJxnJ946KJS05kzZqtI9RPfLJ8Y2ZNzmuvSrOOzj\nfWxslYzGgEluJcS7EbUAggMt82VJygxg4LHmoRI/OQKBgQDnMFkDztAw47YgWPrk\nhyolFlrX5TEi4i4K6NyeFeDvAbAOvAyFihyx6TBaxGaNMdxuR/PH+NTFVAUqN44W\nMX/JMSX7+MSSdOc4TI6XccYBfv2fx186GLBiJiAAHbplXb5BnDsm6snzPvXJih9w\ngsiGhWFsG4yZsG2xfPUOozPs1wKBgQC77Wsw8VV8//pKJJyv+m0dbNGPHv6DBDpZ\n0e0iTB0WRzWY6gsklwVkTzMhT7H+WWTxCAZ5sc9ComHksYCV9deeSiqWCjGZoC6v\nN+roLmIGeiIf97hRQkgSC2e2/NWgTjqbGCClhiW6qtSyg/72nLnRC9YsRrNvzTp4\n5bOipy9gawKBgQDTjBEYA+nLosKoDOPfs7Mj5rXPsoBo3DCnePdPjNObwJ6ajQak\nN4IUZj4CAI+aidKb8ykmhhIkUsK7I8TXmAnHTzrju3ocTe66PIuKFujJu6tWxnfj\n0G2uEhbcJFfIo6QRo2UZMmUDOUxtU/9606GsPmasJvVcOO3XKHGRKFO+VwKBgQCQ\nqxVwiOgpoRj5iLPCWQM0ureb+N0u2Mtep9doJrXhl5HwFIPdeBDjhoCy7Rn11rV5\nQ5dQiYwnHMBFgEufpbCGH0wnUtOaExC9PEuuzz4RBGZOu7F9hIvcsED4QizmCjLd\ngDZplhpnV+kDP8+/4yi8f+MxFIA/2fpgZVvjI9a53wKBgQDFVMGPxT/s8kcEZkLf\nQ7Myql33SjJ5bKAzvDjJHFvR3bfqL229Rw9t91Ziu70x1jZ5M2n0EtCYlbnbdobF\nmGI4uwsjHUyvBrnFBci4vmZVnocnmgrteld1Junuh/r+RguSi2iIgBxPlQ/8UnEi\n+dA4w9XCAiNXTRuKLf6bXdzwMg==\n-----END PRIVATE KEY-----\n",
        "client_email": "jsw-rldc@jswdata-388411.iam.gserviceaccount.com",
        "client_id": "105061062943500212227",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/jsw-rldc%40jswdata-388411.iam.gserviceaccount.com",
        "universe_domain": "googleapis.com",
        "scopes" : [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        }

    gc_1 = gspread.service_account_from_dict(Credentials_1)
    sheet_nrldc = gc_1.open_by_key("1iUeUAryedTpo1flpSkhydFRBX0_WrZGjb9rHAh4m7AE")
    sheet_srldc = gc_1.open_by_key("1NpCvzZdKxRSxGrVVdegPQWyVchjQ6nBZ5JN930n-Cx0")
    sheet_wrldc = gc_1.open_by_key("1CjPL4lx807a4eWlaQNBx536qtvziRqGMHx0q6B5qS_s")
    sheet_erldc = gc_1.open_by_key("1A1wD6tgjXxQrELjlMd00GaO5Qc3o88enpWUtT6YCEH0")
    sheets = [sheet_nrldc, sheet_srldc, sheet_wrldc, sheet_erldc]
    # sheet_srldc = gc_1.open_by_key("1xSNTGxTukDaSoCVBd224UyX0FwnC8qSxJmbnzvnE7eo")
    # sheets = [sheet_srldc]
    all_states = {}
    rldc = {}
    for items in sheets:
        for sheet in items.worksheets():
            df = pd.DataFrame(sheet.get_all_records())
            all_states.update({sheet.title: df})
            if items not in rldc:
                rldc.update({items: [sheet.title]})
            else:
                rldc[items].append(sheet.title)

    print(data_dict)
    api_key = "2V2RU7BMJCKZDGSCZSCEKP7VH"
    date = datetime.now().date()-timedelta(days=2)
    sunrise = ""
    sunset = ""
    for i in data_dict:
        
        print(i)
        count = 0
        for j in data_dict[i]:
            # print(j)
            latitude = list(j.values())[0][0]
            longitude = list(j.values())[0][1]
            api_url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{latitude},{longitude}/{date.strftime('%Y-%m-%d')}?key={api_key}&unitGroup=us&include=current"
            try:
                response = requests.get(api_url)
            except Exception as e:
                print(e)
                break
            if response.status_code == 200:
                data = response.json()
                temp_max = round(((data['days'][0]['tempmax'])-32)*5/9,2)
                temp_min = round(((data['days'][0]['tempmin'])-32)*5/9,2)
                temp=round(((data['days'][0]['temp'])-32)*5/9,2)
                humidity=round(data['days'][0]['humidity'],2)
                precip=round(data['days'][0]['precip'],2)
                windspeed=round(data['days'][0]['windspeed'],2)
                winddir=round(data['days'][0]['winddir'],2)
                cloudcover=round(data['days'][0]['cloudcover'],2)
                solar =  0 if (data['days'][0]['solarradiation']) == None else (data['days'][0]['solarradiation'])
                solar_energy = 0 if (data['days'][0]['solarenergy']) == None else (data['days'][0]['solarenergy'])
                sunrise = (data['days'][0]['sunrise'])
                sunset = (data['days'][0]['sunset'])
                temp = round(temp,2)
                humidity = round(humidity,2)
                precip = round(precip,2)
                windspeed = round(windspeed,2)
                winddir = round(winddir,2)
                cloudcover = round(cloudcover,2)
                solar = round(solar,2)
                solar_energy = round(solar_energy,2)
                state_curr = i
                my_list = [temp_max,temp_min,temp,humidity,precip,windspeed,winddir,cloudcover,solar,solar_energy,sunrise,sunset]
                # print(my_list)
                # items = sheet_srldc
                for items in sheets:
                    if i in rldc[items]:
                        worksheet = items.worksheet(i) 
                        # print(i)
                        # print(worksheet.title)
                        column_values = 1
                        column_index = 1
                        column_values = worksheet.col_values(column_index)
                        search_value = date.strftime('%d-%m-%Y')
                        # print(search_value)
                        matching_indices = [index + 1 for index, value in enumerate(column_values) if value == search_value]
                        # print(matching_indices)

                        # print(count)
                        if count==0 : start_column_index = 'K'
                        if count==1 : start_column_index = 'W'
                        if count==2 : start_column_index = 'AI'
                        if count==3 : start_column_index = 'AU'


                        start_row_index = '18'
                        column_values = worksheet.col_values(column_index)
                        worksheet.update(f'{start_column_index }{start_row_index}', [my_list])
                        print(my_list)
                        count+=1
    # i = 'TAMILNADU'
    # count = 0
    # for j in data_dict[i]:
    #     print(j)
    #     latitude = list(j.values())[0][0]
    #     longitude = list(j.values())[0][1]
    #     api_url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{latitude},{longitude}/{date.strftime('%Y-%m-%d')}?key={api_key}&unitGroup=us&include=current"
    #     try:
    #         response = requests.get(api_url)
    #     except Exception as e:
    #         print(e)
    #         break
    #     if response.status_code == 200:
    #         data = response.json()
    #         temp_max = round(((data['days'][0]['tempmax'])-32)*5/9,2)
    #         temp_min = round(((data['days'][0]['tempmin'])-32)*5/9,2)
    #         temp=round(((data['days'][0]['temp'])-32)*5/9,2)
    #         humidity=round(data['days'][0]['humidity'],2)
    #         precip=round(data['days'][0]['precip'],2)
    #         windspeed=round(data['days'][0]['windspeed'],2)
    #         winddir=round(data['days'][0]['winddir'],2)
    #         cloudcover=round(data['days'][0]['cloudcover'],2)
    #         solar =  0 if (data['days'][0]['solarradiation']) == None else (data['days'][0]['solarradiation'])
    #         solar_energy = 0 if (data['days'][0]['solarenergy']) == None else (data['days'][0]['solarenergy'])
    #         sunrise = (data['days'][0]['sunrise'])
    #         sunset = (data['days'][0]['sunset'])
    #         temp = round(temp,2)
    #         humidity = round(humidity,2)
    #         precip = round(precip,2)
    #         windspeed = round(windspeed,2)
    #         winddir = round(winddir,2)
    #         cloudcover = round(cloudcover,2)
    #         solar = round(solar,2)
    #         solar_energy = round(solar_energy,2)
    #         state_curr = i
    #         my_list = [temp_max,temp_min,temp,humidity,precip,windspeed,winddir,cloudcover,solar,solar_energy,sunrise,sunset]
    #         # print(my_list)
    #         # items = sheet_srldc
    #         # for items in sheets:
    #         #     if i in rldc[items]:
    #         worksheet = sheet_srldc.worksheet(i) 
    #         # print(i)
    #         # print(worksheet.title)
    #         column_values = 1
    #         column_index = 1
    #         column_values = worksheet.col_values(column_index)
    #         search_value = date.strftime('%d-%m-%Y')
    #         # print(search_value)
    #         matching_indices = [index + 1 for index, value in enumerate(column_values) if value == search_value]
    #         # print(matching_indices)

    #         # print(count)
    #         if count==0 : start_column_index = 'K'
    #         if count==1 : start_column_index = 'W'
    #         if count==2 : start_column_index = 'AI'
    #         if count==3 : start_column_index = 'AU'


    #         start_row_index = '18'
    #         column_values = worksheet.col_values(column_index)
    #         worksheet.update(f'{start_column_index }{start_row_index}', [my_list])
    #         count+=1

if __name__ == '__main__':
    fetch_realtime_weather()