
import pandas as pd
from datetime import datetime, timedelta
import gspread
from google.oauth2 import service_account
from google.oauth2.service_account import Credentials
import time
import asyncio
from srldc import preprocess_table as preprocess_table_srldc
from erldc import preprocess_table as preprocess_table_erldc
from nrldc import preprocess_table as preprocess_table_nrldc
from nerldc import preprocess_table as preprocess_table_nerldc
from wrldc import preprocess_table as preprocess_table_wrldc


def authorise():
    Credentials = {
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

    try:
        # Create credentials from the dictionary
        credentials = service_account.Credentials.from_service_account_info(Credentials)

        # Authorize the client
        client = gspread.authorize(credentials)
        return client
    except Exception as e:
        print(e)
        return None


def addtoSheet(states,region):
    Credentials = {
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
    try:
        gc = gspread.service_account_from_dict(Credentials)
        sheet = gc.open_by_key("1xSNTGxTukDaSoCVBd224UyX0FwnC8qSxJmbnzvnE7eo")
        if region == 'SRLDC':
            sheet = gc.open_by_key("1NpCvzZdKxRSxGrVVdegPQWyVchjQ6nBZ5JN930n-Cx0")
        elif region == 'WRLDC':
            sheet = gc.open_by_key("1CjPL4lx807a4eWlaQNBx536qtvziRqGMHx0q6B5qS_s")
        elif region == 'ERLDC':
            sheet = gc.open_by_key("1A1wD6tgjXxQrELjlMd00GaO5Qc3o88enpWUtT6YCEH0")
        elif region == 'NRLDC':
            sheet = gc.open_by_key("1iUeUAryedTpo1flpSkhydFRBX0_WrZGjb9rHAh4m7AE")
        for state in states.keys():
            try:
                worksheet = sheet.worksheet(state)
            except gspread.exceptions.WorksheetNotFound:
                worksheet = sheet.add_worksheet(title=state, rows=2000, cols=20)
            
            # Get existing values in the worksheet
            existing_values = worksheet.get_all_values()
            
            if len(existing_values) == 0:
                # If the worksheet is empty, add the new row as the first row
                worksheet.insert_row(states[state].columns.values.tolist(), 1)
                worksheet.insert_rows(states[state].values.tolist(), 18)
            else:
                # If the worksheet is not empty, add the new row at the top
                worksheet.insert_rows(states[state].values.tolist(), 18)
        # try:
        #     worksheet = sheet.worksheet("TAMILNADU")
        # except gspread.exceptions.WorksheetNotFound:
        #     worksheet = sheet.add_worksheet(title="TAMILNADU", rows=2000, cols=20)
        
        # # Get existing values in the worksheet
        # existing_values = worksheet.get_all_values()
        
        # if len(existing_values) == 0:
        #     # If the worksheet is empty, add the new row as the first row
        #     worksheet.insert_row(states["TAMILNADU"].columns.values.tolist(), 1)
        #     worksheet.insert_rows(states["TAMILNADU"].values.tolist(), 18)
        # else:
        #     # If the worksheet is not empty, add the new row at the top
        #     worksheet.insert_rows(states["TAMILNADU"].values.tolist(), 18)

    except Exception as e:
        print(e)
        return None




def fetch_history():
    authorise()
    start_date = datetime(2020, 5, 19)  # Specify the start date
    end_date = datetime.now().date()-timedelta(days=2)
    end_date = datetime.combine(end_date, datetime.min.time())
    months_srldc = ['Jan','Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep','Oct','Nov','Dec']
    months_wrldc = ['January','February', 'March', 'April', 'May', 'June', 'July', 'August', 'September','October','November','December']
    month_dict_srldc = {index + 1: month for index, month in enumerate(months_srldc)}
    month_dict_wrldc = {index + 1: month for index, month in enumerate(months_wrldc)}

    while start_date<=end_date:
        end_date = datetime.now().date()-timedelta(days=2)
        end_date = datetime.combine(end_date, datetime.min.time())
        curr_date = str(start_date.day) if len(str(start_date.day))==2 else '0'+ str(start_date.day)
        curr_month = str(start_date.month) if len(str(start_date.month))==2 else '0'+ str(start_date.month)
        print(start_date, flush=True)
        states={}
        url_srldc=f'https://srldc.in/var/ftp/reports/psp/{start_date.year}/{month_dict_srldc[start_date.month]}{start_date.year%100}/{start_date.strftime("%d-%m-%Y")}-psp.pdf'
        url_wrldc=f'https://reporting.wrldc.in/dailyreports/PSP/{start_date.year}/{month_dict_wrldc[start_date.month]}/WRLDC_PSP_Report_{start_date.strftime("%d-%m-%Y")}.pdf'
        url_nrldc=f'https://nrldc.in/Websitedata/DoReport/pdf/daily{curr_date}{curr_month}{start_date.year%100}.pdf'
        url_erldc=f'https://app.erldc.in/Content/Upload/Report/PSP/Power%20Supply%20Position%20Report_{start_date.strftime("%d%m%Y")}.pdf'
        url_nerldc = f'https://www.nerldc.in/wp-content/uploads/NER-PSP-REPORT-DATED-{start_date.strftime("%d-%m-%Y")}.pdf'


        try:
            states=preprocess_table_nrldc(start_date,states,url_nrldc)
            addtoSheet(states)
            states={}
        except Exception as e:
            print(f"Data not available for this date:{start_date}:{url_nrldc}", flush=True)


        start_date=start_date+timedelta(days=1)

def fetch_realtime():
    print('hi')
    authorise()
    start_date = datetime.now().date()-timedelta(days=2) # Specify the start date
    curr_date = str(start_date.day) if len(str(start_date.day))==2 else '0'+ str(start_date.day)
    curr_month = str(start_date.month) if len(str(start_date.month))==2 else '0'+ str(start_date.month)
    print(start_date)
    start_date = datetime.combine(start_date, datetime.min.time())
    months_srldc = ['Jan','Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep','Oct','Nov','Dec']
    months_wrldc = ['January','February', 'March', 'April', 'May', 'June', 'July', 'August', 'September','October','November','December']
    month_dict_srldc = {index + 1: month for index, month in enumerate(months_srldc)}
    month_dict_wrldc = {index + 1: month for index, month in enumerate(months_wrldc)}

    states={}
    url_srldc=f'https://srldc.in/var/ftp/reports/psp/{start_date.year}/{month_dict_srldc[start_date.month]}{start_date.year%100}/{start_date.strftime("%d-%m-%Y")}-psp.pdf'
    url_wrldc=f'https://reporting.wrldc.in/dailyreports/PSP/{start_date.year}/{month_dict_wrldc[start_date.month]}/WRLDC_PSP_Report_{start_date.strftime("%d-%m-%Y")}.pdf'
    url_nrldc=f'https://nrldc.in/Websitedata/DoReport/pdf/daily{curr_date}{curr_month}{start_date.year%100}.pdf'
    url_erldc=f'https://app.erldc.in/Content/Upload/Report/PSP/Power%20Supply%20Position%20Report_{start_date.strftime("%d%m%Y")}.pdf'

    retry_attempts = 24
    retry_interval = 3600  # 1 hour

    try:
        for _ in range(retry_attempts):
            # print('hi1')
            states=preprocess_table_srldc(start_date,states,url_srldc)
            # print('hi2')
            addtoSheet(states,"SRLDC")
            # print('hi3')
            states={}
            break
    except Exception as e:
        print(f"Data not available yet on url:{url_srldc}", flush=True)
        print("Retrying in 1 hour...", flush=True)
        time.sleep(retry_interval)
    try:
        for _ in range(retry_attempts):
            states=preprocess_table_wrldc(start_date,states,url_wrldc)
            addtoSheet(states,"WRLDC")
            states={}
            break
    except Exception as e:
        print(f"Data not available yet on url:{url_wrldc}", flush=True)
        print("Retrying in 1 hour...", flush=True)
        time.sleep(retry_interval)
    try:
        for _ in range(retry_attempts):
            states=preprocess_table_nrldc(start_date,states,url_nrldc)
            addtoSheet(states,"NRLDC")
            states={}
            break
    except Exception as e:
        print(f"Data not available yet on url:{url_nrldc}", flush=True)
        print("Retrying in 1 hour...", flush=True)
        time.sleep(retry_interval)
    try:
        for _ in range(retry_attempts):
            states=preprocess_table_erldc(start_date,states,url_erldc)
            addtoSheet(states,"ERLDC")
            states={}
            break
    except Exception as e:
        print(f"Data not available yet on url:{url_erldc}", flush=True)
        print("Retrying in 1 hour...", flush=True)
        time.sleep(retry_interval)
    return 
            

