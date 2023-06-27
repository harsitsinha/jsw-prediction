
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
from threading import Thread


def authorise():
    Credentials = {
    "type": "service_account",
    "project_id": "tough-craft-300510",
    "private_key_id": "b703faaa4749719e204f963c3828a11d91bc0335",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCyUoy6hgYREjcL\nzVkKT01jNMneWJJ01ZJP7hi4CzdWtTqfkzBElORykbc+PQuHAkFqz4SypO2Kpdqk\nj16iZOnwiSGjkxA9w26mGDdT9sq+7BMoiutu3Y0nvUP+/r3/7jSov/nu+0QylnWA\nuEmSoFcJYhzaIu1vdPM6BwxMg9QBEC4hEZFfOi/Nm3jW1Q6+mfNx/v4gkE6G8lTQ\n7BWDFG2nnQ2jtEVEckbzJ1lbeBkZj+m7K2NGaatZWkPO4PCvGR42L/mUIlgnsjSo\nF3wzUpC2uwKyP5JKtUeh2dtdmhRmarivtJka6NCi+APnbNTsJTto/0fCZZTc/qrl\nzcolhDELAgMBAAECggEAF0a8NNqqrw7EHafDwBtC3Di3Qu0nzNVV4FYRZvCb3BcS\n3GJ/kewnd6b6lsasGwbZ2CHXTeRGV2s3w3QtY6Atgya7O9lF7PH2HvsZOb4Ej/BL\nveaIPbPwqWfEmOPSlDYZLI/GhYHJ0JqgItmrTNbeFYz3fCfD+2D2ILRr0A8DHNIy\nSecPb5xhlm2LJBM3EotzZk5en1uRzPGzk/1j3qdmS46B0BLMgxWpFxgasZTxU/me\n6kEjlIqMhLVetC/riDRSY8xU3hN7S30pDIC5Rm1mywUk2A03P7ITMwqhBwrZOfjU\nveyy8WST+Q6pvZgLg/nuWeAw9AR2PE3RqALrLHbb1QKBgQDsjVoXOZhsgb44VUxj\nZPe4Ed6E71+CECytTam6qJYxO8epalt4oCEEz2Zsc4pJHNmHVlfclG0ZT9rpztlp\nOGp+RoUJI4oCDKfYaoBoJJ6zUy958NdYEYpc8weA04QFt7gmYiYaLBqV7tJR53dr\n4P3WRvWk8/fziME0vUMxasyoNwKBgQDA+6dG9C9dnUIo3HANDMxxo2CAcT2xJC1J\nYCCWfoXtsBXPhTw7kLInG+wVsHFHcDZlJpgRj3ae9ROTly5GxnvLAsD1qiWXACN3\nhyth1HoKp2dT/ZI0TPgEIZtsQBVca4MxTk55PoMidbP7wlkPU1JMfq/CT9wX9joF\ncrH5LwHrzQKBgQCzgUBQR4CHNp1mmjPxaPkiUU0Oi6dqR2PfzwNxheUTT03gHblz\n+++SeuA6hCL4AFCCBt99n5R6lMKGklUhJ5KIaHMj0Dg1/eyQqaDvZnqXkhSA8GY5\ninX4uuOCuv+AZz3ywqAvVDCIfkZTqNZSotV8+TQHDKunvqr+nnCS23yaWQKBgBEd\nQDR2q0yuCZP+GKHPpMHfL0u0vzfacXm0YK94AQCXQRfqRPEzX9lADKvPvfwL44cM\n6SeFhYuLSHUqTxxPteLHF72xnSvLA1oyTpKaeUhAmFVGg5THzqbvJA1xkXNkxXm1\n7Besh4yiy5dkIOnPBdlq0sN0uZwWZgbdqRLkLFNxAoGAeTc331lOtwGhxuRu2/T2\nZoM8E9aiYihqq6xPUSZicGva7gOYDRqP4t2IQ1qSE7WIMk+ZgFfKSc/PpVRPLQKQ\neE8ZAOguMchvujH36E/mVcQM0wC8NtK8oNAGnTTjRqWp4m26lAjRVB7BMVP30hlO\nE3UdjVcFfWFpdWjWjgRS2fE=\n-----END PRIVATE KEY-----\n",
    "client_email": "jsw-energy300510@tough-craft-300510.iam.gserviceaccount.com",
    "client_id": "101394003624396677676",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/jsw-energy300510%40tough-craft-300510.iam.gserviceaccount.com",
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


def addtoSheet(states):
    Credentials = {
    "type": "service_account",
    "project_id": "tough-craft-300510",
    "private_key_id": "b703faaa4749719e204f963c3828a11d91bc0335",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCyUoy6hgYREjcL\nzVkKT01jNMneWJJ01ZJP7hi4CzdWtTqfkzBElORykbc+PQuHAkFqz4SypO2Kpdqk\nj16iZOnwiSGjkxA9w26mGDdT9sq+7BMoiutu3Y0nvUP+/r3/7jSov/nu+0QylnWA\nuEmSoFcJYhzaIu1vdPM6BwxMg9QBEC4hEZFfOi/Nm3jW1Q6+mfNx/v4gkE6G8lTQ\n7BWDFG2nnQ2jtEVEckbzJ1lbeBkZj+m7K2NGaatZWkPO4PCvGR42L/mUIlgnsjSo\nF3wzUpC2uwKyP5JKtUeh2dtdmhRmarivtJka6NCi+APnbNTsJTto/0fCZZTc/qrl\nzcolhDELAgMBAAECggEAF0a8NNqqrw7EHafDwBtC3Di3Qu0nzNVV4FYRZvCb3BcS\n3GJ/kewnd6b6lsasGwbZ2CHXTeRGV2s3w3QtY6Atgya7O9lF7PH2HvsZOb4Ej/BL\nveaIPbPwqWfEmOPSlDYZLI/GhYHJ0JqgItmrTNbeFYz3fCfD+2D2ILRr0A8DHNIy\nSecPb5xhlm2LJBM3EotzZk5en1uRzPGzk/1j3qdmS46B0BLMgxWpFxgasZTxU/me\n6kEjlIqMhLVetC/riDRSY8xU3hN7S30pDIC5Rm1mywUk2A03P7ITMwqhBwrZOfjU\nveyy8WST+Q6pvZgLg/nuWeAw9AR2PE3RqALrLHbb1QKBgQDsjVoXOZhsgb44VUxj\nZPe4Ed6E71+CECytTam6qJYxO8epalt4oCEEz2Zsc4pJHNmHVlfclG0ZT9rpztlp\nOGp+RoUJI4oCDKfYaoBoJJ6zUy958NdYEYpc8weA04QFt7gmYiYaLBqV7tJR53dr\n4P3WRvWk8/fziME0vUMxasyoNwKBgQDA+6dG9C9dnUIo3HANDMxxo2CAcT2xJC1J\nYCCWfoXtsBXPhTw7kLInG+wVsHFHcDZlJpgRj3ae9ROTly5GxnvLAsD1qiWXACN3\nhyth1HoKp2dT/ZI0TPgEIZtsQBVca4MxTk55PoMidbP7wlkPU1JMfq/CT9wX9joF\ncrH5LwHrzQKBgQCzgUBQR4CHNp1mmjPxaPkiUU0Oi6dqR2PfzwNxheUTT03gHblz\n+++SeuA6hCL4AFCCBt99n5R6lMKGklUhJ5KIaHMj0Dg1/eyQqaDvZnqXkhSA8GY5\ninX4uuOCuv+AZz3ywqAvVDCIfkZTqNZSotV8+TQHDKunvqr+nnCS23yaWQKBgBEd\nQDR2q0yuCZP+GKHPpMHfL0u0vzfacXm0YK94AQCXQRfqRPEzX9lADKvPvfwL44cM\n6SeFhYuLSHUqTxxPteLHF72xnSvLA1oyTpKaeUhAmFVGg5THzqbvJA1xkXNkxXm1\n7Besh4yiy5dkIOnPBdlq0sN0uZwWZgbdqRLkLFNxAoGAeTc331lOtwGhxuRu2/T2\nZoM8E9aiYihqq6xPUSZicGva7gOYDRqP4t2IQ1qSE7WIMk+ZgFfKSc/PpVRPLQKQ\neE8ZAOguMchvujH36E/mVcQM0wC8NtK8oNAGnTTjRqWp4m26lAjRVB7BMVP30hlO\nE3UdjVcFfWFpdWjWjgRS2fE=\n-----END PRIVATE KEY-----\n",
    "client_email": "jsw-energy300510@tough-craft-300510.iam.gserviceaccount.com",
    "client_id": "101394003624396677676",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/jsw-energy300510%40tough-craft-300510.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com",
    "scopes" : [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    }
    try:
        gc = gspread.service_account_from_dict(Credentials)
        sheet = gc.open_by_key("1unq_HzPORuJSHr6k1zPPiHTJvuyj7RbH76I8oSi9SA0")
        for state in states.keys():
            try:
                worksheet = sheet.worksheet(state)
            except gspread.exceptions.WorksheetNotFound:
                worksheet = sheet.add_worksheet(title=state, rows=1000, cols=20)
            
            # Get existing values in the worksheet
            existing_values = worksheet.get_all_values()
            
            if len(existing_values) == 0:
                # If the worksheet is empty, add the new row as the first row
                worksheet.insert_row(states[state].columns.values.tolist(), 1)
                worksheet.insert_rows(states[state].values.tolist(), 2)
                time.sleep(30)
            else:
                # If the worksheet is not empty, add the new row at the top
                worksheet.insert_rows(states[state].values.tolist(), 2)
                time.sleep(50)

    except Exception as e:
        print(e)
        return None


def fetch_history_srldc():

        start_date = datetime(2020, 4, 1)  # Specify the start date
        end_date = datetime.now().date()-timedelta(days=2)
        end_date = datetime.combine(end_date, datetime.min.time())
        print(start_date)
        start_date = datetime.combine(start_date, datetime.min.time())
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
                states=preprocess_table_srldc(start_date,states,url_srldc)
                addtoSheet(states)
                states={}
                start_date +=timedelta(days=1)
            except Exception as e:
                print(e)
                
def fetch_history_wrldc():

        start_date = datetime(2020, 4, 1)  # Specify the start date
        end_date = datetime.now().date()-timedelta(days=2)
        end_date = datetime.combine(end_date, datetime.min.time())
        print(start_date)
        start_date = datetime.combine(start_date, datetime.min.time())
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
                states=preprocess_table_wrldc(start_date,states,url_wrldc)
                addtoSheet(states)
                states={}
                start_date +=timedelta(days=1)
            except Exception as e:
                print(e)
                
def fetch_history_nrldc():

        start_date = datetime(2020, 4, 1)  # Specify the start date
        end_date = datetime.now().date()-timedelta(days=2)
        end_date = datetime.combine(end_date, datetime.min.time())
        print(start_date)
        start_date = datetime.combine(start_date, datetime.min.time())
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
                start_date +=timedelta(days=1)
            except Exception as e:
                print(e)
                
def fetch_history_erldc():

        start_date = datetime(2020, 4, 1)  # Specify the start date
        end_date = datetime.now().date()-timedelta(days=2)
        end_date = datetime.combine(end_date, datetime.min.time())
        print(start_date)
        start_date = datetime.combine(start_date, datetime.min.time())
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
                states=preprocess_table_erldc(start_date,states,url_erldc)
                addtoSheet(states)
                states={}
                start_date +=timedelta(days=1)
            except Exception as e:
                print(e)
                
def fetch_history_nerldc():

        start_date = datetime(2020, 4, 1)  # Specify the start date
        end_date = datetime.now().date()-timedelta(days=2)
        end_date = datetime.combine(end_date, datetime.min.time())
        print(start_date)
        start_date = datetime.combine(start_date, datetime.min.time())
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
                states=preprocess_table_nerldc(start_date,states,url_nerldc)
                addtoSheet(states)
                states={}
                start_date +=timedelta(days=1)
            except Exception as e:
                print(e)
                



def fetch_history():
    authorise()
    

    Thread(target=fetch_history_srldc).start()
    Thread(target=fetch_history_wrldc).start()
    Thread(target=fetch_history_nrldc).start()
    Thread(target=fetch_history_erldc).start()
    Thread(target=fetch_history_nerldc).start()

        

def fetch_realtime():
    authorise()
    start_date = datetime.now().date()-timedelta(days=1)  # Specify the start date
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
    url_nerldc = f'https://www.nerldc.in/wp-content/uploads/NER-PSP-REPORT-DATED-{start_date.strftime("%d-%m-%Y")}.pdf'

    retry_attempts = 24
    retry_interval = 3600  # 1 hour

    try:
        for _ in range(retry_attempts):
            states=preprocess_table_srldc(start_date,states,url_srldc)
            addtoSheet(states)
            states={}
            break
    except Exception as e:
        print(f"Data not available yet on url:{url_srldc}", flush=True)
        print("Retrying in 1 hour...", flush=True)
        time.sleep(retry_interval)
    try:
        for _ in range(retry_attempts):
            states=preprocess_table_wrldc(start_date,states,url_wrldc)
            addtoSheet(states)
            states={}
    except Exception as e:
        print(f"Data not available yet on url:{url_wrldc}", flush=True)
        print("Retrying in 1 hour...", flush=True)
        time.sleep(retry_interval)
    try:
        for _ in range(retry_attempts):
            states=preprocess_table_nrldc(start_date,states,url_nrldc)
            addtoSheet(states)
            states={}
    except Exception as e:
        print(f"Data not available yet on url:{url_nrldc}", flush=True)
        print("Retrying in 1 hour...", flush=True)
        time.sleep(retry_interval)
    try:
        for _ in range(retry_attempts):
            states=preprocess_table_erldc(start_date,states,url_erldc)
            addtoSheet(states)
            states={}
    except Exception as e:
        print(f"Data not available yet on url:{url_erldc}", flush=True)
        print("Retrying in 1 hour...", flush=True)
        time.sleep(retry_interval)
    try:
        for _ in range(retry_attempts):
            states=preprocess_table_nerldc(start_date,states,url_nerldc)
            addtoSheet(states)
    except Exception as e:
        print(f"Data not available yet on url:{url_nerldc}", flush=True)
        print("Retrying in 1 hour...", flush=True)
        time.sleep(retry_interval)
            

