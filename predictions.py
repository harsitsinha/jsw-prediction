# weather_prediction.py

import tensorflow as tf
from sklearn.model_selection import train_test_split
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.metrics import mean_absolute_percentage_error
from sklearn.ensemble import RandomForestRegressor
# from xgboost import XGBRegressor
# import lightgbm as lgb
# import optuna
import datetime
from sklearn.impute import SimpleImputer
from keras.layers import LSTM, Conv1D, Dense, Flatten, MaxPooling1D, Dropout
from keras.models import Sequential
import json
# import seaborn as sns
import requests
import joblib
import gspread

def make_test_dataset(item):
        print(item)
        test_df_1 = pd.DataFrame(columns=['Date','tempmax_c1', 'tempmin_c1', 'temp_c1', 'humidity_c1', 'precip_c1', 'cloudcover_c1', 'windspeed_c1', 'winddir_c1', 'solarradiation_c1', 'solarenergy_c1', 'sunrise_c1', 'sunset_c1'])
        test_df_2 = pd.DataFrame(columns=['tempmax_c2', 'tempmin_c2', 'temp_c2', 'humidity_c2', 'precip_c2', 'cloudcover_c2', 'windspeed_c2', 'winddir_c2', 'solarradiation_c2', 'solarenergy_c2', 'sunrise_c2', 'sunset_c2'])
        test_df_3 = pd.DataFrame(columns=['tempmax_c3', 'tempmin_c3', 'temp_c3', 'humidity_c3', 'precip_c3', 'cloudcover_c3', 'windspeed_c3', 'winddir_c3', 'solarradiation_c3', 'solarenergy_c3', 'sunrise_c3', 'sunset_c3'])
        test_df_4 = pd.DataFrame(columns=['tempmax_c4', 'tempmin_c4', 'temp_c4', 'humidity_c4', 'precip_c4', 'cloudcover_c4', 'windspeed_c4', 'winddir_c4', 'solarradiation_c4', 'solarenergy_c4', 'sunrise_c4', 'sunset_c4'])

        # cities_dict = {
        #     "chennai": [13.0836939, 80.270186],
        #     "vellore": [12.7948109, 79.0006410968549],
        #     "coimbatore": [11.0018115, 76.9628425],
        #     "madurai": [9.9261153, 78.1140983]
        # }

        count = 0
        for city in item:
            latitude = list(city.values())[0][0]
            longitude = list(city.values())[0][1]
            api = f'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{latitude},{longitude}?key=2V2RU7BMJCKZDGSCZSCEKP7VH&unitGroup=metric'
            api_key = '2V2RU7BMJCKZDGSCZSCEKP7VH'
            # start_date = datetime.date(2023,6,14)
            # api = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{latitude},{longitude}/{start_date}?key={api_key}&unitGroup=us&include=current"

            response = requests.get(api)
            data = response.json()
            for items in data['days']:
                date = items['datetime']
                tempmax = items['tempmax']
                tempmin = items['tempmin']
                temp = items['temp']
                humidity = items['humidity']
                precip = items['precip']
                cloudcover = items['cloudcover']
                windspeed = items['windspeed']
                windir = items['winddir']
                solarradiation = items['solarradiation']
                solarenergy = items['solarenergy']
                sunrise = items['sunrise']
                sunset = items['sunset']
                
                if count == 0:
                    data = {'Date': date, 'tempmax_c1': tempmax, 'tempmin_c1': tempmin, 'temp_c1': temp,
                            'humidity_c1': humidity, 'precip_c1': precip, 'winddir_c1': windir, 'windspeed_c1': windspeed,
                            'cloudcover_c1': cloudcover, 'solarradiation_c1': solarradiation,
                            'solarenergy_c1': solarenergy, 'sunrise_c1': sunrise, 'sunset_c1': sunset}
                    test_df_1 = pd.concat([test_df_1, pd.DataFrame(data, index=[0])], ignore_index=True)
                    
                elif count == 1:
                    data = {'tempmax_c2': tempmax, 'tempmin_c2': tempmin, 'temp_c2': temp,
                            'humidity_c2': humidity, 'precip_c2': precip, 'winddir_c2': windir, 'windspeed_c2': windspeed,
                            'cloudcover_c2': cloudcover, 'solarradiation_c2': solarradiation,
                            'solarenergy_c2': solarenergy, 'sunrise_c2': sunrise, 'sunset_c2': sunset}
                    test_df_2 = pd.concat([test_df_2, pd.DataFrame(data, index=[0])], ignore_index=True)
                elif count == 2:
                    data = {'tempmax_c3': tempmax, 'tempmin_c3': tempmin, 'temp_c3': temp,
                            'humidity_c3': humidity, 'precip_c3': precip, 'winddir_c3': windir, 'windspeed_c3': windspeed,
                            'cloudcover_c3': cloudcover, 'solarradiation_c3': solarradiation,
                            'solarenergy_c3': solarenergy, 'sunrise_c3': sunrise, 'sunset_c3': sunset}
                    test_df_3 = pd.concat([test_df_3, pd.DataFrame(data, index=[0])], ignore_index=True)
                elif count == 3:
                    data = {'tempmax_c4': tempmax, 'tempmin_c4': tempmin, 'temp_c4': temp,
                            'humidity_c4': humidity, 'precip_c4': precip, 'winddir_c4': windir, 'windspeed_c4': windspeed,
                            'cloudcover_c4': cloudcover, 'solarradiation_c4': solarradiation,
                            'solarenergy_c4': solarenergy, 'sunrise_c4': sunrise, 'sunset_c4': sunset}
                    test_df_4 = pd.concat([test_df_4, pd.DataFrame(data, index=[0])], ignore_index=True)

                    
            count+=1

        test_df = pd.concat([test_df_1, test_df_2, test_df_3, test_df_4], axis=1)
        return test_df

def preprocess_test_dataset(test_df_solar, test_df_wind, test_df_demand):
        # Convert date column to datetime type\
        test_df_solar['Date'] = pd.to_datetime(test_df_solar['Date'].str.strip(), format='%Y-%m-%d')

        # Extract relevant features from the date column
        test_df_solar['year'] = test_df_solar['Date'].dt.year
        test_df_solar['month'] = test_df_solar['Date'].dt.month
        test_df_solar['day'] = test_df_solar['Date'].dt.day
        test_df_solar['day_of_week'] = test_df_solar['Date'].dt.dayofweek

        # Drop the original date column
        test_df_solar.drop('Date', axis=1, inplace=True)
        
        # Convert date column to datetime type\
        test_df_wind['Date'] = pd.to_datetime(test_df_wind['Date'].str.strip(), format='%Y-%m-%d')

        # Extract relevant features from the date column
        test_df_wind['year'] = test_df_wind['Date'].dt.year
        test_df_wind['month'] = test_df_wind['Date'].dt.month
        test_df_wind['day'] = test_df_wind['Date'].dt.day
        test_df_wind['day_of_week'] = test_df_wind['Date'].dt.dayofweek

        # Drop the original date column
        test_df_wind.drop('Date', axis=1, inplace=True)

        # Convert date column to datetime type\
        test_df_demand['Date'] = pd.to_datetime(test_df_demand['Date'].str.strip(), format='%Y-%m-%d')

        # Extract relevant features from the date column
        test_df_demand['year'] = test_df_demand['Date'].dt.year
        test_df_demand['month'] = test_df_demand['Date'].dt.month
        test_df_demand['day'] = test_df_demand['Date'].dt.day
        test_df_demand['day_of_week'] = test_df_demand['Date'].dt.dayofweek

        # Drop the original date column
        test_df_demand.drop('Date', axis=1, inplace=True)

        # Convert the "Sunduration" column to timedelta type
        test_df_solar['sunrise_c1'] = pd.to_timedelta(test_df_solar['sunrise_c1'])

        # Extract relevant features from the "Sunduration" column
        test_df_solar['hours_c1_sunrise'] = test_df_solar['sunrise_c1'].dt.components['hours']
        test_df_solar['minutes_c1_sunrise'] = test_df_solar['sunrise_c1'].dt.components['minutes']
        test_df_solar['seconds_c1_sunrise'] = test_df_solar['sunrise_c1'].dt.components['seconds']

        # Drop the original "Sunduration" column
        test_df_solar.drop('sunrise_c1', axis=1, inplace=True)
        # Convert the "Sunduration" column to timedelta type
        test_df_solar['sunset_c1'] = pd.to_timedelta(test_df_solar['sunset_c1'])

        # Extract relevant features from the "Sunduration" column
        test_df_solar['hours_c1_sunset'] = test_df_solar['sunset_c1'].dt.components['hours']
        test_df_solar['minutes_c1_sunset'] = test_df_solar['sunset_c1'].dt.components['minutes']
        test_df_solar['seconds_c1_sunset'] = test_df_solar['sunset_c1'].dt.components['seconds']

        # Drop the original "Sunduration" column
        test_df_solar.drop('sunset_c1', axis=1, inplace=True)
        # Convert the "Sunduration" column to timedelta type
        test_df_solar['sunrise_c2'] = pd.to_timedelta(test_df_solar['sunrise_c2'])

        # Extract relevant features from the "Sunduration" column
        test_df_solar['hours_c2_sunrise'] = test_df_solar['sunrise_c2'].dt.components['hours']
        test_df_solar['minutes_c2_sunrise'] = test_df_solar['sunrise_c2'].dt.components['minutes']
        test_df_solar['seconds_c2_sunrise'] = test_df_solar['sunrise_c2'].dt.components['seconds']

        # Drop the original "Sunduration" column
        test_df_solar.drop('sunrise_c2', axis=1, inplace=True)
        # Convert the "Sunduration" column to timedelta type
        test_df_solar['sunset_c2'] = pd.to_timedelta(test_df_solar['sunset_c2'])

        # Extract relevant features from the "Sunduration" column
        test_df_solar['hours_c2_sunset'] = test_df_solar['sunset_c2'].dt.components['hours']
        test_df_solar['minutes_c2_sunset'] = test_df_solar['sunset_c2'].dt.components['minutes']
        test_df_solar['seconds_c2_sunset'] = test_df_solar['sunset_c2'].dt.components['seconds']

        # Drop the original "Sunduration" column
        test_df_solar.drop('sunset_c2', axis=1, inplace=True)
        # Convert the "Sunduration" column to timedelta type
        test_df_solar['sunrise_c3'] = pd.to_timedelta(test_df_solar['sunrise_c3'])

        # Extract relevant features from the "Sunduration" column
        test_df_solar['hours_c3_sunrise'] = test_df_solar['sunrise_c3'].dt.components['hours']
        test_df_solar['minutes_c3_sunrise'] = test_df_solar['sunrise_c3'].dt.components['minutes']
        test_df_solar['seconds_c3_sunrise'] = test_df_solar['sunrise_c3'].dt.components['seconds']

        # Drop the original "Sunduration" column
        test_df_solar.drop('sunrise_c3', axis=1, inplace=True)
        # Convert the "Sunduration" column to timedelta type
        test_df_solar['sunset_c3'] = pd.to_timedelta(test_df_solar['sunset_c3'])

        # Extract relevant features from the "Sunduration" column
        test_df_solar['hours_c3_sunset'] = test_df_solar['sunset_c3'].dt.components['hours']
        test_df_solar['minutes_c3_sunset'] = test_df_solar['sunset_c3'].dt.components['minutes']
        test_df_solar['seconds_c3_sunset'] = test_df_solar['sunset_c3'].dt.components['seconds']

        # Drop the original "Sunduration" column
        test_df_solar.drop('sunset_c3', axis=1, inplace=True)
        # Convert the "Sunduration" column to timedelta type
        test_df_solar['sunrise_c4'] = pd.to_timedelta(test_df_solar['sunrise_c4'])

        # Extract relevant features from the "Sunduration" column
        test_df_solar['hours_c4_sunrise'] = test_df_solar['sunrise_c4'].dt.components['hours']
        test_df_solar['minutes_c4_sunrise'] = test_df_solar['sunrise_c4'].dt.components['minutes']
        test_df_solar['seconds_c4_sunrise'] = test_df_solar['sunrise_c4'].dt.components['seconds']

        # Drop the original "Sunduration" column
        test_df_solar.drop('sunrise_c4', axis=1, inplace=True)
        # Convert the "Sunduration" column to timedelta type
        test_df_solar['sunset_c4'] = pd.to_timedelta(test_df_solar['sunset_c4'])

        # Extract relevant features from the "Sunduration" column
        test_df_solar['hours_c4_sunset'] = test_df_solar['sunset_c4'].dt.components['hours']
        test_df_solar['minutes_c4_sunset'] = test_df_solar['sunset_c4'].dt.components['minutes']
        test_df_solar['seconds_c4_sunset'] = test_df_solar['sunset_c4'].dt.components['seconds']

        # Drop the original "Sunduration" column
        test_df_solar.drop('sunset_c4', axis=1, inplace=True)

        return test_df_solar, test_df_wind, test_df_demand

def preprocess_test_dataset_2(test_df, test_df_demand):
    test_df['Date'] = pd.to_datetime(test_df['Date'].str.strip(), format='%Y-%m-%d')

    # Extract relevant test_df_demand from the date column
    test_df['year'] = test_df['Date'].dt.year
    test_df['month'] = test_df['Date'].dt.month
    test_df['day'] = test_df['Date'].dt.day
    test_df['day_of_week'] = test_df['Date'].dt.dayofweek

    # Drop the original date column
    test_df.drop('Date', axis=1, inplace=True)

    # Convert the "Sunduration" column to timedelta type
    test_df['sunrise_c1'] = pd.to_timedelta(test_df['sunrise_c1'])

    # Extract relevant test_df_solar from the "Sunduration" column
    test_df['hours_c1_sunrise'] = test_df['sunrise_c1'].dt.components['hours']
    test_df['minutes_c1_sunrise'] = test_df['sunrise_c1'].dt.components['minutes']
    test_df['seconds_c1_sunrise'] = test_df['sunrise_c1'].dt.components['seconds']

    # Drop the original "Sunduration" column
    test_df.drop('sunrise_c1', axis=1, inplace=True)
    # Convert the "Sunduration" column to timedelta type
    test_df['sunset_c1'] = pd.to_timedelta(test_df['sunset_c1'])

    # Extract relevant test_df_solar from the "Sunduration" column
    test_df['hours_c1_sunset'] = test_df['sunset_c1'].dt.components['hours']
    test_df['minutes_c1_sunset'] = test_df['sunset_c1'].dt.components['minutes']
    test_df['seconds_c1_sunset'] = test_df['sunset_c1'].dt.components['seconds']

    # Drop the original "Sunduration" column
    test_df.drop('sunset_c1', axis=1, inplace=True)
    # Convert the "Sunduration" column to timedelta type
    test_df['sunrise_c2'] = pd.to_timedelta(test_df['sunrise_c2'])

    # Extract relevant test_df_solar from the "Sunduration" column
    test_df['hours_c2_sunrise'] = test_df['sunrise_c2'].dt.components['hours']
    test_df['minutes_c2_sunrise'] = test_df['sunrise_c2'].dt.components['minutes']
    test_df['seconds_c2_sunrise'] = test_df['sunrise_c2'].dt.components['seconds']

    # Drop the original "Sunduration" column
    test_df.drop('sunrise_c2', axis=1, inplace=True)
    # Convert the "Sunduration" column to timedelta type
    test_df['sunset_c2'] = pd.to_timedelta(test_df['sunset_c2'])

    # Extract relevant test_df_solar from the "Sunduration" column
    test_df['hours_c2_sunset'] = test_df['sunset_c2'].dt.components['hours']
    test_df['minutes_c2_sunset'] = test_df['sunset_c2'].dt.components['minutes']
    test_df['seconds_c2_sunset'] = test_df['sunset_c2'].dt.components['seconds']

    # Drop the original "Sunduration" column
    test_df.drop('sunset_c2', axis=1, inplace=True)
    # Convert the "Sunduration" column to timedelta type
    test_df['sunrise_c3'] = pd.to_timedelta(test_df['sunrise_c3'])

    # Extract relevant test_df_solar from the "Sunduration" column
    test_df['hours_c3_sunrise'] = test_df['sunrise_c3'].dt.components['hours']
    test_df['minutes_c3_sunrise'] = test_df['sunrise_c3'].dt.components['minutes']
    test_df['seconds_c3_sunrise'] = test_df['sunrise_c3'].dt.components['seconds']

    # Drop the original "Sunduration" column
    test_df.drop('sunrise_c3', axis=1, inplace=True)
    # Convert the "Sunduration" column to timedelta type
    test_df['sunset_c3'] = pd.to_timedelta(test_df['sunset_c3'])

    # Extract relevant test_df_solar from the "Sunduration" column
    test_df['hours_c3_sunset'] = test_df['sunset_c3'].dt.components['hours']
    test_df['minutes_c3_sunset'] = test_df['sunset_c3'].dt.components['minutes']
    test_df['seconds_c3_sunset'] = test_df['sunset_c3'].dt.components['seconds']

    # Drop the original "Sunduration" column
    test_df.drop('sunset_c3', axis=1, inplace=True)
    # Convert the "Sunduration" column to timedelta type
    test_df['sunrise_c4'] = pd.to_timedelta(test_df['sunrise_c4'])

    # Extract relevant test_df_solar from the "Sunduration" column
    test_df['hours_c4_sunrise'] = test_df['sunrise_c4'].dt.components['hours']
    test_df['minutes_c4_sunrise'] = test_df['sunrise_c4'].dt.components['minutes']
    test_df['seconds_c4_sunrise'] = test_df['sunrise_c4'].dt.components['seconds']

    # Drop the original "Sunduration" column
    test_df.drop('sunrise_c4', axis=1, inplace=True)
    # Convert the "Sunduration" column to timedelta type
    test_df['sunset_c4'] = pd.to_timedelta(test_df['sunset_c4'])

    # Extract relevant test_df_solar from the "Sunduration" column
    test_df['hours_c4_sunset'] = test_df['sunset_c4'].dt.components['hours']
    test_df['minutes_c4_sunset'] = test_df['sunset_c4'].dt.components['minutes']
    test_df['seconds_c4_sunset'] = test_df['sunset_c4'].dt.components['seconds']

    # Drop the original "Sunduration" column
    test_df.drop('sunset_c4', axis=1, inplace=True)

    # Convert date column to datetime type\
    test_df_demand['Date'] = pd.to_datetime(test_df_demand['Date'].str.strip(), format='%Y-%m-%d')

    # Extract relevant test_df_demand from the date column
    test_df_demand['year'] = test_df_demand['Date'].dt.year
    test_df_demand['month'] = test_df_demand['Date'].dt.month
    test_df_demand['day'] = test_df_demand['Date'].dt.day
    test_df_demand['day_of_week'] = test_df_demand['Date'].dt.dayofweek

    # Drop the original date column
    test_df_demand.drop('Date', axis=1, inplace=True)
    return test_df, test_df_demand

def authorise():
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
    return sheets

def fetch_weather_predicition():
    sheets = authorise()
    with open('./location.json') as f:
        cities_dict = json.load(f)
    for i in cities_dict:  
        if i in ["BIHAR", "ODISHA", "JHARKHAND", "WEST BENGAL", "SIKKIM"]:
            model = joblib.load(f'./models/{i}_renewable.pkl')
            demand_model = joblib.load(f'./models/{i}_demand_model.pkl')
        elif i in ["ANDHRA PRADESH", "HARYANA", "HIMACHAL PRADESH", "KARNATAKA", "KERALA", "MADHYA PRADESH", "PUNJAB", "RAJASTHAN", "TAMILNADU", "UTTAR PRADESH", "CHHATTISGARH", "GOA", "GUJARAT", "MAHARASHTRA", "TELANGANA", "UTTARAKHAND"]:
            solar_model = joblib.load(f'./models/{i}_solar_model.pkl')
            wind_model = joblib.load(f'./models/{i}_wind_model.pkl')
            demand_model = joblib.load(f'./models/{i}_demand_model.pkl')
        diction = cities_dict[i]
        test_df = make_test_dataset(diction)
        if i in ["BIHAR", "ODISHA", "JHARKHAND", "WEST BENGAL", "SIKKIM"]:
            test_df_new = test_df[[ "Date","humidity_c1", "precip_c1","tempmax_c1","tempmin_c1", "temp_c1","cloudcover_c1", "windspeed_c1", "winddir_c1", "sunrise_c1",  "sunset_c1", "solarradiation_c1", "solarenergy_c1",
                "humidity_c2", "precip_c2","tempmax_c2","tempmin_c2","temp_c2", "windspeed_c2", "winddir_c2","cloudcover_c2", "sunrise_c2", "sunset_c2", "solarradiation_c2", "solarenergy_c2",
                "humidity_c3", "precip_c3","tempmax_c3","tempmin_c3","temp_c3", "windspeed_c3", "winddir_c3","cloudcover_c3", "sunrise_c3", "sunset_c3", "solarradiation_c3", "solarenergy_c3",
                "humidity_c4", "precip_c4","tempmax_c4","tempmin_c4","temp_c4", "windspeed_c4", "winddir_c4","cloudcover_c4", "sunrise_c4", "sunset_c4", "solarradiation_c4", "solarenergy_c4" ]]
             
            test_df_demand = test_df[[ "Date","humidity_c1", "precip_c1", "tempmax_c1", "tempmin_c1", "temp_c1"
                ,"humidity_c2", "precip_c2", "tempmax_c2", "tempmin_c2", "temp_c2"
                ,"humidity_c3", "precip_c3", "tempmax_c3", "tempmin_c3", "temp_c3"
                ,"humidity_c4", "precip_c4", "tempmax_c4", "tempmin_c4", "temp_c4"]]
             
            test_df_new, test_df_demand = preprocess_test_dataset_2(test_df_new, test_df_demand)
            prediction = model.predict(test_df_new)
            prediction_demand = demand_model.predict(test_df_demand)
            # print(prediction, prediction_demand)
            df = pd.DataFrame(columns=['Renewable', 'Demand Met'])
            df['Renewable'] = prediction
            df['Demand Met'] = prediction_demand
            combined_df = pd.concat([df, test_df], axis=1)
        
        elif i in ["ANDHRA PRADESH", "HARYANA", "HIMACHAL PRADESH", "KARNATAKA", "KERALA", "MADHYA PRADESH", "PUNJAB", "RAJASTHAN", "TAMILNADU", "UTTAR PRADESH", "CHHATTISGARH", "GOA", "GUJARAT", "MAHARASHTRA", "TELANGANA", "UTTARAKHAND"]:    
            test_df_solar = test_df[[ "Date","tempmax_c1","tempmin_c1", "temp_c1","cloudcover_c1", "sunrise_c1",  "sunset_c1", "solarradiation_c1", "solarenergy_c1",
            "tempmax_c2","tempmin_c2","temp_c2","cloudcover_c2", "sunrise_c2", "sunset_c2", "solarradiation_c2", "solarenergy_c2",
            "tempmax_c3","tempmin_c3","temp_c3","cloudcover_c3", "sunrise_c3", "sunset_c3", "solarradiation_c3", "solarenergy_c3",
            "tempmax_c4","tempmin_c4","temp_c4","cloudcover_c4", "sunrise_c4", "sunset_c4", "solarradiation_c4", "solarenergy_c4" ]]

            test_df_wind = test_df[[ "Date","humidity_c1", "precip_c1", "tempmax_c1", "tempmin_c1", "temp_c1","cloudcover_c1", "windspeed_c1", "winddir_c1", 
            "humidity_c2", "precip_c2", "tempmax_c2", "tempmin_c2", "temp_c2","cloudcover_c2", "windspeed_c2", "winddir_c2",
            "humidity_c3", "precip_c3", "tempmax_c3", "tempmin_c3", "temp_c3","cloudcover_c3", "windspeed_c3", "winddir_c3",
            "humidity_c4", "precip_c4", "tempmax_c4", "tempmin_c4", "temp_c4","cloudcover_c4", "windspeed_c4", "winddir_c4"]]

            test_df_demand = test_df[[ "Date","humidity_c1", "precip_c1", "tempmax_c1", "tempmin_c1", "temp_c1"
            ,"humidity_c2", "precip_c2", "tempmax_c2", "tempmin_c2", "temp_c2"
            ,"humidity_c3", "precip_c3", "tempmax_c3", "tempmin_c3", "temp_c3"
            ,"humidity_c4", "precip_c4", "tempmax_c4", "tempmin_c4", "temp_c4"]]
            test_df_solar, test_df_wind, test_df_demand = preprocess_test_dataset(test_df_solar, test_df_wind, test_df_demand)
            prediction_solar = solar_model.predict(test_df_solar)
            prediction_wind = wind_model.predict(test_df_wind)
            prediction_demand = demand_model.predict(test_df_demand)
            # print(prediction_solar, prediction_wind, prediction_demand)
            df = pd.DataFrame(columns=['Solar', 'Wind', 'Demand Met'])
            df['Solar'] = prediction_solar
            df['Wind'] = prediction_wind
            df['Demand Met'] = prediction_demand
            combined_df = pd.concat([df, test_df], axis=1)
        # Credentials_1 = {
        #     "type": "service_account",
        #     "project_id": "jswdata-388411",
        #     "private_key_id": "1b395eae16a4a1ace17348be38566cde67d3403f",
        #     "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQCptrmVmM+KWB6V\n7NmDjNj0xY1sYHuKradMxOT5pD9IvMwnTqtGIzQM39oyqycmYNZ+U/NkbNL86Ww6\n5CjSA5UPYjko9mCqLr9kdHmBMHr+ZI/duVFMOfHOETMyzhc5Zl+n+zjL7QiX+/Ol\npC1RRCUW0d0f9/5fjOBJHk9fCMLIyCGozWhXBd1DpZ5RXKquhraH1FRTxsjYKIkK\nJJON+ekreDv3VVI34s9boQWVfKDzKNAk4luhb9cFvtpgF5hx/v82rx7qzm9Ewkd9\n+g3EnrYe+aLttynlRmH8t0K4vBKActePDdiLTeSCgak+IBY410BQnk2gsPtxw88P\n9Whr/Z3dAgMBAAECggEAMrg7KmwqykufiGrqbpgvVqebpmIHSeCv+Q925uyhRRm3\nYZ/vK9zld1uHGFFyN9QFBmgMe1zSiWcxQ0BgurL7X0eZkZZgOTwTUvBER8m3WEOx\ngzAZDdDbZRxa0roo0qy3tbostTU3tkREAqLYMEtPUkyR6zXwPZoahX+bdOlnXR2F\nQAQV+OczAViEjqt9/xvo7mNJVtnyRZGIyHZ6WdDDwDjSPp1QmMwsCE4hxBunEhBm\n9hoPeGRck1dHpWFzTLRdck/FiJxnJ946KJS05kzZqtI9RPfLJ8Y2ZNzmuvSrOOzj\nfWxslYzGgEluJcS7EbUAggMt82VJygxg4LHmoRI/OQKBgQDnMFkDztAw47YgWPrk\nhyolFlrX5TEi4i4K6NyeFeDvAbAOvAyFihyx6TBaxGaNMdxuR/PH+NTFVAUqN44W\nMX/JMSX7+MSSdOc4TI6XccYBfv2fx186GLBiJiAAHbplXb5BnDsm6snzPvXJih9w\ngsiGhWFsG4yZsG2xfPUOozPs1wKBgQC77Wsw8VV8//pKJJyv+m0dbNGPHv6DBDpZ\n0e0iTB0WRzWY6gsklwVkTzMhT7H+WWTxCAZ5sc9ComHksYCV9deeSiqWCjGZoC6v\nN+roLmIGeiIf97hRQkgSC2e2/NWgTjqbGCClhiW6qtSyg/72nLnRC9YsRrNvzTp4\n5bOipy9gawKBgQDTjBEYA+nLosKoDOPfs7Mj5rXPsoBo3DCnePdPjNObwJ6ajQak\nN4IUZj4CAI+aidKb8ykmhhIkUsK7I8TXmAnHTzrju3ocTe66PIuKFujJu6tWxnfj\n0G2uEhbcJFfIo6QRo2UZMmUDOUxtU/9606GsPmasJvVcOO3XKHGRKFO+VwKBgQCQ\nqxVwiOgpoRj5iLPCWQM0ureb+N0u2Mtep9doJrXhl5HwFIPdeBDjhoCy7Rn11rV5\nQ5dQiYwnHMBFgEufpbCGH0wnUtOaExC9PEuuzz4RBGZOu7F9hIvcsED4QizmCjLd\ngDZplhpnV+kDP8+/4yi8f+MxFIA/2fpgZVvjI9a53wKBgQDFVMGPxT/s8kcEZkLf\nQ7Myql33SjJ5bKAzvDjJHFvR3bfqL229Rw9t91Ziu70x1jZ5M2n0EtCYlbnbdobF\nmGI4uwsjHUyvBrnFBci4vmZVnocnmgrteld1Junuh/r+RguSi2iIgBxPlQ/8UnEi\n+dA4w9XCAiNXTRuKLf6bXdzwMg==\n-----END PRIVATE KEY-----\n",
        #     "client_email": "jsw-rldc@jswdata-388411.iam.gserviceaccount.com",
        #     "client_id": "105061062943500212227",
        #     "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        #     "token_uri": "https://oauth2.googleapis.com/token",
        #     "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        #     "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/jsw-rldc%40jswdata-388411.iam.gserviceaccount.com",
        #     "universe_domain": "googleapis.com",
        #     "scopes" : [
        #         'https://www.googleapis.com/auth/spreadsheets',
        #         'https://www.googleapis.com/auth/drive'
        #     ]
        #     }

        # gc_1 = gspread.service_account_from_dict(Credentials_1)
        # sheet_nrldc = gc_1.open_by_key("1iUeUAryedTpo1flpSkhydFRBX0_WrZGjb9rHAh4m7AE")
        # sheet_srldc = gc_1.open_by_key("1xSNTGxTukDaSoCVBd224UyX0FwnC8qSxJmbnzvnE7eo")
        # sheet_wrldc = gc_1.open_by_key("1CjPL4lx807a4eWlaQNBx536qtvziRqGMHx0q6B5qS_s")
        # sheet_erldc = gc_1.open_by_key("1A1wD6tgjXxQrELjlMd00GaO5Qc3o88enpWUtT6YCEH0")
        # sheet = sheet_srldc.worksheet("TAMILNADU")
        diction = {
                "srldc" : ["ANDHRA PRADESH", "TELANGANA", "KARNATAKA", "KERALA", "TAMILNADU"],
                "nrldc" : ["UTTAR PRADESH", "UTTARAKHAND", "RAJASTHAN", "HARYANA", "DELHI", "PUNJAB", "HIMACHAL PRADESH"],
                "erldc" : ["WEST BENGAL", "BIHAR", "JHARKHAND", "ODISHA", "SIKKIM"],
                "wrldc" : ["GUJARAT", "MAHARASHTRA", "MADHYA PRADESH", "CHHATTISGARH", "GOA"]
            }
        if i in diction["srldc"]:
            sheet = sheets[1].worksheet(i)
        elif i in diction["nrldc"]:
            sheet = sheets[0].worksheet(i)
        elif i in diction["erldc"]:
            sheet = sheets[3].worksheet(i)
        elif i in diction["wrldc"]:
            sheet = sheets[2].worksheet(i)

        # for sheet in sheets:
        #     worksheets = sheet.worksheets()
        #     for worksheet in worksheets:
        sheet_headers = sheet.row_values(1)
        # Step 5: Append the row from the DataFrame to the respective columns in the Google Sheet
        row_data = []
        for index,rows in combined_df.iterrows():
            new_row_data = []
            for column in sheet_headers:
                if column in combined_df.columns:
                    new_row_data.append(rows[column])
                else:
                    new_row_data.append("")
            row_data.append(new_row_data)
        row_data.reverse()
        print(sheet)
        range_to_update = f'A2:BF16'
        sheet.update(range_to_update, row_data)

if __name__ == '__main__':
    fetch_weather_predicition()