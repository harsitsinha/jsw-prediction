# df = pd.DataFrame(columns=['Date', 'State', 'Temp', 'Humidity','Precipitation','WindSpeed','Winddir','CloudCover','Solar','Solar_Energy','Sunrise','Sunset'])
state = {}
for i in data_dict:
    state[i] = pd.DataFrame(columns=['Date', 'State', 'Temp', 'Humidity','Precipitation','WindSpeed','Winddir','CloudCover','Solar','Solar_Energy','Sunrise','Sunset'])
start_date = datetime.date(2020,11, 19)
end_date = datetime.date(2020, 11, 20)
current_date = start_date
while current_date<=end_date:
    for i in data_dict:
        print(i)
        df = pd.DataFrame(columns=['Date', 'State', 'Temp', 'Humidity','Precipitation','WindSpeed','Winddir','CloudCover','Solar','Solar_Energy','Sunrise','Sunset'])
        avg_temp = 0
        avg_humidity = 0
        avg_precip = 0
        avg_windspeed = 0
        avg_winddir = 0
        avg_cloudcover = 0
        avg_solar = 0
        avg_solar_energy = 0
        sunrise = ""
        sunset = ""
        # print(data_dict[i])
        length_avg = len(data_dict[i])
        print(length_avg)
        # df = pd.DataFrame(columns=['Column1', 'Column2', 'Column3'])

        for j in data_dict[i]:
            print(list(j.values())[0])
            latitude = list(j.values())[0][0]
            longitude = list(j.values())[0][1]
            api_url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{latitude},{longitude}/{current_date}?key={api_key}&unitGroup=us&include=current"
            print(api_url)

        #     # Send the API request
            response = requests.get(api_url)
            if response.status_code == 200:
                data = response.json()
                avg_temp+=round(((data['days'][0]['temp'])-32)*5/9,2)
                avg_humidity+=round(data['days'][0]['humidity'],2)
                avg_precip+=round(data['days'][0]['precip'],2)
                avg_windspeed+=round(data['days'][0]['windspeed'],2)
                avg_winddir+=round(data['days'][0]['winddir'],2)
                avg_cloudcover+=round(data['days'][0]['cloudcover'],2)
                avg_solar+=  0 if (data['days'][0]['solarradiation']) == None else (data['days'][0]['solarradiation'])
                avg_solar_energy += 0 if (data['days'][0]['solarenergy']) == None else (data['days'][0]['solarenergy'])
                sunrise = (data['days'][0]['sunrise'])
                sunset = (data['days'][0]['sunset'])
        avg_temp = avg_temp/length_avg
        avg_humidity = avg_humidity/length_avg
        avg_precip = avg_precip/length_avg
        avg_windspeed = avg_windspeed/length_avg
        avg_winddir = avg_winddir/length_avg
        avg_cloudcover = avg_cloudcover/length_avg
        avg_solar = avg_solar/length_avg
        avg_solar_energy = avg_solar_energy/length_avg
        print(avg_temp)
        print(avg_humidity)
        print(avg_precip)
        print(avg_windspeed)
        print(avg_winddir)
        print(avg_cloudcover)
        print(avg_solar)
        print(avg_solar_energy)
        print(sunrise)
        print(sunset)
        state_curr = i
        print(state_curr)
        # df = pd.DataFrame(columns=['Date', 'State', 'Temp', 'Humidity','Precipitation','WindSpeed','Winddir','CloudCover','Solar','Solar_Energy','Sunrise','Sunset'])
        row_data = pd.DataFrame({'Date': [current_date.strftime('%Y-%m-%d')], 'State':[state_curr], 'Temp':[avg_temp], 'Humidity':[avg_humidity], 'Precipitation':[avg_precip], 'WindSpeed':[avg_windspeed], 'Winddir':[avg_winddir], 'CloudCover':[avg_cloudcover], 'Solar':[avg_solar], 'Solar_Energy':[avg_solar_energy], 'Sunrise':[sunrise], 'Sunset':[sunset]})

        # row_data = pd.DataFrame({'Date': current_date, 'State':state_curr, 'Temp':avg_temp, 'Humidity':avg_humidity,'Precipitation':avg_precip,'WindSpeed':avg_precip,'Winddir':avg_winddir,'CloudCover':avg_cloudcover,'Solar':avg_solar,'Solar_Energy':avg_solar_energy,'Sunrise':sunrise,'Sunset':sunset})
        # Append the row data to the DataFrame
        state[i] = pd.concat([state[i],row_data])
        # state[i] = df

    current_date += datetime.timedelta(days=1)