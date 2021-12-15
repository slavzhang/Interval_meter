import pandas as pd
import numpy as np
import datetime
import sqlite3
from pandas.tseries.holiday import USFederalHolidayCalendar as uscal

def data_prep(u_data, hour_diff, w_data, export_name):
    '''
    This function will read in the 15-minute interval data export from UtilityAPI. 
        1. Structure the data into one-hour interval
        2. Create new features
        3. Combine with the one-hour interval weather data
        
    Parameters explanation:
    u_data - dataframe; the data export from Utility API with sunrise & sunset hours added
    hour_diff - integer; the hour different between the property location timezone & UTC
    w_data - weather dataset filename (in csv format) - obtained from OpenWeather API
    export_name - the output file name after the data is processed and to be stored on local drive as a csv file
    
    '''
    # Reading in the dataset that included sunrise & sunset data
    # Reading in the weather dataset acquired from Open Weather API
    df = pd.read_csv(u_data)
    w = pd.read_csv(w_data)
    
    # sunrise & sunset hours timezone conversion
    df['sunrise'] = pd.to_datetime(df['sunrise'])
    df['sunset'] = pd.to_datetime(df['sunset'])
    df['sunrise_pst'] = pd.to_datetime(df['sunrise'] - pd.Timedelta(hours=hour_diff)).dt.time
    df['sunset_pst'] = pd.to_datetime(df['sunset'] - pd.Timedelta(hours=hour_diff)).dt.time

    # getting some attributes from the dates & hour interval
    # including: day of the week, hour of the day, holiday/weekend vs workday, season
    
    df['hour'] = pd.to_datetime(df['interval_start']).dt.hour
    df['date'] = pd.to_datetime(df['interval_start']).dt.date
    df['weekday'] = pd.to_datetime(df['interval_start']).apply(lambda x: x.dayofweek)
    df['timestamp'] = pd.to_datetime(df['date']) + pd.to_timedelta(df['hour'], unit='h')
    
    cal = uscal()
    holidays = cal.holidays(start=pd.to_datetime(df['date']).dt.date.min(), 
                        end=pd.to_datetime(df['date']).dt.date.max())
    df['Holiday'] = df['date'].astype('datetime64').isin(holidays)
    df['workday'] = np.where(df.weekday>4, 0, 1)
    df['workday'] = np.where(df['Holiday']==1, 0, df['workday'])
    
    df['season'] = ''
    df.loc[pd.to_datetime(df['interval_start']).dt.month.isin([9,10,11]), 'season'] = 'Fall'
    df.loc[pd.to_datetime(df['interval_start']).dt.month.isin([12,1,2]), 'season'] = 'Winter'
    df.loc[pd.to_datetime(df['interval_start']).dt.month.isin([3,4,5]), 'season'] = 'Spring'
    df.loc[pd.to_datetime(df['interval_start']).dt.month.isin([6,7,8]), 'season'] = 'Summer'
    
    # Processing weather data & Correcting timestamp (original data in UTC timezone)
    # Selecting useful columns
    wed = w[['dt_iso',  'temp', 'feels_like', 'pressure', 'humidity', 
        'wind_speed', 'wind_deg', 'rain_1h', 'clouds_all', 'weather_main', 'weather_description']]
  
    wed['hour_before_temp'] = wed['temp'].shift(periods = 1)
    wed['timestamp'] = pd.to_datetime(wed.dt_iso.str.split('+').str[0])
    wed['timestamp'] = pd.to_datetime(wed['timestamp'] - pd.Timedelta(hours=hour_diff))
    
    # Drop duplicated entries from the weather data
    weather = wed.drop_duplicates(subset='timestamp', keep='first')
    
    # Merging the 1-hour interval energy usage data with the 1-hour interval weather data from OpenWeather API
    hours_wed = pd.merge(df, weather, on='timestamp', how = 'left')
    
    # Creating a new variable 'sunhour' to demonstrate if the timestamp record is within the time during sunlight presented or not
    # Using sql queries to process 
    conn = sqlite3.connect(':memory:')
    # Write the datafrom into memory space
    hours_wed.to_sql('data', conn, index=False)
    # Create the query
    qry = '''
    select 
    timestamp,interval_kWh, weekday, temp, hour, hour_before_temp, feels_like, workday, season,
       pressure, humidity, sunrise_pst, sunset_pst, 
    CASE
    when time(timestamp) > time(sunrise_pst) and time(timestamp) < time(sunset_pst) THEN "Yes"
    else "No"
    END as Sunlight
    from data

    '''
    # Execute the query and update the dataframe
    hours_weather = pd.read_sql_query(qry, conn)
    
    # Create dummies for categorical variables including: 
    sunlight = pd.get_dummies(hours_weather.Sunlight, prefix = 'sunlight')
    workday = pd.get_dummies(hours_weather.workday, prefix = 'workday')
    season = pd.get_dummies(hours_weather.season, prefix = 'season')
    model_prep = hours_weather.join([sunlight, workday, season])
    
    # Exporting the data to csv file
    model_prep.to_csv('{}'.format(export_name).csv)