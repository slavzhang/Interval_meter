import pandas as pd
import request
import json

def sunrise_sunset(df, lat, lon):
    '''
    df - reading in dataframe from a csv file for invertal data
    lat - latitude from Google Map
    lon - longitude from Google Map
    
    '''
    # create new columns for storing the hours
    df['sunrise'] = 0
    df['sunset'] = 0
    # Getting the date from the timestamp
    df['start'] = pd.to_datetime(df['interval_start']).dt.date
    
    def data_sunrise(lat, lon, date):
        url = 'https://api.sunrise-sunset.org/json?lat={}&lng={}&date={}'.format(lat, lon, date)
        info = requests.get(url)
        new = json.loads(info.text)
        return new['results']['sunrise']

    def data_sunset(lat, lon, date):
        url = 'https://api.sunrise-sunset.org/json?lat={}&lng={}&date={}'.format(lat, lon, date)
        info = requests.get(url)
        new = json.loads(info.text)
        return new['results']['sunset']
    
    for i in range(len(df)):
        df['sunrise'][i]= data_sunrise(lat, lon, df['start'][i])
        df['sunset'][i]= data_sunset(lat, lon, df['start'][i])