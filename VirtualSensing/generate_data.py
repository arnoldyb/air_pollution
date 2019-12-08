#!/home/ubuntu/miniconda3/envs/tf/bin/python

import pandas as pd
import numpy as np 
import warnings
import sys
sys.path.append("../HistoricalData/")
from getData import get_data
import glob

warnings.filterwarnings('ignore')

from math import sin, cos, radians, pi

import dask.array as da
import dask.dataframe as dd

def main():
    MIN_LAT = 37.2781261
    MAX_LAT = 38.063446
    MIN_LON = -122.683496
    MAX_LON = -121.814281


    UP_LEFT = (MAX_LAT, MIN_LON)    
    UP_RIGHT = (MAX_LAT, MAX_LON)   
    DOWN_RIGHT = (MIN_LAT, MAX_LON) 
    DOWN_LEFT = (MIN_LAT, MIN_LON)
    
    months  = pd.date_range(start = "2018/09/01", end = "2019/10/02", freq = 'MS') \
        .map(lambda x: x.strftime("%Y/%m/%d")).to_list()
    month_ends = pd.date_range(start = "2018/09/01", end = "2019/11/02", freq = 'M') \
        .map(lambda x: x.strftime("%Y/%m/%d")).to_list()
    
    START_HOUR = '0'
    END_HOUR = '24'

    tot_rows = 0

    for START_DATE, END_DATE in zip(months, month_ends):
        df = get_data(UP_LEFT, UP_RIGHT, DOWN_RIGHT, DOWN_LEFT, 
                      START_DATE, END_DATE, START_HOUR, END_HOUR, 'Monthly')

        df = df[(df.temperature < 120) & 
                (df.temperature > -20) &
                (df.humidity < 100) & 
                (df['2_5um'] < 200)]
        tot_rows += df.shape[0]
        df['wind_direction'] = df['wind_direction'].map(lambda x: x if x is not None else 0)

        # fill in no wind times
        df['wind_speed'].fillna(0, inplace = True)
        df['wind_direction'].replace('', 0, inplace = True)

        # change VRB to 0
        vrb = df[df.wind_direction == "VRB"].index
        df.loc[vrb, 'wind_direction'] = 0
        df.loc[vrb, 'wind_speed'] = 0

        # convert wind to vector components
        df['wind_x'] = df.apply(lambda x: x.wind_speed * cos(radians(int(x.wind_direction))), axis = 1)
        df['wind_y'] = df.apply(lambda x: x.wind_speed * sin(radians(int(x.wind_direction))), axis = 1)

        year,month,day = START_DATE.split("/")
        df.drop(columns=['a_h', 'day', 'daytype', 'device_loc_typ',
                       'hour', 'month', 'timeofday', 'uptime', 'wind_direction', 'wind_speed',
                        '0_3um', '0_5um', '10_0um', '1_0um',  '5_0um', 'agency_name',
                       'aqi',  'call_sign2', 'call_sign3', 'category', 'city',
                       'county', 'created',  'epa_pm25_unit', 
                       'full_aqs_code', 'gust_speed', 'gusts', 'hidden', 'high_reading_flag',
                       'interval', 'intl_aqs_code', 'is_owner','minute', 'parent_id', 
                       'pm10_0', 'pm10_0_atm', 'pm1_0', 'pm1_0_atm',
                       'pm2_5_atm', 'pm2_5_cf_1', 'raw_concentration', 'report_modifier',
                       'rssi', 'sensor_name', 'site_name', 'sys_maint_reqd',
                        'thingspeak_primary_id',
                       'thingspeak_primary_id_read_key', 'thingspeak_secondary_id',
                       'thingspeak_secondary_id_read_key', 'variable_wind_info',
                       'variable_winds', 'wban_number', 'wind_data', 'year', 'zipcode',
                       'zulu_time', 'wkday', 'wind_compass']) \
            .to_parquet(f"{year}{month}_bigger_3.parquet", index = False, compression = 'gzip')
        print(f"{year}{month}_bigger_4.parquet saved")
    print("total rows", tot_rows)
    
main()