#!/usr/bin/env python3

import datetime
from datetime import date, timedelta
from os import path
import pandas as pd

import warnings
warnings.filterwarnings('ignore')

# Class for throwing custom errors
class CustomError(Exception):
    def __init__(self, m):
        self.message = m
    def __str__(self):
        return self.message

# Helper function for getting dates in a given range
def getDates(start, end):
    date_list = []
    start_date = datetime.datetime.strptime(start, "%Y/%m/%d").date()
    end_date = datetime.datetime.strptime(end, "%Y/%m/%d").date()

    delta = end_date - start_date       # as timedelta

    for i in range(delta.days + 1):
        day = start_date + timedelta(days=i)
        date_list.append(day.strftime("%Y%m%d"))

    return date_list

# Helper function for loading data into a dataframe
def loadDataframe(files):
    df = pd.DataFrame(columns=['0_3um', '0_5um', '1_0um', '2_5um', '5_0um', '10_0um', 'pm1_0', 'pm10_0', 'created', 'pm1_0_atm', 'pm2_5_atm', 'pm10_0_atm', 'uptime',
                               'rssi', 'temperature', 'humidity', 'pm2_5_cf_1', 'a_h', 'device_loc_typ', 'high_reading_flag', 'hidden', 'sensor_id', 'sensor_name',
                               'lat', 'lon', 'parent_id', 'is_owner', 'city', 'county', 'zipcode', 'created_at', 'year', 'month', 'day', 'hour', 'minute', 'wban_number',
                               'call_sign', 'call_sign2', 'interval', 'call_sign3', 'zulu_time', 'report_modifier', 'wind_data', 'wind_direction', 'wind_speed', 'gusts',
                               'gust_speed', 'variable_winds', 'variable_wind_info', 'sys_maint_reqd'])

    for file in files:
        file_name = "{}.parquet".format(file)
        if path.exists(file_name):
            tmp_df = pd.read_parquet(file_name)
            df = pd.concat([df,tmp_df],ignore_index=True)
        else:
            print("File {} does not exist".format(file_name))

    return df

# Main function for getting data
def get_data(UP_LEFT, UP_RIGHT, DOWN_RIGHT, DOWN_LEFT, START_DATE, END_DATE, START_HOUR, END_HOUR):

    # Create variables from parameters
    startfile = int(START_DATE.replace('/',''))
    endfile = int(END_DATE.replace('/',''))
    lat_min = DOWN_LEFT[0]
    lat_max = UP_RIGHT[0]
    lon_min = DOWN_LEFT[1]
    lon_max = UP_RIGHT[1]

    try:
        if startfile <= endfile:
            file_list = getDates(START_DATE, END_DATE)
            df = loadDataframe(file_list)
            # Filter data for input bounding box
            df = df[(df.lat > lat_min) & (df.lat < lat_max) 
                              & (df.lon > lon_min) & (df.lon < lon_max)]
            
            # Filter data for input  hours
            df = df[(df.hour >= START_HOUR) & (df.hour <= END_HOUR)]
            df.reset_index(inplace=True, drop=True)
            return df
        else:
            raise CustomError("INPUT ERROR: Start Date is greater than End Date")
    except Exception as e:
        print(e)
