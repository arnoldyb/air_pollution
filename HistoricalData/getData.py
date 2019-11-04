#!/usr/bin/env python3

import datetime
from datetime import date, timedelta
from os import path
import pandas as pd
from collections import OrderedDict
from math import floor
import math  

import boto3
import s3fs
from fastparquet import ParquetFile

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

# Helper function for getting months in a given range
def getMonths(start, end):
    start_date = datetime.datetime.strptime(start, "%Y/%m/%d").date()
    end_date = datetime.datetime.strptime(end, "%Y/%m/%d").date()

    mth_list = list(OrderedDict(((start_date + timedelta(_)).strftime(r"%Y%m"), None) for _ in range((end_date - start_date).days)).keys())
        
    return mth_list

# Helper function for loading data into a dataframe
def loadDataframe(files, folder):
            
    s3 = s3fs.S3FileSystem()
    myopen = s3.open
    s3_resource = boto3.resource('s3')
    df_list = []

    df = pd.DataFrame(columns=['0_3um', '0_5um', '1_0um', '2_5um', '5_0um', '10_0um', 'pm1_0','pm10_0', 'created', 'pm1_0_atm', 'pm2_5_atm', 'pm10_0_atm', 'uptime','rssi', 
                           'temperature', 'humidity', 'pm2_5_cf_1', 'device_loc_typ', 'is_owner', 'sensor_id', 'sensor_name', 'parent_id','lat', 'lon',  'thingspeak_primary_id', 
                           'thingspeak_primary_id_read_key', 'thingspeak_secondary_id', 'thingspeak_secondary_id_read_key', 'a_h', 'high_reading_flag', 'hidden',
                           'city', 'county', 'zipcode', 'created_at', 'year', 'month', 'day', 'hour', 'minute', 'wban_number', 'call_sign', 'call_sign2', 'interval', 
                           'call_sign3', 'zulu_time', 'report_modifier', 'wind_data', 'wind_direction', 'wind_speed', 'gusts', 'gust_speed', 'variable_winds', 'variable_wind_info', 
                           'sys_maint_reqd', 'epa_pm25_unit', 'epa_pm25_value', 'raw_concentration', 'aqi', 'category', 'site_name', 'agency_name', 'full_aqs_code', 'intl_aqs_code'])

    for filenm in files:
        try:
            s3_resource.Object('midscapstone-whos-polluting-my-air', '{}/{}.parquet'.format(folder, filenm)).load()
            pf=ParquetFile('midscapstone-whos-polluting-my-air/{}/{}.parquet'.format(folder, filenm), open_with=myopen)
            tmp_df=pf.to_pandas()
            df_list.append(tmp_df)
        except:
            print("Processing {} failed".format(filenm))
            continue

    df = pd.concat(df_list, axis=0, ignore_index=True)
            
    return df

# Main function for getting data
def get_data(UP_LEFT, UP_RIGHT, DOWN_RIGHT, DOWN_LEFT, START_DATE, END_DATE, START_HOUR, END_HOUR, FOLDER='Daily'):

    # Create variables from parameters
    startfile = int(START_DATE.replace('/',''))
    endfile = int(END_DATE.replace('/',''))
    lat_min = DOWN_LEFT[0]
    lat_max = UP_RIGHT[0]
    lon_min = DOWN_LEFT[1]
    lon_max = UP_RIGHT[1]

    try:
        if startfile <= endfile:
            if FOLDER == 'Daily':
                file_list = getDates(START_DATE, END_DATE)
                folder_name = 'CombinedDailyInterpolated'
            else:
                file_list = getMonths(START_DATE, END_DATE)
                folder_name = 'CombinedMonthly'
                
            df = loadDataframe(file_list, folder_name)
            # Filter data for input bounding box
            df = df[(df.lat > lat_min) & (df.lat < lat_max) 
                              & (df.lon > lon_min) & (df.lon < lon_max)]
            
            # Filter data for input  hours
            df = df[(df.hour >= START_HOUR) & (df.hour <= END_HOUR)]
            
            # Filter out data for devices located inside
            df = df[df.device_loc_typ == 'outside']
            df.reset_index(inplace=True, drop=True)
            return df
        else:
            raise CustomError("INPUT ERROR: Start Date is greater than End Date")
    except Exception as e:
        print(e)

        
# Function for getting noaa data nearest to a given lat-lon for a given time
def getNearestNoaaData(LAT, LON, DATETIMEVAL):
    
    # Parse Inputs
    year = DATETIMEVAL[:4]
    mthstr = '{0:0>2}'.format(DATETIMEVAL[5:7])
    daystr = '{0:0>2}'.format(DATETIMEVAL[8:10])
    hrstr = '{0:0>2}'.format(DATETIMEVAL[11:13])
    minstr = '{0:0>2}'.format(DATETIMEVAL[14:16])
    
    filename = 'asos_' + year + mthstr + daystr
    folder='AsosDaily'
    
    # Get File from s3
    try:
        s3 = s3fs.S3FileSystem()
        myopen = s3.open
        s3_resource = boto3.resource('s3')
        s3_resource.Object('midscapstone-whos-polluting-my-air', '{}/{}.parquet'.format(folder, filename)).load()
        pf=ParquetFile('midscapstone-whos-polluting-my-air/{}/{}.parquet'.format(folder, filename), open_with=myopen)
        df=pf.to_pandas()
        df.reset_index(inplace=True, drop=True)
    except Exception as e:
        print("Processing {}/{} failed".format(folder, filename))
        print(e)
        
    
    # Create datetime filter value by rounding the minute to the previous 5-min value
    datetimefilter=5 * floor(int(year + mthstr + daystr + hrstr + minstr)/5)
    
    # Filter dataframe based on the above datetime value and iterate till a match is found
    while True:
        filtered_df = df[df.datetime == datetimefilter]
        if len(filtered_df) > 0:
            break
        else:
            datetimefilter -= 5
    
    # Get row corresponding to nearest lat-lon based on euclidean distance
    lowdist=9999
    lowindex=-1
    for index, row in filtered_df.iterrows():
        dist = math.sqrt((row['lat'] - LAT)**2 + (row['lon'] - LON)**2)
        if dist < lowdist:
            lowdist=dist
            lowindex=index
    
    nearest_df = filtered_df.loc[lowindex]
    return nearest_df
