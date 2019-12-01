#!/usr/bin/env python3

import pandas as pd
import numpy as np
import json
import datetime
from datetime import date, timedelta
import re
import ast
from fastparquet import ParquetFile, write
import boto3
import s3fs
from geopy.distance import distance
import geopy

import warnings
warnings.filterwarnings('ignore')

# Class for throwing custom errors
class CustomError(Exception):
    def __init__(self, m):
        self.message = m
    def __str__(self):
        return self.message

def createHashKey(row):
    """Helper function to create a hash key based on lat and lon"""
    if np.isnan(row['lat']):
        str_lat = ''
    else:
        str_lat = str(row['lat'])

    if np.isnan(row['lon']):
        str_lon = ''
    else:
        str_lon = str(row['lon'])

    return hash(str_lat + str_lon)

def timeOfDay(hour):
    """Helper function to convert hour to a categorical value"""
    if int(hour) >=0 and int(hour) < 6:
        return 'night'
    elif int(hour) >=6 and int(hour) < 12:
        return 'morning'
    elif int(hour) >=12 and int(hour) < 18:
        return 'afternoon'
    elif int(hour) >=18 and int(hour) < 24:
        return 'evening'
    else:
        return 'other'

def mapLatLon(ts_df, ts_latlon_df, lkp_df, maphashcol, datecol):
    """Helper function for mapping closest lat-lon data point"""
    # Add lat-lon based hashes to noaa and purple air dataframes
    lkp_df[maphashcol] = lkp_df.apply (lambda row: createHashKey(row), axis=1)

    # Keep only the asos columns needed to determine the lat-lon mapping
    lkp_latlon_df = lkp_df[[maphashcol,'lat','lon']]
    lkp_latlon_df.drop_duplicates(inplace=True)
    lkp_latlon_df.set_index(maphashcol, inplace=True)

    # Find the closest lat-lon mapping corresponding to the purple air records
    closest_points = {}
    for name, opoint in ts_latlon_df.iterrows():
        origin = geopy.point.Point(opoint)
        lstDist = []
        for ind, dpoint in lkp_latlon_df.iterrows():
            destination = geopy.point.Point(dpoint)
            distance = geopy.distance.distance(origin, destination).km
            lstDist.append((ind, distance))
        closest_points[name] = sorted(lstDist, key=lambda x: x[1])[0][0]

    # Create dataframe from lat-lon mapping
    latlonmap_df = pd.DataFrame(list(closest_points.items()), columns=['tslatlonhash',maphashcol])

    # Merge purple air data to lat-lon mappings first and then
    # merge the resulting dataframe to asos and epa dataframes
    merged_df = pd.merge(ts_df, latlonmap_df, on='tslatlonhash')

     # Drop common and unwanted columns from noaa and epa dataframes
    lkp_df.drop(['lat','lon'], axis=1, inplace=True)

    # Combine asos data
    combined_df = pd.merge(merged_df, lkp_df,  how='left', left_on=[maphashcol, 'created'], right_on=[maphashcol, datecol])

    return combined_df

def combineData(noaa_df, epa_df, bay_ts_df, month, day, yr):
    """Function to combine noaa, epa and sensor data"""
    try:
        # Add lat-lon based hashes to noaa and purple air dataframes
        bay_ts_df['tslatlonhash'] = bay_ts_df.apply (lambda row: createHashKey(row), axis=1)

        # Keep only the purple air columns needed to determine the lat-lon mapping
        ts_latlon_df = bay_ts_df[['tslatlonhash','lat','lon']]
        ts_latlon_df.drop_duplicates(inplace=True)
        ts_latlon_df.set_index('tslatlonhash', inplace=True)

        try:
            # Find the closest asos lat-lon mapping corresponding to the purple air records
            combined_df = mapLatLon(bay_ts_df, ts_latlon_df, noaa_df, 'asoslatlonhash', 'datetime')

            # Find the closest epa lat-lon mapaping corresponding to the purple air records
            combined_df = mapLatLon(combined_df, ts_latlon_df, epa_df, 'epalatlonhash', 'created')
            combined_df.drop(['tslatlonhash', 'asoslatlonhash', 'epalatlonhash', 'rec_length','num_fields', 'datetime', 'utc', 'parameter'], axis=1, inplace=True)
        except Exception as e:
            print("*** EXCEPTION IN COMBINING NOAA DATA *** {}".format(e))
            combined_df = mapLatLon(bay_ts_df, ts_latlon_df, epa_df, 'epalatlonhash', 'created')
            combined_df.drop(['tslatlonhash', 'epalatlonhash', 'utc', 'parameter'], axis=1, inplace=True)

            #Dummy columns for noaa data in case of noaa error
            combined_df['wban_number'] = None
            combined_df['call_sign'] = None
            combined_df['call_sign2'] = None
            combined_df['interval'] = None
            combined_df['call_sign3'] = None
            combined_df['zulu_time'] = None
            combined_df['report_modifier'] = None
            combined_df['wind_data'] = False
            combined_df['wind_direction'] = None
            combined_df['wind_speed'] = np.nan
            combined_df['gusts'] = False
            combined_df['gust_speed'] = np.nan
            combined_df['variable_winds'] = np.nan
            combined_df['variable_wind_info'] = None
            combined_df['sys_maint_reqd'] = False

        # Only outside sensors
        combined_df = combined_df[combined_df.device_loc_typ == 'outside']

        # Write to S3
        s3 = s3fs.S3FileSystem()
        myopen = s3.open
        s3_resource = boto3.resource('s3')
        write('midscapstone-whos-polluting-my-air/CombinedDailyInterpolated/20{2}{0}{1:02}.parquet'.format(month, day, yr), combined_df, compression='GZIP', open_with=myopen)
        s3_resource.Object('midscapstone-whos-polluting-my-air', 'CombinedDailyInterpolated/20{2}{0}{1:02}.parquet'.format(month, day, yr)).Acl().put(ACL='public-read')
        return combined_df
    except Exception as e:
        print("*** EXCEPTION IN COMBINE DATA *** {}".format(e))

def addToMonthly(df, month, year):
    """Function to add daily data to monthly folder"""

    mth = "{:0>2}".format(month)
    yr = '20' + str(year)

    # define direction degree range by rounding to nearest cardinal direction
    # tried a reduced range of only +/- 15 degrees instead of 45 degrees
    NORTH = (346,15)
    EAST = (76,105)
    SOUTH = (166,195)
    WEST = (256,285)

    try:
        df.reset_index(inplace=True, drop=True)

        # remove rows with na data for 2_5um
        df = df[df['2_5um'].notna()]
        df = df[df.sys_maint_reqd != 1]
        df = df[df.high_reading_flag != 1]

        df['wkday'] = df['created_at'].apply(lambda x: datetime.datetime.strptime(x,"%Y/%m/%dT%H:%M").strftime("%w"))
        df['daytype'] = df['wkday'].apply(lambda x: 'Weekend' if x in ('0','6') else 'Weekday')
        df['timeofday'] = df['hour'].apply(lambda x: timeOfDay(x))

        # go through the dataframe and add new categorical column that indicates direction:
        # North, South, East, West, No wind, Missing, ERROR
        wind_compass = []
        for row in range(len(df)):
            try:
                degree = int(df.loc[row].wind_direction)
            except Exception as e:
                wind_compass.append('Missing')
                continue
            if df.loc[row].wind_speed == 0:
                wind_compass.append('No wind')
            elif degree >= NORTH[0] or degree <= NORTH[1]:
                wind_compass.append('North')
            elif degree >= EAST[0] and degree <= EAST[1]:
                wind_compass.append('East')
            elif degree >= SOUTH[0] and degree <= SOUTH[1]:
                wind_compass.append('South')
            elif degree >= WEST[0] and degree <= WEST[1]:
                wind_compass.append('West')
            else:
                wind_compass.append('ERROR')
        df['wind_compass'] = wind_compass

        try:
            # grab current monthly file
            s3_resource.Object('midscapstone-whos-polluting-my-air', 'CombinedMonthly/{}}{}.parquet'.format(yr, mth)).load()
            pf=ParquetFile('midscapstone-whos-polluting-my-air/CombinedMonthly/{}}{}.parquet'.format(yr, mth), open_with=myopen)
            month_df=pf.to_pandas()
            month_df = month_df.append(df,ignore_index=True)
        except:
            # start of new month
            month_df = df.copy()

        # Write to S3
        s3 = s3fs.S3FileSystem()
        myopen = s3.open
        s3_resource = boto3.resource('s3')
        write('midscapstone-whos-polluting-my-air/CombinedMonthly/{}{}.parquet'.format(yr, mth), month_df, compression='GZIP', open_with=myopen)
        s3_resource.Object('midscapstone-whos-polluting-my-air', 'CombinedMonthly/{}{}.parquet'.format(yr, mth)).Acl().put(ACL='public-read')
    except Exception as e:
        print("*** EXCEPTION IN MONTHLY DATA *** {}".format(e))
