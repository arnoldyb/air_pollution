#!/usr/bin/env python3

import pandas as pd
import numpy as np
import json
import datetime
import re
import ast
from fastparquet import ParquetFile, write
import boto3
import s3fs
from geopy.distance import distance
import geopy
from getData import get_data

import warnings
warnings.filterwarnings('ignore')

# Class for throwing custom errors
class CustomError(Exception):
    def __init__(self, m):
        self.message = m
    def __str__(self):
        return self.message


def createHashKey(row):
    if np.isnan(row['lat']):
        str_lat = ''
    else:
        str_lat = str(row['lat'])


    if np.isnan(row['lon']):
        str_lon = ''
    else:
        str_lon = str(row['lon'])

    return hash(str_lat + str_lon)


# Function for mapping closest lat-lon data point
def mapLatLon(ts_df, ts_latlon_df, lkp_df, maphashcol, datecol):
    # Add lat-lon based hashes to noaa and purple air dataframes
    lkp_df[maphashcol] = lkp_df.apply (lambda row: createHashKey(row), axis=1)

    # Keep only the asos columns needed to determine the lat-lon mapping
    lkp_latlon_df = lkp_df[[maphashcol,'lat','lon']]
    lkp_latlon_df.drop_duplicates(inplace=True)
    lkp_latlon_df.set_index(maphashcol, inplace=True)

    # Find the closest lat-lon mapping corresponding to the purple air records
    closest_points = {}
#     Euclidean distance based calculation
#     for name, point in ts_latlon_df.iterrows():
#         distances = (((lkp_latlon_df - point) ** 2).sum(axis=1)**.5)
#         closest_points[name] = distances.sort_values().index[0]

#     Geopy based distance calculation
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


# Function to combine data from various sources
def combineData(noaa_df, epa_df, bay_ts_df, month, day, yr):
    try:
#     datafolder = "/Users/apaul2/Documents/_Common/capstone/Project/data"

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

    #     # Write to file
    #     parquet_file = "{0}/combined_interpolated/20{3}{1}{2:02}.parquet".format(datafolder, month, day, yr)
    #     write(parquet_file, combined_df,compression='GZIP')
        # Write to S3
        s3 = s3fs.S3FileSystem()
        myopen = s3.open
        s3_resource = boto3.resource('s3')
        write('midscapstone-whos-polluting-my-air/CombinedDailyInterpolated/20{2}{0}{1:02}.parquet'.format(month, day, yr), combined_df, open_with=myopen)
        s3_resource.Object('midscapstone-whos-polluting-my-air', 'CombinedDailyInterpolated/20{2}{0}{1:02}.parquet'.format(month, day, yr)).Acl().put(ACL='public-read')
    except Exception as e:
        print("*** EXCEPTION IN COMBINE DATA *** {}".format(e))
