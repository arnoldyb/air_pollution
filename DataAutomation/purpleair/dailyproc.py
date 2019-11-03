#!/usr/bin/env python3

import pandas as pd
import numpy as np
import json
import datetime, time
from dateutil import tz
from pytz import timezone
from math import floor
import ast
import os
import boto3
import s3fs
from fastparquet import ParquetFile, write

import warnings
warnings.filterwarnings('ignore')


# Class for throwing custom errors
class CustomError(Exception):
    def __init__(self, m):
        self.message = m
    def __str__(self):
        return self.message

def createHashKey(row, col1, col2):
    str_col1 = row[col1]
    str_col2 = row[col2]
    return hash(str_col1 + str_col2)

def getDailyData(s3open, s3Objs, startInd, endInd, year):
    """
    Concatenate the 5 minute data into a single dataframe at daily level
    """

    purple_df = pd.DataFrame(columns=['AGE', 'A_H', 'DEVICE_LOCATIONTYPE', 'Flag', 'Hidden', 'ID', 'Label', 'LastSeen', 'Lat', 'Lon', 'PM2_5Value', 'ParentID', 'THINGSPEAK_PRIMARY_ID',
                           'THINGSPEAK_PRIMARY_ID_READ_KEY', 'THINGSPEAK_SECONDARY_ID', 'THINGSPEAK_SECONDARY_ID_READ_KEY', 'Type', 'humidity', 'isOwner', 'pressure', 'temp_f',
                           'lastModified', 'timeSinceModified', 'v1', 'v2', 'v3', 'v4', 'v5', 'v6'])
    for obj in s3Objs:
        file_name = int(obj.key.replace('PurpleAir/{}'.format(year),'').replace('.parquet',''))
        if file_name >= startInd and file_name < endInd:
            # print("*** FILENAME: {} ***".format(file_name))
            pf=ParquetFile('midscapstone-whos-polluting-my-air/{}'.format(obj.key), open_with=s3open)
            df=pf.to_pandas()
            # print("*** DF LENGTH: {} ***".format(len(df)))

            try:
                df = pd.DataFrame.from_records(df.results)
            except:
                df['results'] =  df['results'].map(lambda d : ast.literal_eval(d))
                df = pd.DataFrame.from_records(df.results)

            # print("*** BEFORE STATS ***")
            try:
                # split the dict in the 'Stats' column into separate columns
                df['Stats'] = df['Stats'].replace(np.nan, '{}', regex=True)
                df['Stats'] =  df['Stats'].map(lambda d : ast.literal_eval(d))
                df = df.join(pd.DataFrame(df["Stats"].to_dict()).T)
                df.drop(['Stats', 'pm','v'], axis=1, inplace=True)   # 'pm' and 'v' are the same as 'PM2_5Value'

                # print("*** AFTER STATS ***")
                df = df[['AGE', 'A_H', 'DEVICE_LOCATIONTYPE', 'Flag', 'Hidden', 'ID', 'Label', 'LastSeen', 'Lat', 'Lon', 'PM2_5Value', 'ParentID', 'THINGSPEAK_PRIMARY_ID',
                                       'THINGSPEAK_PRIMARY_ID_READ_KEY', 'THINGSPEAK_SECONDARY_ID', 'THINGSPEAK_SECONDARY_ID_READ_KEY', 'Type', 'humidity', 'isOwner', 'pressure', 'temp_f',
                                       'lastModified', 'timeSinceModified', 'v1', 'v2', 'v3', 'v4', 'v5', 'v6']]

                # print("*** BEFORE CONCAT ***")
                purple_df = pd.concat([purple_df,df],ignore_index=True)
                # print("*** AFTER CONCAT ***")
            except Exception as e:
                print("*** EXCEPTION IN PROCESSING STATS FOR FILE {}: {}***".format(file_name, e))

    return purple_df


def main():
    # Get Inputs
    year = '2019'
    month = '10'
    day = '1'
    numdays = 31

    for i in range(int(numdays)):
        day = int(day) + i
        daystr = "{:0>2}".format(day)
        print("*** PROCESSING DAY: {} ***".format(day))
        startInd = int(month + daystr + '0659')
        endInd = startInd + 10000

        s3 = s3fs.S3FileSystem()
        myopen = s3.open

        s3_resource = boto3.resource('s3')
        bucket = s3_resource.Bucket('midscapstone-whos-polluting-my-air')
        s3Objs = bucket.objects.filter(Prefix='PurpleAir/{}'.format(year))

        print("*** CALL FUNCTION ***")
        purple_df = getDailyData(myopen, s3Objs, startInd, endInd, year)

        print("*** RENAME COLUMNS ***")
        purple_df.rename(columns={'AGE':'age', 'A_H':'a_h', 'DEVICE_LOCATIONTYPE':'device_loc_typ', 'Flag':'high_reading_flag', 'Hidden':'hidden', 'ID':'sensor_id', 'Label':'sensor_name',
                              'LastSeen':'last_seen', 'Lat':'lat', 'Lon':'lon', 'PM2_5Value':'pm2_5val', 'ParentID':'parent_id', 'THINGSPEAK_PRIMARY_ID':'thingspeak_primary_id',
                              'THINGSPEAK_PRIMARY_ID_READ_KEY':'thingspeak_primary_id_read_key', 'THINGSPEAK_SECONDARY_ID':'thingspeak_secondary_id',
                              'THINGSPEAK_SECONDARY_ID_READ_KEY':'thingspeak_secondary_id_read_key', 'Type':'sensor_type', 'humidity':'humidity', 'isOwner':'is_owner', 'pressure':'pressure',
                              'temp_f':'temp_f', 'lastModified':'av_stat_last_modified', 'timeSinceModified':'av_stat_time_since_last_modified', 'v1':'pm2_5val_10m_avg', 'v2':'pm2_5val_30m_avg',
                              'v3':'pm2_5val_1h_avg', 'v4':'pm2_5val_6h_avg', 'v5':'pm2_5val_24h_avg', 'v6':'pm2_5val_1wk_avg'}, inplace=True)

        # Drop unwanted columns
        purple_df.drop(['age','av_stat_last_modified', 'av_stat_time_since_last_modified','pm2_5val_10m_avg', 'pm2_5val_30m_avg', 'pm2_5val_1h_avg',
               'pm2_5val_6h_avg', 'pm2_5val_24h_avg', 'pm2_5val_1wk_avg', 'pm2_5val','humidity','pressure','temp_f','sensor_type'], axis=1, inplace=True)
        # There may be duplicates in sensor data in case no new readings we obtained since the last refresh
        purple_df.drop_duplicates(inplace=True)

        bayarea_purple_df = purple_df[(purple_df.lat > 37.701933) & (purple_df.lat < 38.008050)
                                  & (purple_df.lon > -122.536985) & (purple_df.lon < -122.186437)]
        bayarea_purple_df.reset_index(inplace=True, drop=True)

        # Get date and time columns in local timezone
        bayarea_purple_df['year'] = bayarea_purple_df['last_seen'].apply(lambda x: datetime.datetime.fromtimestamp(x).replace(tzinfo=tz.tzutc()).astimezone(timezone('US/Pacific')).strftime("%Y"))
        bayarea_purple_df['month'] = bayarea_purple_df['last_seen'].apply(lambda x: datetime.datetime.fromtimestamp(x).replace(tzinfo=tz.tzutc()).astimezone(timezone('US/Pacific')).strftime("%m"))
        bayarea_purple_df['day'] = bayarea_purple_df['last_seen'].apply(lambda x: datetime.datetime.fromtimestamp(x).replace(tzinfo=tz.tzutc()).astimezone(timezone('US/Pacific')).strftime("%d"))
        bayarea_purple_df['hour'] = bayarea_purple_df['last_seen'].apply(lambda x: datetime.datetime.fromtimestamp(x).replace(tzinfo=tz.tzutc()).astimezone(timezone('US/Pacific')).strftime("%H"))
        bayarea_purple_df['minute'] = bayarea_purple_df['last_seen'].apply(lambda x: datetime.datetime.fromtimestamp(x).replace(tzinfo=tz.tzutc()).astimezone(timezone('US/Pacific')).strftime("%M"))
        bayarea_purple_df['10min'] = bayarea_purple_df['minute'].apply(lambda x: "{:02}".format(10 * floor(int(x)/10)))

        bayarea_purple_df['datetime'] = bayarea_purple_df[['year', 'month','day','hour','10min']].apply(lambda x: int(''.join(x)), axis=1)

        # Drop unwanted columns from purple air data
        bayarea_purple_df.drop(['last_seen', 'hour', 'minute', '10min'], axis = 1, inplace=True)
        bayarea_purple_df.drop_duplicates(inplace=True)

        bayarea_purple_dly_df =bayarea_purple_df[(bayarea_purple_df.year == str(year)) & (bayarea_purple_df.month == str(month)) & (bayarea_purple_df.day == str(daystr))]

        # Drop unwanted columns from purple air data
        bayarea_purple_dly_df.drop(['year', 'month', 'day'], axis = 1, inplace=True)
        bayarea_purple_dly_df.drop_duplicates(inplace=True)

        # Add hash column based on the primary and secondary keys
        bayarea_purple_dly_df['sensorhash'] = bayarea_purple_dly_df.apply (lambda row: createHashKey(row,'thingspeak_primary_id_read_key',
                                                                                                    'thingspeak_secondary_id_read_key'), axis=1)
        # save
        print("*** WRITE TO S3 ***")
        write('midscapstone-whos-polluting-my-air/PurpleAirRaw/{}{}{:02}.parquet'.format(year, month, day), bayarea_purple_dly_df, open_with=myopen)
        print("*** MAKE FILE PUBLIC ***")
        s3_resource.Object('midscapstone-whos-polluting-my-air', 'PurpleAirRaw/{}{}{:02}.parquet'.format(year, month, day)).Acl().put(ACL='public-read')

if __name__ == "__main__":
	main()