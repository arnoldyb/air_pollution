#!/usr/bin/env python3

import pandas as pd
import numpy as np
import json
import datetime, time
from dateutil import tz
import ast
import boto3
import s3fs
from fastparquet import ParquetFile, write
import urllib3

import warnings
warnings.filterwarnings('ignore')

# Class for throwing custom errors
class CustomError(Exception):
    def __init__(self, m):
        self.message = m
    def __str__(self):
        return self.message


# Get thingspeak data:
def getThingspeakData(bayarea_purple_df, month, day, yr):
#     datafolder = "/Users/apaul2/Documents/_Common/capstone/Project/data"
    try:
        bay_pa_thingspeak_df = bayarea_purple_df[['sensorhash', 'thingspeak_primary_id','thingspeak_primary_id_read_key',
                                                   'thingspeak_secondary_id','thingspeak_secondary_id_read_key']]
        bay_pa_thingspeak_df.drop_duplicates(inplace=True)
        bay_pa_thingspeak_df.reset_index(inplace=True, drop=True)

        print("Length of pa: {}".format(len(bay_pa_thingspeak_df)))

        ts_s_df = genTS2DF(bay_pa_thingspeak_df, month, "{:02}".format(day), yr)
        ts_p_df = genTS1DF(bay_pa_thingspeak_df, month, "{:02}".format(day), yr)

        # Merge data from the two sensors
        # Only keep records having particle data
        bay_ts_df = pd.merge(ts_s_df, ts_p_df,  how='left', left_on=['sensorhash','created'], right_on=['sensorhash','created'])
        bay_ts_df.drop(['created_at_y'], axis=1, inplace=True)

    #     # Write to file
    #     parquet_file = "{}/thingspeak/thingspeak_20{}{}{:02}.parquet".format(datafolder, yr, month, day)
    #     write(parquet_file, bay_ts_df,compression='GZIP')
        # Write to S3
        s3 = s3fs.S3FileSystem()
        myopen = s3.open
        write('midscapstone-whos-polluting-my-air/ThingspeakDaily/thingspeak_20{}{}{:02}.parquet'.format(yr, month, day), bay_ts_df, compression='GZIP', open_with=myopen)
        s3_resource = boto3.resource('s3')
        s3_resource.Object('midscapstone-whos-polluting-my-air', 'ThingspeakDaily/thingspeak_20{}{}{:02}.parquet'.format(yr, month, day)).Acl().put(ACL='public-read')

    except Exception as e:
        print("*** EXCEPTION IN GET THINGSPEAK DATA *** {}".format(e))

    return bay_ts_df



# Get data from sensor 1
def genTS1DF(sensordf, month, startday, yr):
    try:
        https = urllib3.PoolManager()

        ts_p_df = pd.DataFrame(columns=['created_at', 'pm1_0_atm', 'pm2_5_atm', 'pm10_0_atm', 'uptime', 'rssi', 'temperature', 'humidity', 'pm2_5_cf_1','sensorhash'])
        count, errCount = 0, 0

        for ind, val in sensordf.iterrows():
            qrystr = "https://api.thingspeak.com/channels/{0}/feeds.json?api_key={1}&start=20{4}-{2}-{3}%2000:00:00&end=20{4}-{2}-{3}%2023:59:59& \
                        timezone=America/Los_Angeles&timescale=10".format(val['thingspeak_primary_id'], val['thingspeak_primary_id_read_key'], month, startday, yr)

            try:
                count += 1
                r = https.request('GET',qrystr)
                if r.status == 200:
                    j = json.loads(r.data.decode('utf-8'))
                    df = pd.DataFrame(j['feeds'])
                    df.columns=['created_at', 'pm1_0_atm', 'pm2_5_atm', 'pm10_0_atm', 'uptime', 'rssi', 'temperature', 'humidity', 'pm2_5_cf_1']
                    df['sensorhash'] = val['sensorhash']
                    ts_p_df = pd.concat([ts_p_df,df],ignore_index=True)
            except Exception as e:
    #             print(e)
                errCount += 1
                continue
        print("Of the {} requests, {} errored out.".format(count, errCount))

        # Add a key column based on time
        # This along with the sensorhash column will be used to join the two sensor datasets
        ts_p_df['created'] = ts_p_df['created_at'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%dT%H:%M:%SZ").strftime("%Y%m%d%H%M"))
    except  Exception as e:
        print("*** EXCEPTION IN GEN TS1DF *** {}".format(e))
    return ts_p_df



# Get data from sensor 2
def genTS2DF(sensordf, month, startday, yr):
    try:
        https = urllib3.PoolManager()

        ts_s_df = pd.DataFrame(columns=['created_at', '0_3um', '0_5um', '1_0um', '2_5um', '5_0um', '10_0um', 'pm1_0', 'pm10_0','sensorhash'])
        count, errCount = 0, 0

        for ind, val in sensordf.iterrows():
            qrystr = "https://api.thingspeak.com/channels/{0}/feeds.json?api_key={1}&start=20{4}-{2}-{3}%2000:00:00&end=20{4}-{2}-{3}%2023:59:59& \
                        timezone=America/Los_Angeles&timescale=10".format(val['thingspeak_secondary_id'], val['thingspeak_secondary_id_read_key'], month, startday, yr)

            try:
                count += 1
                r = https.request('GET',qrystr)
                if r.status == 200:
                    j = json.loads(r.data.decode('utf-8'))
                    df = pd.DataFrame(j['feeds'])
                    df.columns=['created_at', '0_3um', '0_5um', '1_0um', '2_5um', '5_0um', '10_0um', 'pm1_0', 'pm10_0']
                    df['sensorhash'] = val['sensorhash']
                    ts_s_df = pd.concat([ts_s_df,df],ignore_index=True)
            except Exception as e:
    #             print(e)
                errCount += 1
                continue
        print("For {}, Of the {} requests, {} errored out.".format(startday, count, errCount))

        # Add a key column based on time
        # This along with the sensorhash column will be used to join the two sensor datasets
        ts_s_df['created'] = ts_s_df['created_at'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%dT%H:%M:%SZ").strftime("%Y%m%d%H%M"))
    except  Exception as e:
        print("*** EXCEPTION IN GEN TS2DF *** {}".format(e))

    return ts_s_df



# Merge Purple Air data
def mergePurpleAir(pa_df, ts_df, address_df, month, day, yr):
#     datafolder = "/Users/apaul2/Documents/_Common/capstone/Project/data"

    # Some numeric columns may have "nan" as a string - convert these values to np.nan
    # so that the data type of these columns are correctly identified
    try:
        ts_df[['0_3um', '0_5um', '1_0um', '2_5um', '5_0um', '10_0um', 'pm1_0','pm10_0', 'created', 'pm1_0_atm', 'pm2_5_atm', 'pm10_0_atm', 'uptime',
               'rssi', 'temperature', 'humidity', 'pm2_5_cf_1']] = ts_df[['0_3um', '0_5um', '1_0um', '2_5um', '5_0um', '10_0um', 'pm1_0',
               'pm10_0', 'created', 'pm1_0_atm', 'pm2_5_atm', 'pm10_0_atm', 'uptime', 'rssi', 'temperature', 'humidity', 'pm2_5_cf_1']].replace("nan", np.nan, regex=True)
        ts_df[['0_3um', '0_5um', '1_0um', '2_5um', '5_0um', '10_0um', 'pm1_0','pm10_0', 'created', 'pm1_0_atm', 'pm2_5_atm', 'pm10_0_atm', 'uptime',
               'rssi', 'temperature', 'humidity', 'pm2_5_cf_1']] = ts_df[['0_3um', '0_5um', '1_0um', '2_5um', '5_0um', '10_0um', 'pm1_0',
               'pm10_0', 'created', 'pm1_0_atm', 'pm2_5_atm', 'pm10_0_atm', 'uptime', 'rssi', 'temperature', 'humidity', 'pm2_5_cf_1']].apply(pd.to_numeric)

        # Get lat lon info from purple air dataset into a separate dataframe
        pa_latlon_df = pa_df[['device_loc_typ', 'is_owner', 'sensor_id', 'sensor_name',  'parent_id', 'lat', 'lon', 'thingspeak_primary_id', 'thingspeak_primary_id_read_key', 'thingspeak_secondary_id',
                                          'thingspeak_secondary_id_read_key', 'sensorhash']]
        pa_latlon_df.drop_duplicates(inplace=True)

        # Get relevant data from purple air dataset into a separate dataframe
        pa_data_df = pa_df[['a_h', 'high_reading_flag', 'hidden', 'datetime', 'sensorhash']]
        pa_data_df.drop_duplicates(inplace=True)

        # Merge purple air data with sensor data
        # Only keep records having particle data
        ts_df = pd.merge(ts_df, pa_latlon_df,  how='left', left_on=['sensorhash'], right_on=['sensorhash'])
        ts_df = pd.merge(ts_df, pa_data_df,  how='left', left_on=['sensorhash', 'created'], right_on=['sensorhash', 'datetime'])

        # Join address dataframe with main dataframe
        ts_df = pd.merge(ts_df, address_df,  how='left', left_on=['lat','lon'], right_on=['lat','lon'])

        ts_df['created_at'] = ts_df['created_at_x'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%dT%H:%M:%SZ").strftime("%Y/%m/%dT%H:%M"))
        ts_df['year'] = ts_df['created_at_x'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%dT%H:%M:%SZ").strftime("%Y"))
        ts_df['month'] = ts_df['created_at_x'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%dT%H:%M:%SZ").strftime("%m"))
        ts_df['day'] = ts_df['created_at_x'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%dT%H:%M:%SZ").strftime("%d"))
        ts_df['hour'] = ts_df['created_at_x'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%dT%H:%M:%SZ").strftime("%H"))
        ts_df['minute'] = ts_df['created_at_x'].apply(lambda x: datetime.datetime.strptime(x,"%Y-%m-%dT%H:%M:%SZ").strftime("%M"))

        # Drop unwanted columns
        ts_df.drop(['created_at_x', 'sensorhash', 'datetime', 'country','state'], axis = 1, inplace=True)

        # Convert data type of attributes to string
        ts_df[['high_reading_flag','sensor_id','parent_id', 'is_owner']] = ts_df[['high_reading_flag','sensor_id','parent_id', 'is_owner']].astype(str)

    #     # Save final dataframe for future use
    # #     parquet_file = "{}/pa_ts/201909{}.parquet".format(datafolder,days_list[i])
    #     parquet_file = "{}/pa_ts/20{}{}{:02}.parquet".format(datafolder, yr, month, day)
    #     write(parquet_file, ts_df,compression='GZIP')
    except  Exception as e:
        print("*** EXCEPTION IN MERGE PURPLE AIR *** {}".format(e))

    return ts_df
