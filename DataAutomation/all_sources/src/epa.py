#!/usr/bin/env python3

import pandas as pd
import numpy as np
import json
import datetime, time
from dateutil import tz
from pytz import timezone
import ast
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


# Get epa data
def getEPAHistData(month, yr):
#     datafolder = "/Users/apaul2/Documents/_Common/capstone/Project/data"

#     epa_df = pd.read_csv("{}/ambient/historical_PM25.csv".format(datafolder))
#     epa_df.columns = ['lat', 'lon', 'utc', 'parameter', 'epa_pm25_unit', 'epa_pm25_value','raw_concentration', 'aqi', 'category', 'site_name', 'agency_name',
#        'full_aqs_code', 'intl_aqs_code']
#     epa_df = pd.read_parquet("{}/ambient/epa_201910.parquet".format(datafolder))

    # Change this to use the csv file being modified every hour
    try:
        try:
            s3 = s3fs.S3FileSystem()
            myopen = s3.open
            s3_resource = boto3.resource('s3')
            s3_resource.Object('midscapstone-whos-polluting-my-air', 'EpaRaw/epa_20{}{}.parquet'.format(yr, month)).load()
            pf=ParquetFile('midscapstone-whos-polluting-my-air/EpaRaw/epa_20{}{}.parquet'.format(yr, month), open_with=myopen)
            epa_df=pf.to_pandas()
        except:
            raise CustomError("FILE ERROR: Epa Raw Dataframe not found")

        # Add a datekey column based on local date
        epa_df.rename(columns={'Latitude':'lat', 'Longitude':'lon', 'UTC':'utc', 'Parameter':'parameter', 'Unit':'epa_pm25_unit', 'Value':'epa_pm25_value',
                    'RawConcentration':'raw_concentration', 'AQI':'aqi', 'Category':'category', 'SiteName':'site_name', 'AgencyName':'agency_name',
                    'FullAQSCode':'full_aqs_code', 'IntlAQSCode':'intl_aqs_code'}, inplace=True)
        epa_df['created'] = epa_df['utc'].apply(lambda x: int(datetime.datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S').replace(tzinfo=tz.tzutc()).astimezone(timezone('US/Pacific')).strftime("%Y%m%d%H%M")))
    except  Exception as e:
        print("*** EXCEPTION IN GET EPA HIST DATA *** {}".format(e))
    return epa_df


# Get daily interpolated epa data
def getEPADailyData(dateint, dt_ind, month, epa_df, yr):
#     datafolder = "/Users/apaul2/Documents/_Common/capstone/Project/data"
    try:
        start = dateint + dt_ind * 10000
        end = start + 10001
        dly_epa_df = epa_df[(epa_df.created >= start) & (epa_df.created < end)]
        dly_epa_df.reset_index(inplace=True, drop=True)

        new_df = pd.DataFrame(columns=['lat', 'lon', 'utc', 'parameter', 'epa_pm25_unit', 'epa_pm25_value', 'raw_concentration', 'aqi', 'category', 'site_name', 'agency_name', 'full_aqs_code', 'intl_aqs_code', 'created'])
        for sitenm in dly_epa_df.site_name.unique():
            indx_ct = 0
            site_df = dly_epa_df[dly_epa_df.site_name == sitenm]
            for i in site_df.created.unique():
                indx_ct += 1
                new_df =  pd.concat([new_df,site_df.iloc[indx_ct - 1:indx_ct]],ignore_index=True)

                if i != site_df.created.max(): # Don't interpolate the last record
                    tmp_df = site_df.iloc[indx_ct - 1:indx_ct][['lat', 'lon', 'utc', 'parameter', 'epa_pm25_unit', 'category', 'site_name', 'agency_name', 'full_aqs_code', 'intl_aqs_code']]
                    for j in range(1,6):
                        new_dt = i + j * 10
                        tmp_df['created'] = int(new_dt)
                        tmp_df['epa_pm25_value'] = np.nan
                        tmp_df['raw_concentration'] = np.nan
                        tmp_df['aqi'] = np.nan
                        new_df =  pd.concat([new_df,tmp_df],ignore_index=True)

        # Convert aqi to numerica for so that it gets interpolated
        new_df[['aqi']] = new_df[['aqi']].replace("nan", np.nan, regex=True)
        new_df[['aqi']] = new_df[['aqi']].apply(pd.to_numeric)

        new_df = new_df.interpolate(method='linear', limit_direction='forward', axis=0)

        int_epa_df = new_df[(new_df.created >= start) & (new_df.created < (end - 1))]
        int_epa_df.reset_index(inplace=True, drop=True)

    #     parquet_file = "{0}/ambient/daily_interpolated/epa_20{3}{1}{2:02}.parquet".format(datafolder, month, dt_ind, yr)
    #     write(parquet_file, int_epa_df,compression='GZIP')
        # Write to S3
        s3 = s3fs.S3FileSystem()
        myopen = s3.open
        write('midscapstone-whos-polluting-my-air/EpaDaily/epa_20{2}{0}{1:02}.parquet'.format(month, dt_ind, yr), int_epa_df, compression='GZIP', open_with=myopen)
        s3_resource = boto3.resource('s3')
        s3_resource.Object('midscapstone-whos-polluting-my-air', 'EpaDaily/epa_20{2}{0}{1:02}.parquet'.format(month, dt_ind, yr)).Acl().put(ACL='public-read')

    except Exception as e:
        print("*** EXCEPTION IN GET EPA DAILY DATA *** {}".format(e))
    return int_epa_df
