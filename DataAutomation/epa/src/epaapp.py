#!/usr/bin/env python3

import requests
import datetime
import pandas as pd
from dateutil.relativedelta import relativedelta
from dateutil import tz
import time
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


def get_AQI(start_time = None, end_time = None):
    """
    Return PM2.5 values for the four sites within our bounding box for
    the specified time period
    """

    if start_time is None:
        start_time = datetime.datetime.now().strftime("%Y-%m-%dT%H")
    if end_time is None:
        end_time = datetime.datetime.now() + relativedelta(hours=1)
        end_time = next_hour.strftime("%Y-%m-%dT%H")

    print("*** START TIME : {} ***".format(start_time))
    print("*** END TIME : {} ***".format(end_time))

    MIN_LAT = 37.2781261
    MAX_LAT = 38.063446
    MIN_LON = -122.683496
    MAX_LON = -121.814281

    EPA_URL = "http://www.airnowapi.org/aq/data/"
    params = {"startDate":start_time,
              "endDate":end_time,
              "parameters":"PM25",
              "BBOX":f"{MIN_LON},{MIN_LAT},{MAX_LON},{MAX_LAT}",
              "dataType":"B",
              "format":"application/json",
              "verbose":1,
              "nowcastonly":1,
              "includerawconcentrations":1,
              "API_KEY":"8FA4FE40-BEC5-4265-B251-D29F48C142D1"}

    r = requests.get(EPA_URL, params = params)
    return pd.DataFrame(r.json())


def handler(event, context):
    """
    This file pulls EPA ambient PM2.5 readings from four regularatory sensors
    in the Bay Area, appends them to a monthly file, and uploads that monthly file to
    S3.
    EPA readings are recorded on the hour. As of 10/6/2019, this script runs once per hour
    at 20 minutes after the hour (readings are not updated immediately) via cron job from Jake's EC2 instance.
    All times are UTC.
    """

    last_hour = datetime.datetime.now()
    next_hour = last_hour + relativedelta(hours=1)
    mth_hour = last_hour + relativedelta(hours=-7)
    current_month = mth_hour.strftime("%Y%m")

    s3 = s3fs.S3FileSystem()
    myopen = s3.open
    s3_resource = boto3.resource('s3')

    try:
        # grab current monthly file
        s3_resource.Object('midscapstone-whos-polluting-my-air', 'EpaRaw/epa_{}.parquet'.format(current_month)).load()
        pf=ParquetFile('midscapstone-whos-polluting-my-air/EpaRaw/epa_{}.parquet'.format(current_month), open_with=myopen)
        month_df=pf.to_pandas()
    except:
        # start of new month
        month_df = pd.DataFrame(columns = ['Latitude', 'Longitude', 'UTC', 'Parameter', 'Unit', 'Value',
                                           'RawConcentration', 'AQI', 'Category', 'SiteName', 'AgencyName',
                                           'FullAQSCode', 'IntlAQSCode'])

    print("Fetching", last_hour.strftime("%Y-%m-%dT%H"))
    # get data
    while True:
        latest_df = get_AQI(last_hour.strftime("%Y-%m-%dT%H"), next_hour.strftime("%Y-%m-%dT%H"))

        if latest_df.shape[0] == 0 or latest_df.shape[1] == 0:
            # API call returns empty DF
            time.sleep(60)
            continue
        else:
            break

    latest_df['UTC'] = pd.to_datetime(latest_df['UTC'])

    # add to our df
    month_df = month_df.append(latest_df,ignore_index=True)

    # save to s3
    write('midscapstone-whos-polluting-my-air/EpaRaw/epa_{}.parquet'.format(current_month), month_df, compression='GZIP', open_with=myopen)
    s3_resource.Object('midscapstone-whos-polluting-my-air', 'EpaRaw/epa_{}.parquet'.format(current_month)).Acl().put(ACL='public-read')
