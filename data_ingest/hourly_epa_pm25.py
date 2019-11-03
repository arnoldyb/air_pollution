#!/home/ubuntu/miniconda3/bin/python

import requests
import datetime
import pandas as pd
from dateutil.relativedelta import relativedelta
import time
import subprocess
import pathlib


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

    
    MIN_LAT = 37.701933
    MAX_LAT = 38.008050
    MIN_LON = -122.536985
    MAX_LON = -122.186437
    
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

def main():
    """
    This file pulls EPA ambient PM2.5 readings from four regularatory sensors
    in the Bay Area, appends them to a monthly file, and uploads that monthly file to 
    S3. 

    EPA readings are recorded on the hour. As of 10/6/2019, this script runs once per hour
    at 20 minutes after the hour (readings are not updated immediately) via cron job from Jake's EC2 instance.

    All times are UTC.
    """
    
    last_hour = datetime.datetime.now()
    next_hour = datetime.datetime.now() + relativedelta(hours=1)
    current_month = last_hour.strftime("%Y%m")
    current_file = f"{current_month}_PM25.csv"
    
    if pathlib.Path(current_file).exists():
        # grab current monthly file
        month_df = pd.read_csv(current_file, parse_dates=['UTC'])
    else:
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
    # save
    month_df.to_csv(current_file, index = False)
    # overwrite s3 
    subprocess.run(f"/home/ubuntu/miniconda3/bin/aws s3 cp {current_file} s3://capstone-air-pollution/EPA/{current_file}", shell = True)
    
main()
