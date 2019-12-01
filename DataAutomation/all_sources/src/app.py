#!/usr/bin/env python3

import pandas as pd
import os
import datetime, time
from dateutil import tz
from pytz import timezone
import boto3
import s3fs
import purpleAir
import epa
import noaa
import thingSpeak
import commonAirPollUtils
from fastparquet import ParquetFile

import warnings
warnings.filterwarnings('ignore')

# Class for throwing custom errors
class CustomError(Exception):
    def __init__(self, m):
        self.message = m
    def __str__(self):
        return self.message


# Function for getting address dataframe
def getAddress():
    """Helper function to read the addresses corresponding to the lat-lon of existing sensors from s3"""

    try:
        s3 = s3fs.S3FileSystem()
        myopen = s3.open
        s3_resource = boto3.resource('s3')
        s3_resource.Object('midscapstone-whos-polluting-my-air', 'UtilFiles/address_latlon.parquet').load()
        pf=ParquetFile('midscapstone-whos-polluting-my-air/UtilFiles/address_latlon.parquet', open_with=myopen)
        address_df=pf.to_pandas()
    except Exception as e:
        print("*** EXCEPTION IN GET ADDRESS: {}".format(e))

    return address_df

def handler(event, context):
    """Main function for processing daily data"""

    # Set date parameters
    prevday = datetime.datetime.now()-datetime.timedelta(1)
    month = prevday.replace(tzinfo=tz.tzutc()).astimezone(timezone('US/Pacific')).strftime("%m")
    startindex = int(prevday.replace(tzinfo=tz.tzutc()).astimezone(timezone('US/Pacific')).strftime("%d"))
    endindex = startindex + 1
    dateint = int(prevday.replace(tzinfo=tz.tzutc()).astimezone(timezone('US/Pacific')).strftime("%Y%m")) * 1000000
    yr = prevday.replace(tzinfo=tz.tzutc()).astimezone(timezone('US/Pacific')).strftime("%y")

    # Get address dataframe
    address_df = getAddress()

    # Get noaa data for the entire month
    bay_noaa_df = noaa.getNOAAData(month, yr)

    # Get historical epa data
    epa_df = epa.getEPAHistData(month, yr)

    for i in range(startindex, endindex):
        try:
            # Get purple air data
            bay_purple_df = purpleAir.getPurpleAirData(yr, month, i)

            # Get thingspeak data
            bay_ts_df = thingSpeak.getThingspeakData(bay_purple_df, month, i, yr)

            # Merge purple air data
            bay_ts_df = thingSpeak.mergePurpleAir(bay_purple_df, bay_ts_df, address_df, month, i, yr)

            # Get noaa data
            dly_noaa_df = noaa.getDailyNOAA(bay_noaa_df, month, i, yr)

            # Get epa data
            int_epa_df = epa.getEPADailyData(dateint, i, month, epa_df, yr)

            # Combine data and save to file
            comb_df = commonAirPollUtils.combineData(dly_noaa_df, int_epa_df, bay_ts_df, month, i, yr)

            # Add to monthly folder
            commonAirPollUtils.addToMonthly(comb_df, month, yr)
        except Exception as e:
            print("Error processing data for {}/{}: \n{}".format(month, startindex, e))
            continue
