#!/usr/bin/env python3

import pandas as pd
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

def getPurpleAirData(yr, month, day):
    """Helper function to get purple air data from s3"""

    filename = "20{}{}{:02}.parquet".format(yr, month, day)
    try:
        s3 = s3fs.S3FileSystem()
        myopen = s3.open
        s3_resource = boto3.resource('s3')
        s3_resource.Object('midscapstone-whos-polluting-my-air', 'PurpleAirDaily/{}'.format(filename)).load()
        pf=ParquetFile('midscapstone-whos-polluting-my-air/PurpleAirDaily/{}'.format(filename), open_with=myopen)
        bay_purple_df=pf.to_pandas()
    except Exception as e:
        print("*** EXCEPTION IN GET PURPLE AIR DATA *** {}".format(e))

    return bay_purple_df
