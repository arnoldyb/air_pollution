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


# Get purpleair data:
def getPurpleAirData(yr, month, day):

#     datafolder = "/Users/apaul2/Documents/_Common/capstone/Project/data"

#     bay_purple_df = pd.read_parquet("{}/purpleair/dailyfiltered/20{}{}{:02}.parquet".format(datafolder, yr, month, day))
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
#         raise CustomError("FILE ERROR: Purple Air Dataframe not found")

    return bay_purple_df
