#!/usr/bin/env python3

import pandas as pd
import json
import os
import datetime
import re
from fastparquet import ParquetFile, write
import s3fs
import boto3

import warnings
warnings.filterwarnings('ignore')

# Class for throwing custom errors
class CustomError(Exception):
    def __init__(self, m):
        self.message = m
    def __str__(self):
        return self.message


def createNOAAdf(lines, fileName):
    """ Helper function to process noaa data"""

#     datafolder = "/Users/apaul2/Documents/_Common/capstone/Project/data"

    # split lines and data chunks
    data = [] # an array of arrays, inner arrays are all data for one record, outer array is all records
    for line in lines:
        try:
            # reset any variables if needed
            record = []
            Report_Modifier = ''
            Wind_Data = False
            Variable_Winds = False
            Gusts = False
            Wind_Direction = ''
            Wind_Speed = ''
            Gust_Speed = ''
            Variable_Wind_Info = ''
            System_Maintenance_Reqd = False

            try:
                line = line.split() # take string of one record's data and split into space separated chunks
                WBAN_Number = line[0][0:5] # The WBAN (Weather Bureau, Army, Navy) number is a unique 5-digit number
                Call_Sign = line[0][5:] # The call sign is a location identifier, three or four characters in length
                suffix = line[1][-2:] # grab the last two digits that are the year (i.e. 19 for 2019)
                Year = '20'+suffix # in YYYY format
                CallSign_Date = re.split(Year, line[1])
                Call_Sign2 = CallSign_Date[0] # this seems to be the same as Call_Sign but without initial letter
                Date = CallSign_Date[1]
                Month = Date[0:2] # in MM format
                Day = Date[2:4] # in DD format
                Hour = Date[4:6] # in HH format
                Minute = Date[6:8] # Observations are recorded on whole five-minute increments (i.e. 00,05,10,...,50,55)
                Record_Length = Date[8:11] # I'm not sure what this is yet - Length of record??
                Date = Date[11:] # MM/DD/YY format
                Timestamp = line[2] # in HH:MM:SS format
                Interval = line[3] # should be 5-MIN as opposed to 1-MIN
                Call_Sign3 = line[4] # for some reason, a THIRD output of the call sign. random.
                Zulu_Time = line[5] # Zulu Time, or military time, or UTC
            except Exception as e:
                print("Exception processing fields : \n{}".format(e))
                continue

            # after this point, data could be missing/optional and data positions are not fixed
            currIndx = 6
            try:
                Next_Data = line[currIndx]
                if not any(x in Next_Data for x in ['KT','SM']):
                    Report_Modifier = Next_Data # AUTO for fully automated report, COR for correction to a previously disseminated report
                    currIndx += 1
                Next_Data = line[currIndx]
                if "KT" in Next_Data:
                    Wind_Data = True
                    Wind_Direction = Next_Data[0:3] # in tens of degrees from true north
                    if Next_Data[0:3] == 'VRB':
                        Variable_Winds = True
                    Wind_Speed = Next_Data[3:5] # in whole knots (two digits)
                    if Next_Data[5] == 'G':
                        Gusts = True
                        Gust_Speed = Next_Data[6:8] # speed in whole knots (two digits)
                else:
                    Wind_Data = False
            except:
                print("OUT OF DATA AT FIELD {}".format(currIndx))
            finally:
                currIndx += 1

            try:
                Next_Data = line[currIndx]
                if Wind_Data:
                    if (re.fullmatch(r'[0-9][0-9][0-9]V[0-9][0-9][0-9]', Next_Data)): #e.g. 180V240 = wind direction varies from 180 to 240 degrees
                        Variable_Wind_Info = Next_Data
                        Variable_Winds = True
            except:
                print("OUT OF DATA AT FIELD {}".format(currIndx))

            if line[-1] == '$':
                System_Maintenance_Reqd = True

            #Sea_Level_Pressure = line[13] # given in tenths of hectopascals (millibars). The last digits are recorded (125 means 1012.5)
            #Station_Type = line[18]
            Num_Fields = len(line)
            record = [WBAN_Number, Call_Sign, Call_Sign2, Year, Month, Day, Hour, Minute, Record_Length, Date, Timestamp, Interval, Call_Sign3, Zulu_Time,
                      Report_Modifier, Wind_Data, Wind_Direction, Wind_Speed, Gusts, Gust_Speed, Variable_Winds, Variable_Wind_Info, System_Maintenance_Reqd, Num_Fields]
            col_names = ["wban_number", "call_sign", "call_sign2", "year", "month", "day", "hour", "minute", "rec_length", "date", "timestamp", "interval", "call_sign3",
                         "zulu_time", "report_modifier", "wind_data", "wind_direction", "wind_speed", "gusts", "gust_speed", "variable_winds", "variable_wind_info", "sys_maint_reqd",
                         "num_fields"]
            data.append(record)
        except Exception as e:
            print("*** EXCEPTION IN CREATE NOAA DF, LINE: {}, ERROR: {}".format(line, e))
            continue

    sample_df = pd.DataFrame(data, columns = col_names)

#     # save Dataframe to file
#     parquet_file = "{}/noaa/{}.parquet".format(datafolder, fileName)
#     write(parquet_file, sample_df,compression='GZIP')

    return sample_df


# Get noaa data for the month
def getNOAAData(month, yr):
#     datafolder = "/Users/apaul2/Documents/_Common/capstone/Project/data"

    # Read station data from file that was stored earlier
#     unique_station_df = pd.read_parquet("{}/noaa/uniq_station_data.parquet".format(datafolder))
    try:
        try:
            s3 = s3fs.S3FileSystem()
            myopen = s3.open
            s3_resource = boto3.resource('s3')
            s3_resource.Object('midscapstone-whos-polluting-my-air', 'UtilFiles/uniq_station_data.parquet').load()
            pf=ParquetFile('midscapstone-whos-polluting-my-air/UtilFiles/uniq_station_data.parquet', open_with=myopen)
            unique_station_df=pf.to_pandas()
        except:
            raise CustomError("FILE ERROR: Unique Station Dataframe not found")

        # List of NOAA stations in the 35 < lat < 40 and  -125 < lon < -120 bounding box
        station_list = ['KAPC', 'KBLU', 'KCCR', 'KHWD', 'KLVK', 'KMAE', 'KMCE', 'KMOD', 'KMRY', 'KMYV', 'KNUQ', 'KOAK', 'KOVE', 'KPRB', 'KSAC', 'KSBP', 'KSCK',
                        'KSFO', 'KSJC', 'KSMF', 'KSNS', 'KSTS', 'KUKI', 'KVCB', 'KWVI']

        # Get NOAA data for desired stattions in a list
        lines = [] # an array of each read line
        bucket = "midscapstone-whos-polluting-my-air"
        s3 = boto3.client('s3')
    #     for station in station_list:
    #         filepath = "{3}/noaa/fmd_201910/64010{0}20{2}{1}".format(station, month, yr, datafolder)
    # #         filepath = "ftp://ftp.ncdc.noaa.gov/pub/data/asos-fivemin/6401-20{2}/64010{0}20{2}{1}.dat".format(station, month, yr)
    #         try:
    #             for line in pd.read_csv(filepath_or_buffer=filepath , encoding='utf-8', header=None, chunksize=1):
    #                 lines.append(line.iloc[0,0])
        for station in station_list:
            try:
                file_name = "AsosRaw/64010{0}20{2}{1}".format(station, '10', '19')
                obj = s3.get_object(Bucket= bucket, Key= file_name)
                df = pd.read_csv(obj['Body'], header=None)
                df.columns = ['dataval']
                for indx, line in df.iterrows():
                    lines.append(line['dataval'])
            except Exception as e:
                print("*** EXCEPTION IN GET NOAA DATA ITERROWS {}: {}".format(line, e))

        # Create noaa dataframe for the month
        print("*** BEFORE CREATE NOAA CALL ***")
        noaa_df = createNOAAdf(lines, '20' + yr + month)
        print("*** AFTER CREATE NOAA CALL ***")
    #     noaa_df = pd.read_parquet("{}/noaa/{}.parquet".format(datafolder, '20' + yr + month))

        # Drop rows where wind speed is not numeric
        noaa_df = noaa_df[noaa_df.wind_speed != 'T']
        merged_noaa_df = pd.merge(noaa_df, unique_station_df, on='wban_number')
        # Convert data type of numeric columns
        merged_noaa_df[['wind_speed','gust_speed','lat','lon']] = merged_noaa_df[['wind_speed','gust_speed','lat','lon']].apply(pd.to_numeric)

        # Get data for bounding box
        bay_noaa_df = merged_noaa_df[(merged_noaa_df.lat > 35) & (merged_noaa_df.lat < 40)
                                      & (merged_noaa_df.lon > -125) & (merged_noaa_df.lon < -120)]
        bay_noaa_df.reset_index(inplace=True, drop=True)
        bay_noaa_df['datetime'] = bay_noaa_df[['year', 'month','day','hour','minute']].apply(lambda x: int(''.join(x)), axis=1)

        return bay_noaa_df
    except Exception as e:
        print("*** EXCEPTION IN GET NOAA DATA *** {}".format(e))
        return None



# Get daily noaa data for the given month
def getDailyNOAA(bay_noaa_df, month, day, yr):
#     datafolder = "/Users/apaul2/Documents/_Common/capstone/Project/data"
    try:
        datestr = '{}/{:02}/{}'.format(month, day, yr)
        dly_noaa_df = bay_noaa_df[bay_noaa_df.date == datestr]
        dly_noaa_df.drop(['year', 'month','day','hour','minute','date','timestamp'], axis=1, inplace=True)
    #     parquet_file = "{0}/noaa/daily/asos_20{3}{1}{2:02}.parquet".format(datafolder, month, day, yr)
    #     write(parquet_file, dly_noaa_df,compression='GZIP')
        # Write to S3
        s3 = s3fs.S3FileSystem()
        myopen = s3.open
        write('midscapstone-whos-polluting-my-air/AsosDaily/asos_20{2}{0}{1:02}.parquet'.format(month, day, yr), dly_noaa_df, open_with=myopen)
        s3_resource = boto3.resource('s3')
        s3_resource.Object('midscapstone-whos-polluting-my-air', 'AsosDaily/asos_20{2}{0}{1:02}.parquet'.format(month, day, yr)).Acl().put(ACL='public-read')

    except Exception as e:
        print("*** EXCEPTION IN GET DAILY NOAA ***: {}".format(e))

    return dly_noaa_df
