"""
Functions and classes that help prepare and shape data for feeding into model
"""

import pandas as pd
import numpy as np
import datetime
import boto3
import s3fs
from fastparquet import ParquetFile
import re
from math import sin, radians, cos
from collections import defaultdict

# helper functions
parse_date = lambda datestr: int(datetime.datetime.strptime(str(datestr), "%Y%m%d%H%M").timestamp())
format_name = lambda namestr: re.sub(r"[ -]{1,}", "_", namestr).lower()


def get_coords(line, boxes):
    box = boxes[(boxes.min_lat < line.lat) & (boxes.max_lat > line.lat) & 
                (boxes.min_lon < line.lon) & (boxes.max_lon > line.lon)].reset_index(drop=True)
    assert box.shape[0] == 1
    return str(box.loc[0, 'x']) + "," + str(box.loc[0, 'y']) # x,y

def time_space(line):
    """ 
    Args:
    a line from a pandas df that must contain:
      -ts_ (int) unix timestamp
      -xy_ (str) str representing x,y coordinates 
    takes x, y, created at
    
    returns string of form TTTT_x_y    
    """
    x,y = line.xy_.split(",")
    return f"{line.ts_}_{x}_{y}"


def get_epa_by_date(start_date, end_date, hourly = True):
    """
    Gets EPA data by whole day at a time
    Args:
      - start_date, end_date (str, format "YYYY/MM/DD" although pandas is pretty smart about picking that stuff up)
      - hourly (boolean, default = True) whether only values on the hour (or interpolated values in between hours)
        are returned
    """
    
    date_range = pd.date_range(start = start_date, end=end_date, freq='D')
    df_list = []
    
    # Get File from s3
    try:
        for one_day in date_range:
            filename = 'epa_' + one_day.strftime("%Y%m%d")
            folder='EpaDaily'
            
            s3 = s3fs.S3FileSystem()
            myopen = s3.open
            s3_resource = boto3.resource('s3')
            s3_resource.Object('midscapstone-whos-polluting-my-air', '{}/{}.parquet'.format(folder, filename)).load()
            pf=ParquetFile('midscapstone-whos-polluting-my-air/{}/{}.parquet'.format(folder, filename), open_with=myopen)
            df=pf.to_pandas()
            df.reset_index(inplace=True, drop=True)
            if hourly:
                hourly_filter = np.where(df.created % 100 == 0, True, False)
                df_list.append(df[hourly_filter])
            else:
                df_list.append(df)
            
    except Exception as e:
        print(f"Processing {folder}/{filename} failed")
        print(e)
        
    all_df = pd.concat(df_list, ignore_index = True) \
        .assign(
            ts_ = lambda da: da['created'].map(parse_date),
            site_id = lambda da: da.apply(lambda l: str(l['ts_']) + "_" + format_name(l['site_name']), axis = 1)
        ) \
        .set_index("site_id", drop = True)['epa_pm25_value']
    
    # create lookup dictionary based site id and value
    lookup = {}
    for site_id, val in all_df.iteritems():
        lookup[site_id] = val
    
    return lookup


def hourly_date_range(start_date, end_date):
    """
    Args
      - start_date, end_date (str, format "YYYY/MM/DD")
    Returns:
      - list of date ranges that starts at midnight of start_date and ends at one hour before midnight on end_date
        return list is of format YYYYMMDDHHMM for compatibility with other dataframes

    """
    actual_end_date = datetime.datetime.strptime(end_date, "%Y/%m/%d") + datetime.timedelta(days = 1)
    
    # got up to -1 to leave out midnight on the next day
    date_range = pd.date_range(start = start_date, end=actual_end_date, freq='H')[:-1]
    return [int(d.timestamp()) for d in date_range]
    

def get_noaa_by_date(start_date, end_date, hourly = True, id_field = "call_sign"):
    """
    Gets NOAA data by whole day at a time
    Args:
      - start_date, end_date (str, format "YYYY/MM/DD" although pandas is pretty smart about picking that stuff up)
      - hourly (boolean, default = True) whether only values on the hour (or interpolated values in between hours)
        are returned
      - id_field (str, default = call_sign) field to use as unique identifier for each station
      
    Returns:
      - pandas DataFrame with a unique time-location identifier for each row and columns for wind_x and wind_y
    """
    
    date_range = pd.date_range(start = start_date, end=end_date, freq='D')
    df_list = []
    
    # Get File from s3
    try:
        for one_day in date_range:
            filename = 'asos_' + one_day.strftime("%Y%m%d")
            folder='AsosDaily'
            
            s3 = s3fs.S3FileSystem()
            myopen = s3.open
            s3_resource = boto3.resource('s3')
            s3_resource.Object('midscapstone-whos-polluting-my-air', '{}/{}.parquet'.format(folder, filename)).load()
            pf=ParquetFile('midscapstone-whos-polluting-my-air/{}/{}.parquet'.format(folder, filename), open_with=myopen)
            df=pf.to_pandas()
            df.reset_index(inplace=True, drop=True)
            if hourly:
                hourly_filter = np.where(df.datetime % 100 == 0, True, False)
                df_list.append(df[hourly_filter])
            else:
                df_list.append(df)
            
    except Exception as e:
        print(f"Processing {folder}/{filename} failed")
        print(e)
        
    all_df = pd.concat(df_list, ignore_index = True)
    all_df['wind_direction'] = all_df['wind_direction'].map(lambda x: x if x is not None else 0)

    # fill in no wind times
    all_df['wind_speed'].fillna(0, inplace = True)
    all_df['wind_direction'].replace('', 0, inplace = True)

    # change VRB to 0
    vrb = all_df[all_df.wind_direction == "VRB"].index
    all_df.loc[vrb, 'wind_direction'] = 0
    all_df.loc[vrb, 'wind_speed'] = 0

    all_df['wind_x'] = all_df.apply(lambda x: x.wind_speed * cos(radians(int(x.wind_direction))), axis = 1)
    all_df['wind_y'] = all_df.apply(lambda x: x.wind_speed * sin(radians(int(x.wind_direction))), axis = 1)

    all_df['ts_'] = all_df['datetime'].map(parse_date)
    all_df['site_id'] = all_df.apply(lambda l: str(l['ts_']) + "_" +  format_name(l[id_field]), axis = 1)
    
    lookup = defaultdict(list)
    _ = all_df.apply(lambda l: lookup[l.site_id].extend([l.wind_x, l.wind_y]), axis = 1)
    
    return lookup


def nearest_epa(line, static_lookup, epa_lookup):
    
    ## will need to pass in static lookup/epa_lookup
    nearest_epa_station = format_name(static_lookup.loc[f"{line.xy_[0]}_{line.xy_[1]}", 'closest_epa'])
    if f"{line.ts_}_{nearest_epa_station}" in epa_lookup:
        return epa_lookup[f"{line.ts_}_{nearest_epa_station}"]
    else:
        return np.nan

def nearest_humid_temp(line, static_lookup, humid_temp_lookup):
    NN_list = static_lookup['NN_list'][f"{line.xy_[0]}_{line.xy_[1]}"]
    
    for NN in NN_list:
        if str(NN) in humid_temp_lookup[line.ts_]:
            humidity = humid_temp_lookup[line.ts_][str(NN)]['humidity']
            temperature = humid_temp_lookup[line.ts_][str(NN)]['temperature']
            break
    else:
        humidity = np.nan
        temperature = np.nan
    
    return humidity, temperature


def fill_in_avgs(line, val, source_df):
    """if a particular observation is missing a value (e.g., humidity, temperature)
    fill in with the average at that timestamp"""
    
    if np.isnan(line[val]):
        return source_df.loc[line.ts_, val]
    else:
        return line[val]
    
    
def get_wind(line, static_lookup_dict, noaa_lookup):
    nearest_noaa = static_lookup_dict['closest_NOAA'][f"{line.xy_[0]}_{line.xy_[1]}"]
    
    if len(noaa_lookup[f"{line.ts_}_{nearest_noaa.lower()}"]) != 2:
        return np.nan, np.nan
    
    return noaa_lookup[f"{line.ts_}_{nearest_noaa.lower()}"]

def get_neighbors_space_time(line, neighbor_lookup):
    """
    Inputs: single observation, a dict of form {timestamp_x_y:[PA values]}
    Outputs: vector of length 25 corresponding to surrounding neighbor observations
    """   
    t = line.ts_
    x = line.xy_[0]
    y = line.xy_[1]
    neighbors = np.zeros((25))
    
    c = 0
    for i in range(-2,3):
        for j in range(-2,3):
            n = neighbor_lookup[f"{t}_{x+i}_{y+j}"] 
            
            if n:
                neighbors[c] = np.mean(n)
            c += 1
    
    return neighbors


def n_neighbors(line):
    """
    For a given grid square, how many actual sensors are in its immediate vicinity (5x5 grid)
    """
    c = 0
    for n in range(25):
        if line[f"neighbor_{n}"] > 0:
            c +=1
    return c