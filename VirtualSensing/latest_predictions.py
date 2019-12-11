#!/home/ubuntu/miniconda3/envs/tf/bin/python


import pandas as pd
import datetime
import numpy as np
from copy import deepcopy
from collections import defaultdict
import ast
from itertools import product
from joblib import load

import sys
import os
sys.path.append("../")

# angshuman's functions
from HistoricalData.getData import getNearestEpaData, getNearestNoaaData, get_data

# custom helper functions
import modelUtils as mu

from scipy.signal import convolve2d
import argparse
import subprocess

import boto3


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=str, help="First Date to be exported", required = False)
    parser.add_argument("--end", type=str, help="First Date to be exported", required = False)
    args = parser.parse_args()

    if args.start is None:
        START_DATE = datetime.datetime.now() - datetime.timedelta(days=2)
        START_DATE = START_DATE.strftime("%Y/%m/%d")
    else:
        START_DATE = args.start
    
    if args.end is None:
        END_DATE = datetime.datetime.now() - datetime.timedelta(days=2)
        END_DATE = END_DATE.strftime("%Y/%m/%d")
    else:
        END_DATE = args.end

    MIN_LAT = 37.2781261
    MAX_LAT = 38.063446
    MIN_LON = -122.683496
    MAX_LON = -121.814281

    UP_LEFT = (MAX_LAT, MIN_LON)    
    UP_RIGHT = (MAX_LAT, MAX_LON)   
    DOWN_RIGHT = (MIN_LAT, MAX_LON) 
    DOWN_LEFT = (MIN_LAT, MIN_LON)    

    START_HOUR = '0'        
    END_HOUR = '24'
        
    ### load static features
    static_features = pd.read_csv("big_static_data.csv").drop(columns=['Unnamed: 0'])
    
    #### load Purple Air / temperature / humidity
    PA_df = get_data(UP_LEFT, UP_RIGHT, DOWN_RIGHT, DOWN_LEFT, 
                 START_DATE, END_DATE, START_HOUR, END_HOUR)
    
    boxes = pd.read_csv("../bigger_500m_grid.csv")
    sensor_locs = PA_df[['sensor_id', 'lat', 'lon']].drop_duplicates(subset=['sensor_id'])
    sensor_locs['xy_'] = sensor_locs.apply(lambda l: mu.get_coords(l,boxes), axis = 1)
    sensor_locs.set_index("sensor_id",drop = True, inplace = True)
    
    ## filter only on hourly readings; drop PA readings above 300
    filtered_PA_df = PA_df[(PA_df['2_5um'] < 300) & (PA_df.minute == '00')]\
                    .assign(ts_ = lambda df_: df_['created'].map(mu.parse_date),
                            xy_ = lambda df_: df_['sensor_id'].map(
                                lambda s: sensor_locs.loc[s,'xy_']),
                            time_space_id = lambda df_: df_.apply(mu.time_space, axis = 1)) \
                    .set_index('time_space_id', drop = True)[['2_5um', 'temperature', 
                                                              'humidity', 'sensor_id', 'ts_']]
    
    # create mapping to be used later
    humid_temp_lookup = {}

    for idx, row in filtered_PA_df.iterrows():
        if row['ts_'] in humid_temp_lookup:
            humid_temp_lookup[row['ts_']][row['sensor_id']] = {'temperature':row['temperature'],
                                                               'humidity':row['humidity']}
        else:
            humid_temp_lookup[row['ts_']] = {}
            humid_temp_lookup[row['ts_']][row['sensor_id']] = {'temperature':row['temperature'],
                                                               'humidity':row['humidity']}
    
    # get noaa data based on time and station
    noaa_lookup = mu.get_noaa_by_date(START_DATE, END_DATE)
    
    # get EPA data based on time and station
    epa_lookup = mu.get_epa_by_date(START_DATE, END_DATE)
    
    ##
    static_lookup = static_features[~static_features.in_water] \
                    .assign(xy_ = lambda df_: df_.apply(lambda l: f"{l.x}_{l.y}",axis = 1)) \
                    .set_index("xy_", drop = True)

    static_lookup['NN_list'] = static_lookup['NN_list'].map(ast.literal_eval)
    static_lookup_dict = static_lookup.to_dict()
    xy_lookup = static_lookup[['center_lat','center_lon']].T.to_dict()

    # every date string in our pre-defined period
    date_strs = mu.hourly_date_range(START_DATE, END_DATE)

    # every xy coordinate in static features
    xy_s = [(x,y) for x,y in zip(static_features['x'], static_features['y'])]

    # build dataframe from every xy coordinate at every timestep
    input_set = pd.DataFrame(product(date_strs,xy_s), columns = ['ts_', 'xy_']) \
                    .assign(dt_ = lambda df_: pd.to_datetime(df_['ts_'], unit='s'))
    assert input_set.shape[0] == len(date_strs) * len(xy_s)
    
    # assign each time-space location an EPA value
    input_set['epa_pm25_value'] = input_set \
        .apply(lambda l: mu.nearest_epa(l, static_lookup, epa_lookup), axis =1)
        
    # assign each time-space humidity and temperature
    # based on nearest PA sensor
    input_set['humidity'], input_set['temperature'] = \
        zip(*input_set.apply(lambda l: mu.nearest_humid_temp(l, static_lookup, humid_temp_lookup), axis = 1))
    
    ### fill in temp, humid by looking at the avg for that timestamp
    # avg temp, humid by timestamp for filling in null values
    avg_hum_temp = input_set.groupby('ts_').agg({'humidity':'mean', 
                                                 'temperature':'mean',
                                                 'epa_pm25_value':'mean'})

    input_set['humidity'] = input_set \
        .apply(lambda l: mu.fill_in_avgs(l, 'humidity',avg_hum_temp), axis = 1)
    input_set['temperature'] = input_set \
        .apply(lambda l: mu.fill_in_avgs(l, 'temperature',avg_hum_temp), axis = 1)
    input_set['epa_pm25_value'] = input_set \
        .apply(lambda l: mu.fill_in_avgs(l, 'epa_pm25_value',avg_hum_temp), axis = 1)

    
    # get wind for every time-space step
    input_set['wind_x'], input_set['wind_y'] = \
        zip(*input_set.apply(lambda l: mu.get_wind(l, static_lookup_dict, noaa_lookup), axis = 1))
    
    avg_wind = input_set.fillna(0).groupby('ts_').agg({'wind_x':'mean', 'wind_y':'mean'})
    input_set['wind_x'] = input_set.apply(lambda l: mu.fill_in_avgs(l, 'wind_x', avg_wind), axis = 1)
    input_set['wind_y'] = input_set.apply(lambda l: mu.fill_in_avgs(l, 'wind_y', avg_wind), axis = 1)
    
    # ndvi, elevation
    input_set['ndvi'] = input_set.xy_.map(lambda xy: static_lookup['ndvi'][f"{xy[0]}_{xy[1]}"])
    input_set['elevation'] = input_set.xy_.map(lambda xy: static_lookup['elevation'][f"{xy[0]}_{xy[1]}"])
    
    # drop any remaining na
    input_set.dropna(inplace = True)
    
    # time to get neighbors for every space-time location
    neighbor_lookup = defaultdict(list)
    _ = filtered_PA_df.apply(lambda x: neighbor_lookup[x.name].append(x['2_5um']), axis = 1)
    
    input_set.dropna(inplace = True)
    input_neighbors = np.stack(
        input_set.apply(
            lambda x: mu.get_neighbors_space_time(x, neighbor_lookup),axis = 1
        ).values
    )
    
    cols = ['epa_pm25_value', 'humidity', 'temperature', 'wind_x', 'wind_y', 'ndvi', 'elevation']
    
    ########## really put everything together
    final_input = np.concatenate((input_set[cols].to_numpy(), 
                                  input_neighbors),
                                 axis = 1)
    
    ## load model
    rf = load("RF_final.joblib")
    preds = rf.predict(final_input)
    input_set['preds'] = preds
    
    for n in range(25):
        input_set[f'neighbor_{n}'] = input_neighbors[:,n]
    
    input_set['n_neighbors'] = input_set.apply(mu.n_neighbors, axis = 1)
    input_set['lonely_factor'] = 1 / (input_set['n_neighbors'] + 1)
    
    sd = START_DATE.replace("/", "")
    #input_set.to_csv(f"{sd}_all_preds.csv", index = False)

    ## load history from file
    if os.path.isfile("seven_day_predictions.csv"):
        master_list = pd.read_csv("seven_day_predictions.csv", parse_dates=['dt_']) \
            .assign(xy_ = lambda df_: df_.xy_.map(ast.literal_eval)) \
            .append(input_set) \
            .drop_duplicates(subset=['ts_','xy_'])
        del input_set
    else:
        master_list = input_set
    
    # drop observations older than 7 days
    dropoff_date = datetime.datetime.now() - datetime.timedelta(days=8)
    dropoff_date = datetime.datetime.combine(dropoff_date.date(), 
                                             datetime.datetime.min.time())
    old_ = master_list[master_list['dt_'] <= dropoff_date].index
    master_list.drop(old_, inplace = True)

    master_list.to_csv("seven_day_predictions.csv", index = False)
    
    avg_preds = master_list.groupby(['xy_']).agg({'preds':'mean',
                                                'n_neighbors':'mean',
                                                'lonely_factor':'mean'}).reset_index() \
        .assign(x = lambda df_: df_['xy_'].map(lambda x: x[0]),
                y = lambda df_: df_['xy_'].map(lambda x: x[1]))
    
    ## add higher order lonely?
    max_x = avg_preds.x.max()
    max_y = avg_preds.y.max()
    neighbor_map = np.zeros([max_y + 1, max_x + 1])
    # map n_neighbors to grid
    neighbor_map[avg_preds.y.values, avg_preds.x.values] =  avg_preds.n_neighbors.values
    c = 1 / (1 + np.array([(i**2 + j**2)**0.5 for i in range(-5,6) for j in range(-5,6)]).reshape(11,11))
    cmap = convolve2d(neighbor_map,c, mode = 'same', boundary = 'fill')
    
    avg_preds['robust_lonely'] = avg_preds.apply(lambda l: cmap[l.y, l.x], axis = 1)
    
    
    avg_preds['lat'] = avg_preds.xy_.map(lambda p: static_lookup.loc[f"{p[0]}_{p[1]}",'center_lat'] )
    avg_preds['lon'] = avg_preds.xy_.map(lambda p: static_lookup.loc[f"{p[0]}_{p[1]}",'center_lon'] )
    
    avg_preds.to_csv("latest_avg.csv", index = False)

    
    # send to s3
#     subprocess.run(f"/home/ubuntu/miniconda3/bin/aws s3 cp latest_avg.csv s3://capstone-air-pollution/model_output/latest_avg.csv", 
#                    shell = True)

    s3 = boto3.client('s3')
    s3.upload_file(
        'latest_avg.csv', 'capstone-air-pollution', 'model_output/latest_avg.csv',
        ExtraArgs={'ACL': 'public-read'}
    )

    
main()