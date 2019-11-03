import pandas as pd
import numpy as np 
import datetime as dt
import warnings
import os
from copy import deepcopy
from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsRegressor
from sklearn.metrics import mean_squared_error, r2_score
from joblib import dump, load
import matplotlib.pyplot as plt
import sys
sys.path.append("../HistoricalData/")
from getData import get_data

import warnings
warnings.filterwarnings('ignore')


def import_data():
    # constants
    from getData import get_data
    UP_LEFT = (38.008050, -122.536985)    
    UP_RIGHT = (38.008050, -122.186437)   
    DOWN_RIGHT = (37.701933, -122.186437) 
    DOWN_LEFT = (37.701933, -122.536985)  
    START_DATE = '2018/10/01' 
    END_DATE = '2019/09/02'   
    START_HOUR = '0'        
    END_HOUR = '24'
    
    # load data into dataframe
    data_df = get_data(UP_LEFT, UP_RIGHT, DOWN_RIGHT, DOWN_LEFT, START_DATE, END_DATE, START_HOUR, END_HOUR, 'Monthly')
    data_df['created'] =  pd.to_datetime(data_df['created'], format='%Y%m%d%H%M')
    
    # remove outliers naively
    data_df = data_df[data_df['2_5um'] < np.percentile(data_df['2_5um'], 99.5)]
    
    return data_df


def split_data(columns_to_keep, stratify=True):

    if stratify:
        # stratifying by sensor_id to avoid data_leakage
        id_train_dev, id_test = train_test_split(data_df.sensor_id.unique(), test_size=.2)
        id_train, id_dev = train_test_split(id_train_dev, test_size=.125)

        data_df_train = data_df[data_df.sensor_id.isin(id_train)]
        data_df_dev = data_df[data_df.sensor_id.isin(id_dev)]
        data_df_test = data_df[data_df.sensor_id.isin(id_test)]
        
        X_train = data_df_train[columns_to_keep]
        X_dev = data_df_dev[columns_to_keep]
        X_test = data_df_test[columns_to_keep]
        y_train = data_df_train['2_5um']
        y_dev = data_df_dev['2_5um']
        y_test = data_df_test['2_5um']
    else:
        # standard train-dev-test split
        X_data_df = data_df[columns_to_keep]
        y_data_df = data_df['2_5um']

        X_train_and_dev, X_test, y_train_and_dev, y_test = train_test_split(X_data_df, y_data_df, test_size=0.20, random_state=42)
        X_train, X_dev, y_train, y_dev = train_test_split(X_train_and_dev, y_train_and_dev, test_size=0.125, random_state=42)
        
    return (X_train, X_dev, X_test, y_train, y_dev, y_test)


def build_kNN_model(data_df):
    # turn datetime into an integer so that a model can fit on it
    data_df['time_delta'] = (data_df['created'] - pd.Timestamp('2019-09-01 00:00:00')) / np.timedelta64(1, 'm')
    
    # these colunms are the only ones I'm using in kNN
    columns_to_keep = ['time_delta', 'lat', 'lon']
    
    X_train, X_dev, X_test, y_train, y_dev, y_test = split_data(columns_to_keep)
    
    # fit the model
    regr = KNeighborsRegressor(n_neighbors=2**5)
    regr.fit(X_train, y_train)
    
    return (regr, X_train, X_dev, X_test, y_train, y_dev, y_test)
    
    
def test_model(regr, X_dev, y_dev):
    # make predictions
    y_pred = regr.predict(X_dev)

    # Print the mean squared error
    print("Mean squared error: %.2f"
          % mean_squared_error(y_dev, y_pred))
    # Print the explained variance score: 1 is perfect prediction, 0 is equivalent to guessing the expected value each time
    print('Variance score: %.2f' % r2_score(y_dev, y_pred))
    
    
def save_model(regr, name):
    dump(regr, os.path.join('models', name+'_model.joblib'))


if __name__ == '__main__':
    data_df = import_data()
    regr, X_train, X_dev, X_test, y_train, y_dev, y_test = build_kNN_model(data_df)
    test_model(regr, X_dev, y_dev)
    save_model(regr, 'kNN')
    
    