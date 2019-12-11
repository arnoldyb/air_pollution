#!/home/ubuntu/miniconda3/envs/tf/bin/python

"""This particular cross validation script tests max_features and max_samples"""

import glob
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import GridSearchCV
from joblib import dump

def main():
    train = pd.read_parquet("final_train_2.parquet").sample(n = 200000, random_state = 55)
    
    neighbor_cols = [f'neighbor_{n}' for n in range(25)]
    input_cols = ['imputed_epa_pm25_value', 'imputed_hum', 
                  'imputed_temperature', 'wind_x','wind_y', 
                  'ndvi', 'elevation'] + neighbor_cols
    
    rf = RandomForestRegressor(random_state = 23)
    param_grid = {'n_estimators':[100,200], 'max_features':['auto', 'sqrt'], 'max_samples':[None, 0.1, 0.25,0.5]}
    gcv = GridSearchCV(rf, param_grid, scoring =['neg_mean_squared_error', 'neg_mean_squared_log_error', 'r2'],
                       refit = False, cv = 3)
    gcv.fit(train[input_cols], train['2_5um'])
    pd.DataFrame(gcv.cv_results_).to_csv("gcv_results1209.csv", index = False)
    #dump(gcv, "cv_rf.joblib")
    
main()