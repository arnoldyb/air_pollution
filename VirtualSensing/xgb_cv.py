#!/home/ubuntu/miniconda3/envs/tf/bin/python

"""
used for testing various parameters on xgboost
"""

import glob
import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.model_selection import GridSearchCV
import warnings
warnings.filterwarnings("ignore")


def main():
    train = pd.read_parquet("final_train_2.parquet").sample(n = 200000, random_state = 55)
    
    neighbor_cols = [f'neighbor_{n}' for n in range(25)]
    input_cols = ['imputed_epa_pm25_value', 'imputed_hum', 
                  'imputed_temperature', 'wind_x','wind_y', 
                  'ndvi', 'elevation'] + neighbor_cols
    
    xg_reg = xgb.XGBRegressor(objective ='reg:squarederror', random_state = 23)
   
    param_grid = {'n_estimators':[10,100],
                  'lambda':[0.1,0.5,1],
                  'alpha':[0.1,0.5,1],
                  'subsample':[0.25,0.5,1],
                  'base_score':[0.5, train['2_5um'].mean()]}
    
    gcv = GridSearchCV(xg_reg, param_grid, scoring =['neg_mean_squared_error', 'r2'],
                       refit = False, cv = 3)
    gcv.fit(train[input_cols], train['2_5um'])
    pd.DataFrame(gcv.cv_results_).to_csv("xgb_gcv_results1209.csv", index = False)
    #dump(gcv, "cv_rf.joblib")
    
main()