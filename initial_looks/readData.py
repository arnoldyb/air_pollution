#!/usr/bin/env python3

import os
import pandas as pd
import datetime, time
from fastparquet import write

import warnings
warnings.filterwarnings('ignore')

while True:
    try:
        now = datetime.datetime.now()
        datetimeval = datetime.datetime.now().strftime("%Y%m%d%H%M")
        parquet_file = "data.parquet"
        if now.minute % 5 == 4 and now.second == 56:
            data_df = pd.read_json(path_or_buf="https://www.purpleair.com/json")
            write(parquet_file, data_df,compression='GZIP')
            os.system("aws s3 cp data.parquet s3://midscapstone-whos-polluting-my-air/PurpleAir/{}.parquet".format(datetimeval))
            time.sleep(120)
    except:
        pass
