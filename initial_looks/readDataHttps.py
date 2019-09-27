#!/usr/bin/env python3

import os
import pandas as pd
import datetime, time
from fastparquet import write
import urllib3
import json

import warnings
warnings.filterwarnings('ignore')

https = urllib3.PoolManager()

while True:
    try:
        now = datetime.datetime.now()
        datetimeval = datetime.datetime.now().strftime("%Y%m%d%H%M")
        parquet_file = "data.parquet"
        if now.minute % 5 == 4 and now.second == 56:
            r = https.request('GET',"https://www.purpleair.com/json")
            if r.status != 200:
                time.sleep(240)
                continue
            j = json.loads(r.data.decode('utf-8'))
            data_df = pd.DataFrame(j)
            write(parquet_file, data_df,compression='GZIP')
            os.system("aws s3 cp data.parquet s3://midscapstone-whos-polluting-my-air/PurpleAir/{}.parquet".format(datetimeval))
            time.sleep(120)
    except:
        pass
