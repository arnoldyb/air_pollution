#/usr/bin/env Python3

import sys
import os
import re
import pandas as pd
import s3fs
import boto3
import random, json
from fastparquet import ParquetFile

from flask import Flask, render_template, request, redirect, Response, url_for, jsonify
from flask_jsglue import JSGlue

app = Flask(__name__)
JSGlue(app)

@app.route('/')
def output():
        # serve index template
        return render_template('index.html')

@app.route("/update")
def update():
    """Find lat lon for desired number of sensors in the bounding box."""

    # ensure parameters are present
    if not request.args.get("sw"):
        raise RuntimeError("missing sw")
    if not request.args.get("ne"):
        raise RuntimeError("missing ne")


    # ensure parameters are in lat,lng format
    if not re.search("^-?\d+(?:\.\d+)?,-?\d+(?:\.\d+)?$", request.args.get("sw")):
        raise RuntimeError("invalid sw")
    if not re.search("^-?\d+(?:\.\d+)?,-?\d+(?:\.\d+)?$", request.args.get("ne")):
        raise RuntimeError("invalid ne")

    # Get desired number of sensors
    if not request.args.get("q"):
        q=0
    else:
        q = int(request.args.get("q"))

    # explode southwest corner into two variables
    (sw_lat, sw_lng) = [float(s) for s in request.args.get("sw").split(",")]

    # explode northeast corner into two variables
    (ne_lat, ne_lng) = [float(s) for s in request.args.get("ne").split(",")]

    loc_lst = []
    try:
        s3 = s3fs.S3FileSystem()
        myopen = s3.open
        s3_resource = boto3.resource('s3')
        s3_resource.Object('midscapstone-whos-polluting-my-air', 'UtilFiles/dummylatlon.parquet').load()
        pf=ParquetFile('midscapstone-whos-polluting-my-air/UtilFiles/dummylatlon.parquet', open_with=myopen)
        df=pf.to_pandas()

        df[['lat','lon']] = df[['lat','lon']].apply(pd.to_numeric)

        df_filtered = df[(df.lat > sw_lat) & (df.lat < ne_lat) & (df.lon > sw_lng) & (df.lon < ne_lng)]
        df_filtered.reset_index(inplace=True, drop=True)
        df_sorted = df_filtered.sort_values(by='value', ascending=False)
        top_lat = df_sorted.head(q).lat.tolist()
        top_lon = df_sorted.head(q).lon.tolist()
        loc_lst = list(zip(top_lat, top_lon))
    except Exception as e:
        print("*** EXCEPTION IN GET ADDRESS: {}".format(e))
    return jsonify(loc_lst)

if __name__ == '__main__':
    app.run("0.0.0.0", "8083")

