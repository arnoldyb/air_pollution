#/usr/bin/env Python3

import sys
import os
import re
import pandas as pd
import numpy as np

from flask import Flask, render_template, request, redirect, Response, url_for, jsonify
from flask_jsglue import JSGlue
import random, json

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

    # explode southwest corner into two variables
    (sw_lat, sw_lng) = [float(s) for s in request.args.get("sw").split(",")]

    # explode northeast corner into two variables
    (ne_lat, ne_lng) = [float(s) for s in request.args.get("ne").split(",")]

    top_lat, top_lon = get_top_locations(1, sw_lat, sw_lng, ne_lat, ne_lng)
    lst=[(top_lat, top_lon)]
    return jsonify(lst)

def get_top_locations(n, sw_lat, sw_lng, ne_lat, ne_lng):
    # filtering assumes northern and western hemisphere for now, could be generalized with some extra work
    df_filtered = df[(sw_lat < df.lat) &
                     (df.lat < ne_lat) &
                     (ne_lng > df.lon) &
                     (df.lon > sw_lng)]
    df_sorted = df_filtered.sort_values(by='value', ascending=False)
    top_lat = df_sorted.head(n).lat.tolist()
    top_lon = df_sorted.head(n).lon.tolist()
    return (top_lat, top_lon)

def generate_fake_data():
    # this function will be removed once we are pulling cached results from s3
    random.seed(42)
    data = {'lat': np.random.uniform(37.701933, 38.008050, 50),
            'lon': np.random.uniform(-122.536985, -122.186437, 50),
            'value': np.random.uniform(0, 10, 50)}
    df = pd.DataFrame(data)
    return df

if __name__ == '__main__':
    df = generate_fake_data()
    app.run()
    # for running in ec2
    # app.run("0.0.0.0", "80")
