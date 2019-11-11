#/usr/bin/env Python3

import sys
import os
import re

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

    #dummy calcs. need to replace with actual logic
    dummylat = (sw_lat + ne_lat)/2
    dummylng = (sw_lng + ne_lng)/2

    lst=[(dummylat, dummylng)]
    return jsonify(lst)

if __name__ == '__main__':
    app.run()
    # for running in ec2
    # app.run("0.0.0.0", "80")
