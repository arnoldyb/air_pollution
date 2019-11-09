#/usr/bin/env Python3

import sys

from flask import Flask, render_template, request, redirect, Response, url_for, jsonify
import random, json

app = Flask(__name__)

@app.route('/')
def output():
        # serve index template
        return render_template('index.html')

@app.route("/update")
def update():
    """Find up to 10 places within view."""

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

    print(sw_lat, sw_lng, ne_lat, ne_lng)

if __name__ == '__main__':
    app.run()
    # for running in ec2
    # app.run("0.0.0.0", "80")
