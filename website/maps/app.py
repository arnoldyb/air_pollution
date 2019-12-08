#/usr/bin/env Python3

import re
import pandas as pd
import numpy as np
import s3fs
import boto3
import botocore
from datetime import datetime, timedelta
from fastparquet import ParquetFile
from sklearn import preprocessing
from flask import Flask, render_template, request, redirect, Response, url_for, jsonify
from flask_jsglue import JSGlue

app = Flask(__name__)
JSGlue(app)


def distance(point1, point2):
    '''
    take two lat/lon pairs and calculate the Euclidean distance between them. There is probably a function to do this,
    but I didn't have internet at the time
    :param point1: tuple
    :param point2: tuple
    :return: distance: float
    '''
    return np.sqrt((point1[0]-point2[0])**2 + (point1[1]-point2[1])**2)


@app.route('/')
def output():
    # define bounding box
    min_lat = 37.701933
    max_lat = 38.008050
    max_lon = -122.186437
    min_lon = -122.536985

    # load existing sensor network
    try:
        # load from local instance
        print("Looking for sensor file locally.", flush=True)
        df = pd.read_parquet("./pasensors.parquet")
    except:
        # otherwise go to S3. Try today's date first, then iteratively look backward day by day
        print("No local sensor file. Searching S3.", flush=True)
        file_date = datetime.today()
        while True:
            try:
                filename = file_date.strftime('%Y%m%d') + ".parquet"
                print("Looking for file " + filename, flush=True)
                s3 = s3fs.S3FileSystem()
                myopen = s3.open
                s3_resource = boto3.resource('s3')
                s3_resource.Object('midscapstone-whos-polluting-my-air', 'PurpleAirDaily/{}'.format(filename)).load()
                pf = ParquetFile('midscapstone-whos-polluting-my-air/PurpleAirDaily/{}'.format(filename), open_with=myopen)
                df = pf.to_pandas()
                break
            except botocore.exceptions.ClientError:
                file_date = file_date - timedelta(days=1)
    global existing_lst_full
    unique_sensor_df = df.drop_duplicates(subset="sensor_id")
    unique_sensor_df = unique_sensor_df[(unique_sensor_df.lat > min_lat) & (unique_sensor_df.lat < max_lat) &
                                 (unique_sensor_df.lon > min_lon) & (unique_sensor_df.lon < max_lon)]
    unique_sensor_df.reset_index(inplace=True, drop=True)
    existing_lat = unique_sensor_df.lat.tolist()
    existing_lon = unique_sensor_df.lon.tolist()
    existing_name = unique_sensor_df.sensor_name.tolist()
    existing_lst_full = list(zip(existing_lat, existing_lon, existing_name))

    # load polluters
    global polluter_lst_full
    try:
        print("Looking for polluter file locally.", flush=True)
        polluter_df = pd.read_csv("./polluters.csv")
    except:
        print("No local polluter file. Searching S3.", flush=True)
        bucket = "midscapstone-whos-polluting-my-air"
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=bucket, Key='UtilFiles/polluters.csv')
        polluter_df = pd.read_csv(obj['Body'])
    polluter_df = polluter_df[(polluter_df.Lat > min_lat) & (polluter_df.Lat < max_lat) &
                                 (polluter_df.Lon > min_lon) & (polluter_df.Lon < max_lon)]
    polluter_df.reset_index(inplace=True, drop=True)
    polluter_lat = polluter_df.Lat.tolist()
    polluter_lon = polluter_df.Lon.tolist()
    polluter_name = polluter_df.Name.tolist()
    polluter_street = polluter_df.Street.tolist()
    polluter_city = polluter_df.City.tolist()
    polluter_pm = polluter_df.PM.tolist()
    polluter_lst_full = list(zip(polluter_lat, polluter_lon, polluter_name, polluter_street, polluter_city, polluter_pm))

    # Load predictions
    global df_predictions
    try:
        print("Looking for prediction file locally.", flush=True)
        df_predictions = pd.read_csv("./preds_loneliness.csv")
    except:
        print("No local prediction file. Searching S3.", flush=True)
        bucket = "midscapstone-whos-polluting-my-air"
        s3 = boto3.client('s3')
        # obj = s3.get_object(Bucket= bucket, Key= 'UtilFiles/preds.csv')
        obj = s3.get_object(Bucket=bucket, Key='UtilFiles/preds_loneliness.csv')
        df_predictions = pd.read_csv(obj['Body'])
    df_predictions.drop(['xy_'], axis=1, inplace=True)

    # normalize predictions
    min_max_scaler = preprocessing.MinMaxScaler()
    preds = df_predictions[['preds']].values.astype(float)
    preds_normalized = min_max_scaler.fit_transform(preds)
    df_predictions['preds_normalized'] = preds_normalized

    # normalize loneliness
    lonely = df_predictions[['lonely_factor']].values.astype(float)
    lonely_factor_normalized = min_max_scaler.fit_transform(lonely)
    df_predictions['lonely_factor_normalized'] = lonely_factor_normalized

    # create combined score
    loneliness_weight = 1
    df_predictions['score'] = loneliness_weight * df_predictions['preds_normalized'] + \
                              (1 - loneliness_weight) * df_predictions['lonely_factor_normalized']

    # prepare heatmap
    global heatmap_lst_full
    preds_log = np.log(list(preds))
    df_predictions['preds_log'] = preds_log
    df_predictions = df_predictions[(df_predictions.lat > min_lat) & (df_predictions.lat < max_lat) &
                                 (df_predictions.lon > min_lon) & (df_predictions.lon < max_lon)]
    heatmap_lat = df_predictions.lat.tolist()
    heatmap_lon = df_predictions.lon.tolist()
    heatmap_pred = df_predictions.preds_log.tolist()
    heatmap_lst_full = list(zip(heatmap_lat, heatmap_lon, heatmap_pred))

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

    # filter by current bounding box
    df_filtered = df_predictions[(df_predictions.lat > sw_lat) & (df_predictions.lat < ne_lat) &
                                 (df_predictions.lon > sw_lng) & (df_predictions.lon < ne_lng)]
    df_filtered.reset_index(inplace=True, drop=True)

    # sort
    df_sorted = df_filtered.sort_values(by='score', ascending=False)
    df_sorted.reset_index(inplace=True, drop=True)

    # if we don't have enough grid points, just return them all to the user
    if len(df_sorted.index) <= q:
        top_lat = df_sorted.lat.tolist()
        top_lon = df_sorted.lon.tolist()
        loc_lst = list(zip(top_lat, top_lon))
    else:
        # select top candidates
        force_spacing = True
        if force_spacing:
            # enforce a minimum spacing as locations are chosen one-by-one and subsequent recommendations don't update
            # based on earlier recommendations. .02 is an aesthetically pleasing minimum spacing at the default zoom
            # level. We could consider making defining it based on SME input for how close they would realistically
            # place any two sensors.

            min_spacing = .02
            candidate_index = 0
            current_choices = pd.DataFrame(columns=df_sorted.columns)
            last_index = len(df_sorted.index)
            # until we have enough selections
            while len(current_choices.index) < q:
                if candidate_index > last_index - 1:
                    print("ran out of candidates, decreasing spacing", flush=True)
                    min_spacing /= 2
                    candidate_index = min(df_sorted.index)
                    continue
                # get candidate as lat lon tuple
                candidate = (df_sorted.lat.loc[candidate_index], df_sorted.lon.loc[candidate_index])
                append = True
                for already_chosen_index in current_choices.index:
                    # compare to already chosen locations as lat lon tuple
                    already_chosen = current_choices.lat.loc[already_chosen_index],\
                                     current_choices.lon.loc[already_chosen_index]
                    if distance(candidate, already_chosen) < min_spacing:
                        # not a good candidate, too close to an existing sensor
                        append = False
                        break
                if append:
                    # a valid candidate, add to recommendations and drop from available choices
                    current_choices = current_choices.append(df_sorted.loc[candidate_index])
                    df_sorted.drop(index=candidate_index)
                candidate_index += 1

            top_lat = current_choices.head(q).lat.tolist()
            top_lon = current_choices.head(q).lon.tolist()
            loc_lst = list(zip(top_lat, top_lon))
        else:
            # if no minimum spacing constraint, just choose top n regardless of spacing
            top_lat = df_sorted.head(q).lat.tolist()
            top_lon = df_sorted.head(q).lon.tolist()
            loc_lst = list(zip(top_lat, top_lon))

    location_json = {
        "recommendations": loc_lst
    }
    return jsonify(location_json)

@app.route("/getstaticmarkers")
def getstaticmarkers():
    """Find lat lon for existing sensors and polluters."""

    existing_lst = existing_lst_full
    polluter_lst = polluter_lst_full
    heatmap_lst = heatmap_lst_full

    marker_json = {
        "existing": existing_lst,
        "polluters": polluter_lst,
        "heatmappy": heatmap_lst
    }
    return jsonify(marker_json)

if __name__ == '__main__':
    app.run("0.0.0.0", "8083")
