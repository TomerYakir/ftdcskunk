#!/usr/bin/python
"""
Python script to analyze ftdc data and output anomalies and detected correlations
"""
import argparse
import simplejson
import time
import numpy as np
import subprocess
import math

def epoch_to_utc(timestamp):
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp / 1000))

def run_ftdc_util(diagnostics_data, temp_file):
    command = './ftdc export -o %s %s' % (temp_file, diagnostics_data)
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    process.wait()

def import_export_json(temp_file):
    command = 'mongoimport --drop --db=ftdc --collection=ftdc --port 30000 %s' % temp_file
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    process.wait()

def load_json_data():
    from pymongo import MongoClient
    json_data = []
    client = MongoClient('mongodb://localhost:30000/')
    db = client['ftdc']
    collection = db.ftdc
    for doc in collection.find().sort("start",1):
        json_data.append(doc)
    return json_data

def get_z_score(val, mean, std):
    if std != 0.0:
        return (val - mean) / (std * z_test_stdev_factor)
    else:
        return 0

def get_outliers_by_z_score(metric, arr, raw_arr, mean):
    #print(metric)
    std = np.std(arr)
    metric["mean"] = mean
    metric["std"] = std
    z_scores = []
    for val in raw_arr:
        z_scores.append(get_z_score(val, mean, std))
    metric["z_scores"] = z_scores
    for val in z_scores:
        if abs(val) > z_test_outlier_threshold and val > 0:
            outlier = 1
        elif abs(val) > z_test_outlier_threshold and val < 0:
            outlier = -1
        else:
            outlier = 0
        metric["outliers"].append(outlier)

def analyze_outliers(metric, setting):
    raw_arr = metric["values"]
    arr = np.array(raw_arr)
    min_val = np.min(arr)
    mean = np.mean(arr)
    max_val = np.max(arr)
    metric["min"] = min_val
    metric["max"] = max_val
    if setting["outlier_detection_method"] == "z-test":
        get_outliers_by_z_score(metric, arr, raw_arr, mean)
    if setting["outlier_detection_method"] == "thresholdAbove":
        for val in raw_arr:
            metric["outliers"].append(1 if val > setting["thresholdValue"] else 0)
    if setting["outlier_detection_method"] == "thresholdBelow":
        for val in raw_arr:
            metric["outliers"].append(-1 if val < setting["thresholdValue"] else 0)
    for i in xrange(len(raw_arr)):
        if metric["outliers"][i] == 0:
            metric["values_for_chart"][i]["type"] = "normal"
        else:
            metric["values_for_chart"][i]["type"] = "outlier"

def add_metric_to_timeseries(key, metric, setting, timestamp):
    if not metrics.get(key):
        metrics[key] = {
            "values": [],
            "values_for_chart": [],
            "raw_values": [metric],
            "outliers": [],
            "z_scores": [],
            "checked": False,
            "mean": 0,
            "min": 0,
            "max": 0,
            "displayName": setting["export_name"],
            "fullName": key,
            "code": setting["code"]
        }
    if setting["raw_value_type"] == "per_sec": # assumption - data is continuous
        prev_value = metrics[key]["raw_values"][-1]
        #print("%s: prev %s, metric %s, per_sec %s" % (key, prev_value, metric, metric - prev_value))
        metrics[key]["raw_values"].append(metric)
        metric = metric - prev_value
    metrics[key]["values"].append(metric)
    metrics[key]["values_for_chart"].append({
        "date": epoch_to_utc(timestamp),
        "value": metric
    })

temp_file = 'temp.json'
metrics = {}
parser = argparse.ArgumentParser()
parser.add_argument("-v","--verbose", help="verbose", action="store_true")
parser.add_argument("-s","--skipftdc", help="skip reading FTDC", action="store_true")
parser.add_argument("data", nargs='*', help="Full path to the diagnostics data")
args = parser.parse_args()
if args.verbose:
    _verbose = 1
else:
    _verbose = 0

def log_verbose(msg):
    if _verbose:
        print(msg)

# read settings
log_verbose("read settings")
fh = open('settings.json', 'r')
settings = simplejson.load(fh)
z_test_stdev_factor = settings.get("z-test-stdev-factor", 3)
z_test_outlier_threshold = settings.get("z-test-outlier-threshold", 0.5)

# run the ftdc util to generate the json file
if not args.skipftdc:
    log_verbose("export FTDC")
    run_ftdc_util(args.data[0], temp_file)

# transform the raw JSON
if not args.skipftdc:
    import_export_json(temp_file)

# load the JSON data
log_verbose("load JSON data")
json_data = load_json_data()

# process the info
log_verbose("process information")
for entry in json_data:
    timestamp = entry["start"]
    for key, metric in entry.items():
        if settings["metricsSettings"].get(key):
            add_metric_to_timeseries(key, metric, settings["metricsSettings"][key], timestamp)

# find outliers
log_verbose("analyze metrics")
for key, metric in metrics.items():
    analyze_outliers(metric, settings["metricsSettings"][key])

# correlate
log_verbose("analyze correlations")
correlations = {}
for key, metric in metrics.items():
    for secondKey, secondMetric in metrics.items():
        if key != secondKey and not correlations.get("%s_%s" % (key, secondKey)) and not correlations.get("%s_%s" % (secondKey, key)):
            f_arr = np.array(metric["outliers"])
            s_arr = np.array(secondMetric["outliers"])
            corr = abs(round(np.corrcoef(f_arr, s_arr)[0, 1],3))
            if math.isnan(corr):
                corr = 0
            correlations["%s_%s" % (key, secondKey)] = {
                "MetricOne": metric["code"],
                "MetricTwo": secondMetric["code"],
                "Score": corr,
                "Tooltip": "%s, %s, Correlation Score: %s" % (metric["displayName"], secondMetric["displayName"], corr)
            }
export_correlations = []
for key, corr in correlations.items():
    export_correlations.append(corr)

# export to file
export_metrics = {}

for key, metric in metrics.items():
    metric.pop("outliers", None)
    metric.pop("z_scores", None)
    metric["values"] = metric["values_for_chart"]
    metric.pop("values_for_chart", None)
    export_metrics[metric["code"]] = metric


export_struct = {
    "metrics": export_metrics,
    "correlations": export_correlations,
    "metricsProcessed": 984
}

from bson.json_util import dumps
print(dumps(export_struct))
