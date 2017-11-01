#!/usr/bin/python
"""
Python script to analyze ftdc data and output anomalies and detected correlations
"""
import argparse
import simplejson
import json
import time
import numpy as np
import subprocess

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
    for doc in collection.find():
        json_data.append(doc)
    return json_data

def add_metric_to_timeseries(key, metric, setting):
    if not metrics.get(key):
        metrics[key] = {
            "values": [],
            "outliers": [],
            "z_scores": []
        }
    if setting["raw_value_type"]



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
for entry in json_data:
    timestamp = entry["start"]
    for key, metric in entry.items():
        if settings["metricsSettings"].get(key):
            print("%s %s %s" % (timestamp, key, metric))
            add_metric_to_timeseries(key, metric, settings["metricsSettings"][key])
