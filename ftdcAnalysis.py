#!/usr/bin/python
"""
Python script to analyze ftdc data and output anomalies and detected correlations
"""
import argparse
import simplejson
import time
import numpy as np

def epoch_to_utc(timestamp):
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp / 1000))


def run_ftdc_util(diagnostics_data, temp_file):
    import subprocess
    # TODO - change to ftdc export
    command = './ftdc decode -o %s %s' % (temp_file, diagnostics_data)
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    process.wait()


def log_verbose(msg):
    if _verbose:
        print(msg)


def add_value_to_delta(delta, starting_value):
    return starting_value + delta


def add_values_to_metrics(metric):
    key = metric["Key"]
    prev_value = metric["Value"]
    if not metrics.get(key):
        metrics[key] = {
            "key": key,
            "raw_values": [],
            "per_sec_values": [],
            "outliers": []
        }
    metrics[metric["Key"]]["raw_values"].append(prev_value)
    for delta in metric["Deltas"]:
        metrics[metric["Key"]]["raw_values"].append(prev_value + delta)
        metrics[metric["Key"]]["per_sec_values"].append(delta)
        prev_value += delta


def get_z_score(val, mean, std):
    return (val - mean) / (std * z_test_stdev_factor)


def get_outliers_by_z_score(metric, raw_arr):
    arr = np.array(raw_arr)
    mean = np.mean(arr)
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
    raw_arr = []
    if setting["raw_value_type"] == "per_sec":
        raw_arr = metric["per_sec_values"]
    else:
        raw_arr = metric["raw_values"]
    if setting["outlier_detection_method"] == "z-test":
        get_outliers_by_z_score(metric, raw_arr)
    if setting["outlier_detection_method"] == "thresholdAbove":
        for val in raw_arr:
            metric["outliers"].append(1 if val > setting["thresholdValue"] else 0)
    if setting["outlier_detection_method"] == "thresholdBelow":
        for val in raw_arr:
            metric["outliers"].append(1 if val < setting["thresholdValue"] else 0)

## TODO - aggregate to 1mins
def export_to_csv():
    dim_cnt = len(metrics)
    csv_dict = {}
    for key, metric in metrics.items():
        if settings["metricsSettings"].get(key):
            setting = settings["metricsSettings"][key]
            if setting["raw_value_type"] == "per_sec":
                raw_arr = metric["per_sec_values"]
            else:
                raw_arr = metric["raw_values"]
            i = 0
            key = setting.get("export_name", key)
            for val in raw_arr:
                date_key = epoch_to_utc(start_point + (i * 1000 * 60)) # TODO - remove * 60
                if not csv_dict.get(date_key):
                    csv_dict[date_key] = {}
                csv_dict[date_key][key] = val
                i += 1
    i = 0
    header_data = ''
    for idx in sorted(csv_dict):
        row = csv_dict[idx]
        if i == 0:
            header_data = 'timestamp,'
        row_data = '%s,' % idx
        for key, metric in row.items():
            if i == 0:
                header_data += "%s," % key
            row_data += "%s," % metric
        row_data = row_data[:-1]
        if i == 0:
            header_data = header_data[:-1]
            print(header_data)
        print(row_data)
        i += 1

temp_file = 'temp.json'
metrics = {}
parser = argparse.ArgumentParser()
parser.add_argument("-v","--verbose", help="verbose", action="store_true")
parser.add_argument("data", nargs='*', help="Full path to the diagnostics data")
args = parser.parse_args()
if args.verbose:
    _verbose = 1
else:
    _verbose = 0

# read settings
fh = open('settings.json', 'r')
settings = simplejson.load(fh)
z_test_stdev_factor = settings.get("z-test-stdev-factor", 3)
z_test_outlier_threshold = settings.get("z-test-outlier-threshold", 0.5)

# run the ftdc util to generate the json file
run_ftdc_util(args.data[0], temp_file)

# import json - TODO - we are currently ignoring gaps
fh = open(temp_file, 'r')
json_data = simplejson.load(fh)
chunk_idx = 0
for chunk in json_data:
    chunk_start = chunk["Metrics"][000]["Value"]
    if chunk_idx == 0:
        start_point = chunk_start
    for metric in chunk["Metrics"]:
        # read metrics into new array
        if settings["metricsSettings"].get(metric["Key"]):
            add_values_to_metrics(metric)
    chunk_idx += 1
# dispose the original json
fh = None
json_data = None

# analyze outliers
for key, metric in metrics.items():
    if settings["metricsSettings"].get(key):
        analyze_outliers(metric, settings["metricsSettings"][key])
    print(metric)
#export_to_csv()

# correlations
# numpy.corrcoef(list1, list2)[0, 1]
