#  Copyright 2019 U.C. Berkeley RISE Lab
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import logging

import numpy as np
import sys
import csv
from cloudburst.shared.proto.cloudburst_pb2 import CloudburstError, DAG_ALREADY_EXISTS

BENCHMARK_START_PORT = 3000
TRIGGER_PORT = 2999

unit_dict = {'s': 1, 'ms': 1000, 'us': 1000000}

def print_latency_stats(data, ident, log=False, epoch=0, unit='ms', bname=None, args=[], csv_filename=None):
    # Amplify according to unit
    data = [x * unit_dict[unit] for x in data]
    
    npdata = np.array(data)
    tput = 0

    if epoch > 0:
        tput = len(data) / epoch

    mean = np.mean(npdata)
    median = np.percentile(npdata, 50)
    p75 = np.percentile(npdata, 75)
    p95 = np.percentile(npdata, 95)
    p99 = np.percentile(npdata, 99)
    mx = np.max(npdata)

    p25 = np.percentile(npdata, 25)
    p05 = np.percentile(npdata, 5)
    p01 = np.percentile(npdata, 1)
    mn = np.min(npdata)

    output = ('%s LATENCY:\n\tsample size: %d\n' +
              '\tTHROUGHPUT: %.4f\n'
              '\tTime unit: %s\n'
              '\tmean: %.3f, median: %.3f\n' +
              '\tmin/max: (%.3f, %.3f)\n' +
              '\tp25/p75: (%.3f, %.3f)\n' +
              '\tp5/p95: (%.3f, %.3f)\n' +
              '\tp1/p99: (%.3f, %.3f)') % (ident, len(data), tput, unit, mean,
                                           median, mn, mx, p25, p75, p05, p95,
                                           p01, p99)

    if log:
        logging.info(output)
    else:
        print(output)

    if csv_filename:
        args = ":".join(args) if args else None
        csv_output = {
            'BNAME': bname,
            'IDENT': ident, 
            'ARGS': args,
            'SAMPLE_SIZE': len(data), 
            'THROUGHPUT': tput, 
            'TIME_UNIT': unit,
            'MEAN': mean,
            'MEDIAN': median,
            'MIN': mn,
            'MAX': mx,
            'P25': p25,
            'P75': p75,
            'P5': p05,
            'P95': p95,
            'P1': p01,
            'P99': p99
            }
        with open(csv_filename, 'a', newline='') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=csv_output.keys())
            if csv_file.tell() == 0:
                writer.writeheader()
            writer.writerow(csv_output)

# Function name = DAG name
def register_dag_for_single_func(cloudburst_client, function_name):
    functions = [function_name]
    connections = []
    success, error = cloudburst_client.register_dag(function_name, functions,
                                                     connections)
    if not success and error != DAG_ALREADY_EXISTS:
        print('Failed to register DAG: %s' % (CloudburstError.Name(error)))
        sys.exit(1)