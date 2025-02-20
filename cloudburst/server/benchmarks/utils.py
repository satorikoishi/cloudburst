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

from copy import deepcopy
import logging

import numpy as np
import sys
import csv
import queue
import threading
import time
from cloudburst.shared.proto.cloudburst_pb2 import CloudburstError, DAG_ALREADY_EXISTS

BENCHMARK_START_PORT = 3000
TRIGGER_PORT = 2999
C_ID_BASE = 1234567890
SEC_TO_USEC = 1000000.0
POCKET_MOCK_LATENCY = 317
POCKET_1M_LATENCY = 10000
POCKET_INIT_LATENCY = 200

def gen_c_id(offset):
    return f'{C_ID_BASE + offset}'

class Profiler():
    def __init__(self, bname=None, num_clients=0, args=[]):
        self.tput = 0
        self.lat = []
        self.epoch_lat = []
        self.epoch = 0
        self.thread_lock = threading.Lock()
        self.clock = time.time()

        self.bname = bname
        self.num_clients = num_clients
        self.args = args
        
    def commit(self, latency):
        with self.thread_lock:
            self.tput += 1
            self.lat.append(latency)
            self.epoch_lat.append(latency)

    
    def print_tput(self, csv_filename=None):
        duration = time.time() - self.clock
        epoch_tput = (float)(self.tput) / duration
        epoch_tput_num=deepcopy(self.tput)
        epoch_lat = deepcopy(self.epoch_lat)
        output = f"""EPOCH {self.epoch}, THROUGHPUT: {epoch_tput} /s, DURATION: {duration} s"""
        print(output)
        logging.info(output)

        if csv_filename:
            log_throughput_to_csv(epoch=self.epoch, thruput=epoch_tput, bname=self.bname, num_clients=self.num_clients, args=self.args, duration=duration, csv_filename=csv_filename)
        
        # Reset counter
        with self.thread_lock:
            self.tput = 0
            self.epoch_lat.clear()
        self.clock = time.time()
        self.epoch += 1
        return epoch_tput_num, epoch_lat

class ClientMeta():
    def __init__(self, c_id):
        self.c_id = c_id
        self.start_time = time.time()
        
    def reset(self):
        self.start_time = time.time()
    
    def get_latency(self):
        return time.time() - self.start_time

unit_dict = {'s': 1, 'ms': 1000, 'us': 1000000}

def print_detailed_latency(data, unit='ms', csv_filename=None):
    # Print all latencies
    data = [x * unit_dict[unit] for x in data]
    if csv_filename:
        with open(csv_filename, 'a', newline='') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(data)

def print_latency_stats(data, ident, log=False, epoch=0, unit='ms', bname=None, args=[], csv_filename=None, num_clients=1):
    # Amplify according to unit
    data = [x * unit_dict[unit] for x in data]
    
    npdata = np.array(data)
    tput = 0

    if epoch > 0:
        tput = len(data) / epoch

    mean = np.mean(npdata)
    median = np.percentile(npdata, 50)
    p75 = np.percentile(npdata, 75)
    p90 = np.percentile(npdata, 90)
    p95 = np.percentile(npdata, 95)
    p99 = np.percentile(npdata, 99)
    mx = np.max(npdata)

    p25 = np.percentile(npdata, 25)
    p10 = np.percentile(npdata, 10)
    p05 = np.percentile(npdata, 5)
    p01 = np.percentile(npdata, 1)
    mn = np.min(npdata)

    output = ('%s LATENCY:\n\tsample size: %d\n' +
              '\tNUM_CLIENTS: %d\n'
              '\tTHROUGHPUT: %.4f\n'
              '\tTime unit: %s\n'
              '\tmean: %.3f, median: %.3f\n' +
              '\tmin/max: (%.3f, %.3f)\n' +
              '\tp25/p75: (%.3f, %.3f)\n' +
              '\tp10/p90: (%.3f, %.3f)\n'
              '\tp5/p95: (%.3f, %.3f)\n' +
              '\tp1/p99: (%.3f, %.3f)') % (ident, len(data), num_clients, tput, unit, mean,
                                           median, mn, mx, p25, p75, p10, p90, p05, p95,
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
            'NUM_CLIENTS': num_clients,
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
            'P10': p10,
            'P90': p90,
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
    logging.info(f'Successfully regiestered DAG {function_name}')

def shredder_setup_data(cloudburst_client, setup_func='setup'):
    cloud_setup_shredder = cloudburst_client.get_function(setup_func)
    if cloud_setup_shredder is None:
        def setup_shredder(cloudburst):
            return cloudburst.execute_js_fun(setup_func, client_name='shredder')
        cloud_setup_shredder = cloudburst_client.register(setup_shredder, setup_func)
        if cloud_setup_shredder:
            logging.info('Successfully registered setup shredder function.')
        else:
            print('Failed registered setup shredder function.')
            sys.exit(1)
    
    res = cloud_setup_shredder().get()
    logging.info(f'Setup shredder result: {res}')

# For testing Tput
def client_recv_dag_response(cloudburst_client, stop_event, meta_dict, q, profiler):
    while True:
        if stop_event.is_set() and q.full():
            break
        res = cloudburst_client.async_recv_dag_response()
        if res == None:
            continue
        c_id, _ = res
        lat = meta_dict[c_id].get_latency()
        q.put(c_id)    
        profiler.commit(lat) 

def log_throughput_to_csv(epoch, thruput, bname, num_clients, args, duration, csv_filename):
    args = ":".join(args) if args else None
    csv_output = {
        'BNAME': bname,
        'EPOCH': epoch,
        'NUM_CLIENTS': num_clients,
        'ARGS': args,
        'THROUGHPUT': thruput,
        'DURATION(s)': duration
    }
    with open(csv_filename, 'a', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, delimiter='\t', fieldnames=csv_output.keys())
        if csv_file.tell() == 0:
            writer.writeheader()
        writer.writerow(csv_output)

def emulate_exec(compute_duration):
    # precised sleep
    now = time.time()
    end = now + compute_duration / SEC_TO_USEC
    while now < end:
        now = time.time()