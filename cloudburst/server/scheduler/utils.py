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
import zmq

from anna.lattices import SetLattice

import cloudburst.server.utils as sutils
from cloudburst.shared.proto.shared_pb2 import StringSet

FUNCOBJ = 'funcs/index-allfuncs'

NUM_EXEC_THREADS = 3

EXECUTORS_PORT = 7002
SCHEDULERS_PORT = 7004

unit_dict = {'s': 1, 'ms': 1000, 'us': 1000000}

def get_func_list(client, prefix, fullname=False):
    funcs = client.get(FUNCOBJ)[FUNCOBJ]
    if not funcs:
        return []

    prefix = sutils.FUNC_PREFIX + prefix
    decoded = map(lambda v: str(v, 'utf-8'), funcs.reveal())
    result = list(filter(lambda fn: fn.startswith(prefix), decoded))

    if not fullname:
        result = list(map(lambda fn: fn.split(sutils.FUNC_PREFIX)[-1], result))

    return result


def put_func_list(client, funclist):
    # Convert to a set in order to remove duplicates.
    result = set()
    for val in funclist:
        result.add(bytes(val, 'utf-8'))

    lattice = SetLattice(result)
    client.put(FUNCOBJ, lattice)


def get_cache_ip_key(ip):
    return 'ANNA_METADATA|cache_ip|' + ip


def get_pin_address(ip, tid):
    return 'tcp://' + ip + ':' + str(sutils.PIN_PORT + tid)


def get_unpin_address(ip, tid):
    return 'tcp://' + ip + ':' + str(sutils.UNPIN_PORT + tid)


def get_exec_address(ip, tid):
    return 'tcp://' + ip + ':' + str(sutils.FUNC_EXEC_PORT + tid)


def get_queue_address(ip, tid):
    return 'tcp://' + ip + ':' + str(sutils.DAG_QUEUE_PORT + int(tid))


def get_scheduler_list_address(mgmt_ip):
    return 'tcp://' + mgmt_ip + ':' + str(SCHEDULERS_PORT)


def get_scheduler_update_address(ip):
    return 'tcp://' + ip + ':' + str(sutils.SCHED_UPDATE_PORT)


def get_ip_set(management_request_socket, exec_threads=True):
    # we can send an empty request because the response is always the same
    management_request_socket.send(b'')

    try:
        ips = StringSet()
        ips.ParseFromString(management_request_socket.recv())
        result = set()

        if exec_threads:
            for ip in ips.keys:
                for i in range(NUM_EXEC_THREADS):
                    result.add((ip, i))

            return result
        else:
            return set(ips.keys)
    except zmq.ZMQError as e:
        if e.errno == zmq.EAGAIN:
            return None
        else:
            raise e


def find_dag_source(dag):
    sinks = set()
    for conn in dag.connections:
        sinks.add(conn.sink)

    funcs = set(map(lambda fref: fref.name, dag.functions))
    for sink in sinks:
        funcs.remove(sink)

    return funcs

def print_scheduler_stats(data, unit='ms', log=False, msg=None):
    if msg:
        if log:
            logging.info(msg)
        else:
            print(msg)

    for k, v in data.items():
        assert(type(v) == list)
        if len(v) == 0:
            continue
        # Amplify according to unit
        npv = [x * unit_dict[unit] for x in v]
        npdata = np.array(npv)
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

        output = ('DAG %s:\n\tsample size: %d\n' +
              '\tTime unit: %s\n'
              '\tmean: %.3f, median: %.3f\n' +
              '\tmin/max: (%.3f, %.3f)\n' +
              '\tp25/p75: (%.3f, %.3f)\n' +
              '\tp5/p95: (%.3f, %.3f)\n' +
              '\tp1/p99: (%.3f, %.3f)') % (k, len(npdata), unit, mean,
                                           median, mn, mx, p25, p75, p05, p95,
                                           p01, p99)
        
        if log:
            logging.info(output)
        else:
            print(output)

