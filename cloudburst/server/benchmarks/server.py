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
import sys

import zmq

from cloudburst.client.client import CloudburstConnection
from cloudburst.server.benchmarks import (
    composition,
    locality,
    lambda_locality,
    mobilenet,
    predserving,
    scaling,
    utils
)
from cloudburst.server.benchmarks.micro import (
    micro_func,
    prepare
)
from cloudburst.server.benchmarks.comparison import (
    accumulate,
    k_hop,
    list_traversal,
    read_array,
    social_network,
    split,
    no_op,
    profile,
    profile_executor,
    compute_emulate,
    data_size,
    ycsb,
    facebook_social,
    arbiter_benefit,
    cache_cold
)
import cloudburst.server.utils as sutils

logging.basicConfig(filename='log_benchmark.txt', level=logging.INFO,
                    format='%(asctime)s %(message)s')

def benchmark(ip, cloudburst_address, tid):
    cloudburst = CloudburstConnection(cloudburst_address, ip, tid)

    ctx = zmq.Context(1)

    benchmark_start_socket = ctx.socket(zmq.PULL)
    benchmark_start_socket.bind('tcp://*:' + str(utils.BENCHMARK_START_PORT + tid))
    kvs = cloudburst.kvs_client

    while True:
        msg = benchmark_start_socket.recv_string()
        
        logging.info(f'Received msg: {msg}')
        splits = msg.split(':')

        resp_addr = splits[0]
        bname = splits[1]
        num_requests = int(splits[2])
        args = []
        if len(splits) > 3:
            args = splits[3:]
            
        logging.info(f'Received req from {resp_addr}, bench: {bname}, num_req: {num_requests}')

        sckt = ctx.socket(zmq.PUSH)
        sckt.connect('tcp://' + resp_addr + f':{utils.TRIGGER_PORT}')
        
        run_bench(bname, num_requests, cloudburst, kvs, sckt, args)

def run_bench(bname, num_requests, cloudburst, kvs, sckt, args=[], create=False):
    logging.info('Running benchmark %s, %d requests.' % (bname, num_requests))

    if bname == 'composition':
        total, scheduler, kvs, retries = composition.run(cloudburst, num_requests,
                                                         sckt)
    elif bname == 'locality':
        total, scheduler, kvs, retries = locality.run(cloudburst, num_requests,
                                                      create, sckt)
    elif bname == 'redis' or bname == 's3':
        total, scheduler, kvs, retries = lambda_locality.run(bname, kvs,
                                                             num_requests,
                                                             sckt)
    elif bname == 'predserving':
        total, scheduler, kvs, retries = predserving.run(cloudburst, num_requests,
                                                         sckt)
    elif bname == 'mobilenet':
        total, scheduler, kvs, retries = mobilenet.run(cloudburst, num_requests,
                                                       sckt)
    elif bname == 'scaling':
        total, scheduler, kvs, retries = scaling.run(cloudburst, num_requests,
                                                     sckt, create)
    elif bname == 'prepare':
        total, scheduler, kvs, retries = prepare.run(cloudburst, num_requests,
                                                     sckt)
    elif bname == 'micro':
        total, scheduler, kvs, retries = micro_func.run(cloudburst, num_requests,
                                                     sckt, args)
    elif bname == 'k_hop':
        total, scheduler, kvs, retries = k_hop.run(cloudburst, num_requests,
                                                     sckt, args)
    elif bname == 'list_traversal':
        total, scheduler, kvs, retries = list_traversal.run(cloudburst, num_requests,
                                                     sckt, args)
    elif bname == 'social_network':
        total, scheduler, kvs, retries = social_network.run(cloudburst, num_requests,
                                                     sckt, args)
    elif bname == 'read_array':
        total, scheduler, kvs, retries = read_array.run(cloudburst, num_requests,
                                                     sckt, args)
    elif bname == 'split':
        total, scheduler, kvs, retries = split.run(cloudburst, num_requests,
                                                     sckt, args)
    elif bname == 'accumulate':
        total, scheduler, kvs, retries = accumulate.run(cloudburst, num_requests,
                                                     sckt, args)
    elif bname == 'no_op':
        total, scheduler, kvs, retries = no_op.run(cloudburst, num_requests,
                                                     sckt, args)
    elif bname == 'profile':
        total, scheduler, kvs, retries = profile.run(cloudburst, num_requests,
                                                     sckt, args)
    elif bname == 'profile_executor':
        total, scheduler, kvs, retries = profile_executor.run(cloudburst, num_requests,
                                                     sckt, args)
    elif bname == 'compute_emulate':
        total, scheduler, kvs, retries = compute_emulate.run(cloudburst, num_requests,
                                                     sckt, args)
    elif bname == 'facebook_social':
        total, scheduler, kvs, retries = facebook_social.run(cloudburst, num_requests,
                                                     sckt, args)
    elif bname == 'arbiter_benefit':
        total, scheduler, kvs, retries = arbiter_benefit.run(cloudburst, num_requests,
                                                     sckt, args)
    elif bname == 'cache_cold':
        total, scheduler, kvs, retries = cache_cold.run(cloudburst, num_requests,
                                                     sckt, args)
    elif bname == 'data_size':
        total, scheduler, kvs, retries = data_size.run(cloudburst, num_requests,
                                                     sckt, args)
    elif bname == 'ycsb':
        total, scheduler, kvs, retries = ycsb.run(cloudburst, num_requests,
                                                     sckt, args)
    else:
        logging.info('Unknown benchmark type: %s!' % (bname))
        sckt.send(b'END')
        return
    
    summary_result(bname, sckt, total, scheduler, kvs, retries, args, num_requests)

def summary_result(bname, sckt, total, scheduler, kvs, retries, args=[], num_requests=1):
    # some benchmark modes return no results
    if not total:
        sckt.send(b'END')
        logging.info('*** Benchmark %s finished. It returned no results. ***'
                     % (bname))
        return
    else:
        sckt.send(b'END')
        logging.info('*** Benchmark %s finished. ***' % (bname))

    logging.info('Total computation time: %.4f' % (sum(total)))
    if len(total) > 0:
        utils.print_latency_stats(total, 'E2E', True, bname=bname, args=args, csv_filename="benchmark_lat.csv", num_clients=num_requests)
    if len(scheduler) > 0:
        utils.print_latency_stats(scheduler, 'SCHEDULER', True, bname=bname, args=args, csv_filename="benchmark_lat.csv", num_clients=num_requests)
    if len(kvs) > 0:
        utils.print_latency_stats(kvs, 'KVS', True, bname=bname, args=args, csv_filename="benchmark_lat.csv", num_clients=num_requests)
    logging.info('Number of KVS get retries: %d' % (retries))


if __name__ == '__main__':
    if len(sys.argv) > 1:
        conf_file = sys.argv[1]
    else:
        conf_file = 'conf/cloudburst-config.yml'

    conf = sutils.load_conf(conf_file)
    bench_conf = conf['benchmark']

    benchmark(conf['ip'], bench_conf['cloudburst_address'],
              int(bench_conf['thread_id']))
