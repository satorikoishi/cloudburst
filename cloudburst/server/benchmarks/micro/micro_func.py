import logging
import random
import sys
import time

import cloudpickle as cp

from cloudburst.server.benchmarks import utils
from cloudburst.server.benchmarks.micro import meta
from cloudburst.shared.reference import CloudburstReference

# Args: dag_name, v_size

def run(cloudburst_client, num_requests, sckt, args):
    if len(args) < 2:
        print(f"{args} too short. Args at least 2: dag_name, v_size")
        sys.exit(1)
    
    dag_name = args[0]
    v_size = int(args[1])
    
    if v_size not in meta.ARR_SIZE:
        print(f'{v_size} not in {meta.ARR_SIZE}')
        sys.exit(1)

    logging.info(f'Running micro bench. Dag: {dag_name}, Vsize: {v_size}')

    total_time = []
    scheduler_time = []
    kvs_time = []

    retries = 0

    log_start = time.time()

    log_epoch = 0
    epoch_total = []

    for i in range(num_requests):
        # TODO: generate ref for different workloads
        key = meta.key_gen(v_size, random.randrange(meta.NUM_KV_PAIRS))
        
        # DAG name = Function name
        arg_map = {dag_name: key}
        
        start = time.time()
        res = cloudburst_client.call_dag(dag_name, arg_map)
        end = time.time()
        s_time = end - start
        
        start = time.time()
        res.get()
        end = time.time()
        k_time = end - start
        
        scheduler_time += [s_time]
        kvs_time += [k_time]
        epoch_total += [s_time + k_time]
        total_time += [s_time + k_time]

        log_end = time.time()
        if (log_end - log_start) > 10:
            if sckt:
                sckt.send(cp.dumps(epoch_total))
            utils.print_latency_stats(epoch_total, 'EPOCH %d E2E' %
                                        (log_epoch), True, bname='micro', args=args, csv_filename='benchmark.csv')

            epoch_total.clear()
            log_epoch += 1
            log_start = time.time()

    return total_time, scheduler_time, kvs_time, retries
