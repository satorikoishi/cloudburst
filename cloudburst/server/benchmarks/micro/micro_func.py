import logging
import random
import sys
import time

import cloudpickle as cp

from cloudburst.server.benchmarks import utils
from cloudburst.server.benchmarks.micro import meta
from cloudburst.shared.reference import CloudburstReference

# Args: function_name, v_size

def run(cloudburst_client, num_requests, sckt, args):
    if len(args) < 2:
        print(f"{args} too short. Args at least 2: function_name, v_size")
        sys.exit(1)
    
    function_name = args[0]
    v_size = args[1]
    
    if v_size not in meta.ARR_SIZE:
        print(f'{v_size} not in {meta.ARR_SIZE}')
        sys.exit(1)

    logging.info(f'Running micro bench. Function: {function_name}, Vsize: {v_size}')

    total_time = []
    scheduler_time = []
    kvs_time = []

    retries = 0

    log_start = time.time()

    log_epoch = 0
    epoch_total = []

    for i in range(num_requests):
        start = time.time()
        
        # TODO: generate ref for different workloads
        key = meta.key_gen(v_size, random.randrange(meta.NUM_KV_PAIRS))
        ref = CloudburstReference(key, True)
        
        cloudburst_client.exec_func(function_name, ref)
        
        end = time.time()

        epoch_total.append(end - start)
        total_time.append(end - start)

        log_end = time.time()
        if (log_end - log_start) > 10:
            if sckt:
                sckt.send(cp.dumps(epoch_total))
            utils.print_latency_stats(epoch_total, 'EPOCH %d E2E' %
                                        (log_epoch), True)

            epoch_total.clear()
            log_epoch += 1
            log_start = time.time()

    return total_time, scheduler_time, kvs_time, retries
