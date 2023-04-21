import logging
import queue
import random
import sys
import threading
import time

import cloudpickle as cp

from cloudburst.server.benchmarks import utils
from cloudburst.server.benchmarks.micro import meta
from cloudburst.shared.reference import CloudburstReference

# Args: dag_name, v_size

def client_call_dag(cloudburst_client, stop_event, meta_dict, q, *args):
    client_name, dag_name, v_size = args
    logging.info(f'kvs_name: {client_name}, dag_name: {dag_name}, v_size: {v_size}')
    while True:
        if stop_event.is_set():
            break
        c_id = q.get()
        key = meta.key_gen(v_size, random.randrange(meta.NUM_KV_PAIRS))
        # DAG name = Function name
        arg_map = {dag_name: [key, client_name]}
        
        meta_dict[c_id].reset()
        cloudburst_client.call_dag(dag_name, arg_map, direct_response=True, async_response=True, output_key=c_id)

def run(cloudburst_client, num_requests, sckt, args):
    if len(args) < 3:
        print(f"{args} too short. Args at least 2: client_name, dag_name, v_size")
        sys.exit(1)
    
    single_key = False
    if len(args) >= 4:
        if args[-1] == 'tput':
            return run_tput_example(cloudburst_client, num_requests, sckt, args)
        else:
            key = int(args[3])
            single_key = True

    client_name = args[0]
    dag_name = args[1]
    v_size = int(args[2])
    
    if v_size not in meta.ARR_SIZE[client_name]:
        print(f'{v_size} not in {meta.ARR_SIZE[client_name]}')
        sys.exit(1)

    logging.info(f'Running micro bench. KVS client: {client_name}, Dag: {dag_name}, Vsize: {v_size}')

    total_time = []
    scheduler_time = []
    kvs_time = []

    retries = 0

    log_start = time.time()

    log_epoch = 0
    epoch_total = []

    for i in range(num_requests):
        if not single_key:
            # TODO: generate ref for different workloads
            key = meta.key_gen(v_size, random.randrange(meta.NUM_KV_PAIRS))
        
        # DAG name = Function name
        arg_map = {dag_name: [key, client_name]}
        
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
                                        (log_epoch), True, bname='micro', args=args, csv_filename='benchmark_lat.csv')

            epoch_total.clear()
            log_epoch += 1
            log_start = time.time()

    return total_time, scheduler_time, kvs_time, retries

def run_tput_example(cloudburst_client, num_clients, sckt, args):
    client_name = args[0]
    dag_name = args[1]
    v_size = int(args[2])
    logging.info(f'Running list traversal, kvs_name {client_name}, v_size {v_size}, dag: {dag_name}')
    client_meta_dict = {}
    profiler = utils.Profiler(bname='micro', num_clients=num_clients, args=args)

    client_q = queue.Queue(maxsize=num_clients)
    for i in range(num_clients):
        c_id = utils.gen_c_id(i)
        client_q.put(c_id)
        client_meta_dict[c_id] = utils.ClientMeta(c_id)
    
    stop_event = threading.Event()
    call_worker = threading.Thread(target=client_call_dag, args=(cloudburst_client, stop_event, client_meta_dict, client_q, client_name, dag_name, v_size), daemon=True)
    recv_worker = threading.Thread(target=utils.client_recv_dag_response, args=(cloudburst_client, stop_event, client_meta_dict, client_q, profiler), daemon=True)
    
    call_worker.start()
    recv_worker.start()
    
    for i in range(5):
        time.sleep(10)
        epoch_tput, epoch_lat =  profiler.print_tput(csv_filename='benchmark_tput.csv')
        # send epoch result to trigger
        sckt.send(cp.dumps((epoch_tput, epoch_lat)))

    stop_event.set()
    call_worker.join()
    recv_worker.join()
    
    return profiler.lat, [], [], 0
