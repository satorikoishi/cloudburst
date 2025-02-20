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
import time
import zmq

import cloudpickle as cp

from cloudburst.server.benchmarks import utils

logging.basicConfig(filename='log_trigger.txt', level=logging.INFO,
                    format='%(asctime)s %(message)s')

NUM_THREADS = 1

ips = []
with open('bench_ips.txt', 'r') as f:
    line = f.readline()
    while line:
        ips.append(line.strip())
        line = f.readline()

msg = sys.argv[1]
ctx = zmq.Context(1)

recv_socket = ctx.socket(zmq.PULL)
recv_socket.bind(f'tcp://*:{utils.TRIGGER_PORT}')

sent_msgs = 0

if 'create' in msg:
    sckt = ctx.socket(zmq.PUSH)
    sckt.connect('tcp://' + ips[0] + f':{utils.BENCHMARK_START_PORT}')

    sckt.send_string(msg)
    sent_msgs += 1
else:
    for ip in ips:
        for tid in range(NUM_THREADS):
            sckt = ctx.socket(zmq.PUSH)
            sckt.connect('tcp://' + ip + ':' + str(utils.BENCHMARK_START_PORT + tid))

            sckt.send_string(msg)
            sent_msgs += 1

epoch_total = []
total = []
exec_epoch_total = []
exec_total = []
new_exec_tot = []
end_recv = 0

epoch_recv = 0
epoch = 1
epoch_thruput = 0
epoch_start = time.time()

total_tput = 0.0
total_elapsed = 0.0

_, bname, num_clients, *args = msg.split(':')
num_clients = int(num_clients) * NUM_THREADS

while end_recv < sent_msgs:
    msg = recv_socket.recv()

    if b'END' in msg:
        end_recv += 1
    else:
        msg = cp.loads(msg)

        if type(msg) == tuple:
            epoch_thruput += msg[0]
            new_tot = msg[1]
            if len(msg) > 2:
                new_exec_tot = msg[2]
        else:
            new_tot = msg

        epoch_total += new_tot
        total += new_tot
        exec_epoch_total += new_exec_tot
        exec_total += new_exec_tot
        
        epoch_recv += 1


        if epoch_recv == sent_msgs:
            epoch_end = time.time()
            elapsed = epoch_end - epoch_start
            thruput = epoch_thruput / elapsed

            total_tput += epoch_thruput
            total_elapsed += elapsed

            logging.info('\n\n*** EPOCH %d ***' % (epoch))
            logging.info('\tTHROUGHPUT: %.2f' % (thruput))
            logging.info('\tELAPSED: %.2f' % (elapsed))
            utils.print_latency_stats(epoch_total, f'E2E EPOCH {epoch}', True, bname=bname, args=args, csv_filename='latency.csv', num_clients=num_clients)
            utils.print_detailed_latency(epoch_total, csv_filename='detailed_latency.csv')
            if len(exec_epoch_total) > 0:
                utils.print_latency_stats(exec_epoch_total, f'EXEC EPOCH {epoch}', True, bname=bname, args=args, csv_filename='exec_latency.csv', num_clients=num_clients)
                utils.print_detailed_latency(exec_epoch_total, csv_filename='exec_detailed_latency.csv')
            utils.log_throughput_to_csv(epoch, thruput, bname=bname, num_clients=num_clients, args=args, duration=elapsed, csv_filename='throughput.csv')

            epoch_recv = 0
            epoch_thruput = 0
            epoch_total.clear()
            exec_epoch_total.clear()
            epoch_start = time.time()
            epoch += 1

logging.info('*** END ***')

if epoch > 2 and len(total) > 0:
    utils.print_latency_stats(total, 'E2E TOTAL', True, bname=bname, args=args, csv_filename='latency.csv', num_clients=num_clients)
    if len(exec_total) > 0:
        utils.print_latency_stats(exec_total, f'EXEC TOTAL', True, bname=bname, args=args, csv_filename='exec_latency.csv', num_clients=num_clients)
    utils.log_throughput_to_csv('TOTAL', total_tput/total_elapsed, bname=bname, num_clients=num_clients, args=args, duration=elapsed, csv_filename='throughput.csv')
