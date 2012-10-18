#!/usr/bin/env python

import parallel
import multiprocessing
import time
import uuid

# max ms spent sending/receiving each job
TRANSPORT_MS = 50

def worker_addresses(ip_addresses):
    worker_addresses = []
    for address in ip_addresses:
        worker_addresses.append(('%s:%s' % (address, parallel.VENT_PORT_DEFAULT), '%s:%s' % (address, parallel.SINK_PORT_DEFAULT)))
    return worker_addresses

def work(vent_port, sink_port, worker_pool):
    def result_received(job_id, result):
        pass
    worker, close, run_job = parallel.construct_worker(worker_pool, {'vent_port': vent_port, 'sink_port': sink_port})
    worker(result_received)

def pusher(vent_port, sink_port, worker_addresses, send_jobs, on_recv_result):
    worker, close, run_job = parallel.construct_worker(worker_addresses, {'vent_port': vent_port, 'sink_port': sink_port})
    send_jobs(run_job)
    worker(on_recv_result)

# provides functions to start a set of workers including 1 pusher and num - 1 normal workers. worker_pool is a list containing vent/sink address pairs.
def construct_worker_pool(num, worker_addresses, send_jobs, on_recv_result, num_pushers=1):
    processes = []
    def start(start_port=5000):
        for i in range(num_pushers):
            p = multiprocessing.Process(target=pusher, args=(start_port, start_port + 1, worker_addresses, send_jobs, on_recv_result))
            p.start()
            processes.append(p)
            start_port += 2
        for i in range(num - num_pushers):
            p = multiprocessing.Process(target=work, args=(start_port, start_port + 1, worker_addresses))
            p.start()
            processes.append(p)
            start_port += 2
    def kill():
        for p in processes:
            p.terminate()
    return start, kill

def check_for_completion(total_completed, num_jobs, timeout):
    tstart = time.time()
    while (time.time() - tstart) < timeout * 0.001:
        if total_completed.value == num_jobs:
            return True
    return False
