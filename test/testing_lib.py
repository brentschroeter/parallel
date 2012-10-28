#!/usr/bin/env python

import parallel
import multiprocessing
import time
import uuid

# max ms spent sending/receiving each job
TRANSPORT_MS = 50

def worker_addresses(ip_addresses):
    '''Returns a list of (vent, sink) pairs based on ip_addresses.'''
    worker_addresses = []
    for address in ip_addresses:
        worker_addresses.append(('%s:%s' % (address, parallel.VENT_PORT_DEFAULT), '%s:%s' % (address, parallel.SINK_PORT_DEFAULT)))
    return worker_addresses

def work(vent_port, sink_port, worker_pool):
    '''Creates one basic worker.'''
    def result_received(job_id, result, args):
        pass
    worker, close, run_job = parallel.construct_worker(worker_pool, {'vent_port': vent_port, 'sink_port': sink_port})
    worker(result_received, ())

def pusher(vent_port, sink_port, worker_addresses, send_jobs, sender_args, on_recv_result, on_recv_result_args):
    worker, close, run_job = parallel.construct_worker(worker_addresses, {'vent_port': vent_port, 'sink_port': sink_port})
    send_jobs(run_job, sender_args)
    worker(on_recv_result, on_recv_result_args)

def construct_worker_pool(num, worker_addresses, send_jobs, sender_args, on_recv_result, on_recv_result_args, num_pushers=1):
    '''Constructs functions to start a pool of workers (some of which are also servers).'''
    processes = []
    def start(start_port=5000):
        for i in range(num_pushers):
            p = multiprocessing.Process(target=pusher, args=(start_port, start_port + 1, worker_addresses, send_jobs, sender_args, on_recv_result, on_recv_result_args))
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

def check_for_completion(total_completed, num_jobs, timeout_ms):
    '''Returns True if multiprocessing.RawValue total_completed is equal to int num_jobs before timeout_ms have passed.'''
    tstart = time.time()
    while (time.time() - tstart) < timeout_ms * 0.001:
        if total_completed.value == num_jobs:
            return True
    return False
