#!/usr/bin/env python

import parallel
import multiprocessing
import time
import uuid

# max ms spent sending/receiving each job
TRANSPORT_MS = 50

def work(vent_port, sink_port, worker_pool, worker_id):
    def result_received(job_id, result):
        pass
    worker, close, run_job = parallel.construct_worker(worker_pool, {'vent_port': vent_port, 'sink_port': sink_port, 'worker_id': worker_id})
    worker(result_received)

# provides functions to start a set of workers including 1 pusher and num - 1 normal workers. worker_pool is a list containing vent/sink address pairs.
def construct_worker_pool(num, worker_pool, push_func, logging=False, num_pushers=1):
    processes = []
    worker_ids = []
    def start(start_port=5000):
        for i in range(num_pushers):
            if logging:
                worker_ids.append(uuid.uuid4())
            else:
                worker_ids.append(None)
            p = multiprocessing.Process(target=push_func, args=(start_port, start_port + 1, worker_pool, worker_ids[-1]))
            p.start()
            processes.append(p)
            start_port += 2
        for i in range(num - num_pushers):
            if logging:
                worker_ids.append(uuid.uuid4())
            else:
                worker_ids.append(None)
            p = multiprocessing.Process(target=work, args=(start_port, start_port + 1, worker_pool, worker_ids[-1]))
            p.start()
            processes.append(p)
            start_port += 2
    def kill():
        for p in processes:
            p.terminate()
    def get_worker_ids():
        return worker_ids
    return start, kill, get_worker_ids

def check_for_completion(total_completed, num_jobs, timeout):
    tstart = time.time()
    while (time.time() - tstart) < timeout * 0.001:
        if total_completed.value == num_jobs:
            return True
    return False
