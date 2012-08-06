#!/usr/bin/env python

import parallel
import multiprocessing

# max ms spent sending/receiving each job
TRANSPORT_MS = 50

def work(vent_port, sink_port, worker_pool):
    def result_received(job_id, result):
        pass
    worker, close, run_job = parallel.construct_worker(worker_pool, {'vent_port': vent_port, 'sink_port': sink_port})
    worker(result_received)

# provides functions to start a set of workers including 1 pusher and num - 1 normal workers. worker_pool is a list containing vent/sink address pairs.
def construct_worker_pool(num, worker_pool, push_func):
    processes = []
    def start(start_port=5000):
        p = multiprocessing.Process(target=push_func, args=(start_port, start_port + 1, worker_pool))
        p.start()
        processes.append(p)
        for i in range(num - 1):
            start_port += 2
            p = multiprocessing.Process(target=work, args=(start_port, start_port + 1, worker_pool))
            p.start()
            processes.append(p)
    def kill():
        for p in processes:
            p.terminate()
    return start, kill
