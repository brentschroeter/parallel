#!/usr/bin/env python

import parallel
import unittest
import multiprocessing
import time
import string
import random
from guppy import hpy

WORKER_POOL = [('localhost:5000', 'localhost:5001'), ('localhost:5002', 'localhost:5003'), ('localhost:5004', 'localhost:5005')]
# each usage pattern is in the format (sets, sleep time between sets, repetitions per set, sleep time for each job)
NUM_STRINGS = 500
STR_LEN = 200

hp = hpy()

def str_job(s):
    # reverse the string
    return s[::-1]

def get_timeout(num_workers):
    transportation_time = 20 * NUM_STRINGS + 1000
    computation_time = 5 * NUM_STRINGS / num_workers
    return transportation_time + computation_time

def run_jobs(run_job):
    def generate_str(length):
        return ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(length))
    for i in range(NUM_STRINGS):
        run_job(str_job, (generate_str(STR_LEN)))
    print hp.heap()

def push(vent_port, sink_port, on_completed):
    total_completed = [0] # stored as a list as a workaround for Python variable scoping "quirk"
    def result_received(result, job_id):
        total_completed[0] += 1
        if total_completed[0] == NUM_STRINGS:
            on_completed()
        
    worker, close, run_job = parallel.construct_worker(WORKER_POOL, {'vent_port': vent_port, 'sink_port': sink_port})
    run_jobs(run_job)
    worker(result_received)

def work(vent_port, sink_port):
    def result_received(job_id, result):
        pass

    worker, close, run_job = parallel.construct_worker(WORKER_POOL, {'vent_port': vent_port, 'sink_port': sink_port})
    worker(result_received)

class TestParallel(unittest.TestCase):
    def test_mem(self):
        completed = multiprocessing.Value('i')
        completed.value = False
        def on_completed():
            completed.value = True
        def check_for_completion(timeout):
            tstart = time.time()
            while (time.time() - tstart) < timeout * 0.001:
                if completed.value:
                    return True
            return False
        p1 = multiprocessing.Process(target=push, args=('5000', '5001', on_completed))
        p2 = multiprocessing.Process(target=work, args=('5002', '5003'))
        p3 = multiprocessing.Process(target=work, args=('5004', '5005'))
        hp.setrelheap()
        p1.start()
        p2.start()
        p3.start()
        timeout = get_timeout(3)
        if not check_for_completion(timeout):
            self.fail('Did not complete tasks before timeout.')
        p1.terminate()
        p2.terminate()
        p3.terminate()

if __name__ == '__main__':
    unittest.main()
