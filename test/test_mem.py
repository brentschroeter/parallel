#!/usr/bin/env python

import parallel
import unittest
import time
import string
import random
import testing_lib
from multiprocessing import RawValue
from guppy import hpy

WORKER_POOL = [('localhost:5000', 'localhost:5001'), ('localhost:5002', 'localhost:5003'), ('localhost:5004', 'localhost:5005')]
NUM_STRINGS = 200
STR_LEN = 1000
MEM_LIMIT = 8000000
NUM_WORKERS = 3

def str_job(s):
    # reverse the string
    return s[::-1]

def get_timeout(num_workers):
    transportation_time = testing_lib.TRANSPORT_MS * NUM_STRINGS + 1000
    computation_time = 5 * NUM_STRINGS / num_workers
    return transportation_time + computation_time

def run_jobs(run_job, fail):
    hp = hpy()
    def generate_str(length):
        return ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(length))
    for i in range(NUM_STRINGS):
        run_job(str_job, (generate_str(STR_LEN)))
    size = hp.heap().size
    if size > MEM_LIMIT:
        fail('Heap size exceeded %d bytes (%d).' % (MEM_LIMIT, size))

class TestParallel(unittest.TestCase):
    def test_mem(self):
        total_completed = RawValue('i')
        def result_received(result, job_id):
            total_completed.value += 1
        def check_for_completion():
            timeout = get_timeout(NUM_WORKERS)
            tstart = time.time()
            while (time.time() - tstart) < timeout * 0.001:
                if total_completed.value == NUM_STRINGS:
                    return True
            return False
        def push(vent_port, sink_port, worker_pool):
            worker, close, run_job = parallel.construct_worker(worker_pool, {'vent_port': vent_port, 'sink_port': sink_port})
            run_jobs(run_job, self.fail)
            worker(result_received)

        total_completed.value = 0
        start_workers, kill_workers = testing_lib.construct_worker_pool(NUM_WORKERS, WORKER_POOL, push)
        start_workers()
        timeout = get_timeout(NUM_WORKERS)
        completion = check_for_completion()
        kill_workers()
        if not completion:
            self.fail('Not all jobs received: %d / %d' % (total_completed.value, NUM_STRINGS))

if __name__ == '__main__':
    unittest.main()
