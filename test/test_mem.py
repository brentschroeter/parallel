#!/usr/bin/env python

import parallel
import unittest
import string
import random
import testing_lib
from multiprocessing import RawValue
from guppy import hpy

WORKER_POOL = [('localhost:5000', 'localhost:5001'), ('localhost:5002', 'localhost:5003'), ('localhost:5004', 'localhost:5005')]
NUM_STRINGS = 200
STR_LEN = 100000000
MEM_LIMIT = 206000000
NUM_WORKERS = 3

def str_job(s):
    # reverse the string
    return s[::-1]

def generate_str(length):
    return ''.join(random.choice(string.ascii_uppercase) for x in range(length))

def get_timeout():
    transportation_time = testing_lib.TRANSPORT_MS * NUM_STRINGS + 1000
    computation_time = 0.00004 * NUM_STRINGS * STR_LEN
    return transportation_time + computation_time

def run_jobs(run_job, test_str, fail):
    hp = hpy()
    test_str = generate_str(STR_LEN)
    for i in range(NUM_STRINGS):
        run_job(str_job, (test_str))
    size = hp.heap().size
    if size > MEM_LIMIT:
        fail('Heap size exceeded %d bytes (%d).' % (MEM_LIMIT, size))

class TestParallel(unittest.TestCase):
    def test_mem(self):
        total_completed = RawValue('i')
        print 'Constructing test string (this could take some time).'
        test_str = generate_str(STR_LEN)
        print 'Done. Starting test.'
        def result_received(result, job_id):
            total_completed.value += 1
        def push(vent_port, sink_port, worker_pool, worker_id):
            worker, close, run_job = parallel.construct_worker(worker_pool, {'vent_port': vent_port, 'sink_port': sink_port})
            run_jobs(run_job, test_str, self.fail)
            worker(result_received)

        total_completed.value = 0
        start_workers, kill_workers, get_worker_ids = testing_lib.construct_worker_pool(NUM_WORKERS, WORKER_POOL, push)
        start_workers()
        completion = testing_lib.check_for_completion(total_completed, NUM_STRINGS, get_timeout())
        kill_workers()
        if not completion:
            self.fail('Not all jobs received: %d / %d' % (total_completed.value, NUM_STRINGS))

if __name__ == '__main__':
    unittest.main()
