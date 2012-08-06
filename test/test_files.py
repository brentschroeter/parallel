#!/usr/bin/env python

import parallel
import unittest
import time
import random
import os
import testing_lib
from multiprocessing import RawValue

WORKER_POOL = [('localhost:5000', 'localhost:5001'), ('localhost:5002', 'localhost:5003'), ('localhost:5004', 'localhost:5005')]
TESTING_STR = 'Testing testing testing.'
NUM_WORKERS = 3
NUM_FILES = 5

def file_job(file_contents):
    path = 'testing_files/%s' % str(random.randint(0, 999999)) + '.txt'
    f = open(path, 'w')
    f.write(file_contents)
    f.close()
    return path

def get_timeout(num_workers):
    transportation_time = testing_lib.TRANSPORT_MS * NUM_FILES + 1000
    working_time = 10 * NUM_FILES / num_workers

    return working_time + transportation_time

class TestParallel(unittest.TestCase):
    def test_files(self):
        total_completed = RawValue('i')
        total_completed.value = 0
        def result_received(result, job_id):
            try:
                f = open(result)
                file_contents = f.read()
                f.close()
                os.remove(result)
                self.assertEquals(file_contents, TESTING_STR)
            except:
                self.fail('File not present.')
            total_completed.value += 1
        def check_for_completion():
            timeout = get_timeout(NUM_WORKERS)
            tstart = time.time()
            while (time.time() - tstart) < timeout * 0.001:
                if total_completed.value == NUM_FILES:
                    return True
            return False
        def push(vent_port, sink_port, worker_pool):
            worker, close, run_job = parallel.construct_worker(worker_pool, {'vent_port': vent_port, 'sink_port': sink_port})
            for i in range(NUM_FILES):
                run_job(file_job, (TESTING_STR))
            worker(result_received)

        if not os.path.exists(os.path.join(os.path.dirname(__file__), 'testing_files/')):
            os.mkdir('testing_files')
        start_workers, kill_workers = testing_lib.construct_worker_pool(NUM_WORKERS, WORKER_POOL, push)
        start_workers()
        if not check_for_completion():
            self.fail('Not all jobs received: %d / %d' % (total_completed.value, NUM_FILES))
        kill_workers()
        os.rmdir('testing_files')

if __name__ == '__main__':
    unittest.main()
