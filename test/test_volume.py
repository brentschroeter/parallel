#!/usr/bin/env python

import parallel
import unittest
import time
import threading
import testing_lib
from collections import namedtuple
from multiprocessing import RawValue

UsagePattern = namedtuple('UsagePattern', 'sets set_sleep set_reps job_sleep')

WORKER_POOL = [('localhost:5000', 'localhost:5001'), ('localhost:5002', 'localhost:5003'), ('localhost:5004', 'localhost:5005')]
NUM_WORKERS = 2
USAGE_PATTERNS = (UsagePattern(5, 5000, 1, 1000), UsagePattern(500, 0, 1, 10), UsagePattern(5, 3000, 100, 100))

def wait_job(ms):
    time.sleep(ms * 0.001)
    return 1

def get_timeout(pattern, num_workers):
    time_per_set = max(pattern.set_sleep, pattern.set_reps * pattern.job_sleep / num_workers)
    transportation_time = testing_lib.TRANSPORT_MS * (pattern.sets * pattern.set_reps) + 1000

    return time_per_set * pattern.sets + transportation_time

def run_jobs_with_pattern(run_job, pattern):
    for i in range(pattern.sets):
        time.sleep(pattern.set_sleep * 0.001)
        for j in range(pattern.set_reps):
            run_job(wait_job, (pattern.job_sleep))

class TestParallel(unittest.TestCase):
    def test_volume(self):
        total_completed = RawValue('i')
        def result_received(result, job_id):
            total_completed.value += 1
        for pattern in USAGE_PATTERNS:
            def push(vent_port, sink_port, worker_pool):
                worker, close, run_job = parallel.construct_worker(worker_pool, {'vent_port': vent_port, 'sink_port': sink_port})
                t = threading.Thread(target=run_jobs_with_pattern, args=[run_job, pattern])
                t.start()
                worker(result_received)

            total_completed.value = 0
            total_jobs = pattern.sets * pattern.set_reps
            start_workers, kill_workers = testing_lib.construct_worker_pool(NUM_WORKERS, WORKER_POOL, push)
            start_workers()
            if not testing_lib.check_for_completion(total_completed, total_jobs, get_timeout(pattern, NUM_WORKERS)):
                self.fail('Failed on usage pattern: %s' % str(pattern))
            kill_workers()

if __name__ == '__main__':
    unittest.main()
