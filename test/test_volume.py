#!/usr/bin/env python

import parallel
import unittest
import time
import threading
import testing_lib
import config
from multiprocessing import RawValue

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

def send_jobs(run_job, args):
    pattern, = args
    t = threading.Thread(target=run_jobs_with_pattern, args=[run_job, pattern])
    t.start()

def on_recv_result(result, job_info, args):
    total_completed, = args
    total_completed.value += 1

class TestParallel(unittest.TestCase):
    def test_volume(self):
        total_completed = RawValue('i')
        for pattern in config.USAGE_PATTERNS:
            total_completed.value = 0
            total_jobs = pattern.sets * pattern.set_reps

            start_workers, kill_workers = testing_lib.construct_worker_pool(len(config.WORKER_ADDRESSES), config.WORKER_ADDRESSES, send_jobs, (pattern,), on_recv_result, (total_completed,))
            start_workers()
            if not testing_lib.check_for_completion(total_completed, total_jobs, get_timeout(pattern, len(config.WORKER_ADDRESSES))):
                self.fail('Failed on usage pattern: %s' % str(pattern))
            kill_workers()

if __name__ == '__main__':
    unittest.main()
