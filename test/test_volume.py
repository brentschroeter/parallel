#!/usr/bin/env python

import parallel
import unittest
import multiprocessing
import time
import threading
from collections import namedtuple

UsagePattern = namedtuple('UsagePattern', 'sets set_sleep set_reps job_sleep')

WORKER_POOL = [('localhost:5000', 'localhost:5001'), ('localhost:5002', 'localhost:5003'), ('localhost:5004', 'localhost:5005')]
NUM_WORKERS = 2
USAGE_PATTERNS = (UsagePattern(5, 5000, 1, 1000), UsagePattern(500, 0, 1, 10), UsagePattern(5, 3000, 100, 100))

def construct_worker_pool(num, push_args):
    processes = []
    def start(start_port=5000):
        p = multiprocessing.Process(target=push, args=(start_port, start_port + 1, push_args))
        p.start()
        processes.append(p)
        for i in range(num - 1):
            start_port += 2
            p = multiprocessing.Process(target=work, args=(start_port, start_port + 1))
            p.start()
            processes.append(p)
    def kill():
        for p in processes:
            p.terminate()
    return start, kill

def wait_job(ms):
    time.sleep(ms * 0.001)
    return 1

def get_timeout(pattern, num_workers):
    time_per_set = max(pattern.set_sleep, pattern.set_reps * pattern.job_sleep / num_workers)
    transportation_time = 30 * (pattern.sets * pattern.set_reps) + 1000

    return time_per_set * pattern.sets + transportation_time

def run_jobs_with_pattern(run_job, pattern):
    for i in range(pattern.sets):
        time.sleep(pattern.set_sleep * 0.001)
        for j in range(pattern.set_reps):
            run_job(wait_job, (pattern.job_sleep))

def push(vent_port, sink_port, args):
    pattern, result_received = args
    worker, close, run_job = parallel.construct_worker(WORKER_POOL, {'vent_port': vent_port, 'sink_port': sink_port})
    t = threading.Thread(target=run_jobs_with_pattern, args=[run_job, pattern])
    t.start()
    worker(result_received)

def work(vent_port, sink_port):
    def result_received(job_id, result):
        pass
    worker, close, run_job = parallel.construct_worker(WORKER_POOL, {'vent_port': vent_port, 'sink_port': sink_port})
    worker(result_received)

class TestParallel(unittest.TestCase):
    def test_volume(self):
        total_completed = multiprocessing.RawValue('i')
        def result_received(result, job_id):
            total_completed.value += 1
        for pattern in USAGE_PATTERNS:
            def check_for_completion():
                total_jobs = pattern.sets * pattern.set_reps
                timeout = get_timeout(pattern, 3)
                tstart = time.time()
                while (time.time() - tstart) < timeout * 0.001:
                    if total_completed.value == total_jobs:
                        return True
                return False
            total_completed.value = 0
            push_args = (pattern, result_received)
            start_workers, kill_workers = construct_worker_pool(NUM_WORKERS, push_args)
            start_workers()
            if not check_for_completion():
                self.fail('Failed on usage pattern: %s' % str(pattern))
            else:
                kill_workers()

if __name__ == '__main__':
    unittest.main()
