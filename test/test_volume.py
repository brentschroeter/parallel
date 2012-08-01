#!/usr/bin/env python

import parallel
import unittest
import multiprocessing
import time
import threading

WORKER_POOL = [('localhost:5000', 'localhost:5001'), ('localhost:5002', 'localhost:5003'), ('localhost:5004', 'localhost:5005')]
# each usage pattern is in the format (sets, sleep time between sets, repetitions per set, sleep time for each job)
USAGE_PATTERNS = ((5, 5000, 1, 1000), (500, 0, 1, 10), (5, 3000, 100, 100))


def wait_job(ms):
    time.sleep(ms * 0.001)
    return 1

def get_timeout(usage_pattern, num_workers):
    sets = usage_pattern[0]
    set_wait = usage_pattern[1]
    reps = usage_pattern[2]
    rep_wait = usage_pattern[3]
    
    time_per_set = max(set_wait, reps * rep_wait / num_workers)

    return time_per_set * sets + 2000 # add 2 seconds for connecting and sending

def run_jobs_with_pattern(run_job, usage_pattern):
    for i in range(usage_pattern[0]):
        time.sleep(usage_pattern[1] * 0.001)
        for j in range(usage_pattern[2]):
            run_job(wait_job, (usage_pattern[3]))

def push(vent_port, sink_port, usage_pattern, on_completed):
    on_completed()
    total_jobs = usage_pattern[0] * usage_pattern[2]
    total_completed = [0] # stored as a list as a workaround for Python variable scoping "quirk"
    def result_received(result, job_id):
        total_completed[0] += 1
        if total_completed[0] == total_jobs:
            print 'completed.'
            on_completed()
        
    worker, close, run_job = parallel.construct_worker(WORKER_POOL, {'vent_port': vent_port, 'sink_port': sink_port})
    t = threading.Thread(target=run_jobs_with_pattern, args=[run_job, usage_pattern])
    t.start()
    worker(result_received)

def work(vent_port, sink_port):
    def result_received(job_id, result):
        pass

    worker, close, run_job = parallel.construct_worker(WORKER_POOL, {'vent_port': vent_port, 'sink_port': sink_port})
    worker(result_received)

class TestParallel(unittest.TestCase):
    def test_volume(self):
        completed = [False]
        def on_completed():
            completed
            completed[0] = True
        def check_for_completion(timeout):
            tstart = time.time()
            while (time.time() - tstart) * 1000 < timeout:
                if completed[0]:
                    return True
            return False
        for usage_pattern in USAGE_PATTERNS:
            completed[0] = False
            p1 = multiprocessing.Process(target=push, args=('5000', '5001', usage_pattern, on_completed))
            p2 = multiprocessing.Process(target=work, args=('5002', '5003'))
            p3 = multiprocessing.Process(target=work, args=('5004', '5005'))
            p1.start()
            p2.start()
            p3.start()
            timeout = get_timeout(usage_pattern, 3)
            if not check_for_completion(timeout):
                self.fail('Did not complete tasks before timeout.')
            p1.terminate()
            p2.terminate()
            p3.terminate()

if __name__ == '__main__':
    unittest.main()
