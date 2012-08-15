#!/usr/bin/env python

import parallel
import unittest
import thread
import testing_lib
import time
import uuid
from multiprocessing import RawValue

NUM_JOBS = 20
NUM_WORKERS = 3
WAIT_TIME = 100
WORKER_POOL = [('localhost:5000', 'localhost:5001'), ('localhost:5002', 'localhost:5003'), ('localhost:5004', 'localhost:5005')]

def wait_job(ms):
    time.sleep(ms * 0.001)
    return 1

def get_timeout(num_workers):
    transportation_time = testing_lib.TRANSPORT_MS * NUM_JOBS + 1000
    working_time = WAIT_TIME * NUM_JOBS

    return working_time + transportation_time

def check_load_balance(worker_ids):
    try:
        for i in range(min(len(worker_ids), NUM_JOBS)):
            worker_id = worker_ids[i]
            log_file = open('worker_logs/%s.txt' % worker_id)
            if len(log_file.read()) == 0:
                return False
    except IOError:
        return False
    return True

class TestParallel(unittest.TestCase):
    def test_multiprocessing(self):
        total_completed = RawValue('i')
        def result_received(result, job_id):
            total_completed.value += 1
        def check_load_balance(worker_ids):
            try:
                for i in range(min(len(worker_ids), NUM_JOBS)):
                    worker_id = worker_ids[i]
                    log_file = open('worker_logs/%s.txt' % worker_id)
                    if len(log_file.read()) == 0:
                        return False
            except IOError:
                return False
            return True
        def push(vent_port, sink_port, worker_pool, worker_id):
            worker, close, run_job = parallel.construct_worker(worker_pool, {'vent_port': vent_port, 'sink_port': sink_port, 'worker_id': worker_id})
            for i in range(NUM_JOBS):
                run_job(wait_job, (WAIT_TIME))
            worker(result_received)

        total_completed.value = 0
        start_workers, kill_workers, get_worker_ids = testing_lib.construct_worker_pool(NUM_WORKERS, WORKER_POOL, push, logging=True)
        start_workers()
        completion = testing_lib.check_for_completion(total_completed, NUM_JOBS, get_timeout(NUM_WORKERS))
        kill_workers()
        if not completion:
            self.fail('Not all jobs received: %d / %d' % (total_completed.value, NUM_JOBS))
        if not check_load_balance(get_worker_ids()):
            self.fail('Not all workers utilized.')

    def test_threading(self):
        total_completed = RawValue('i')
        def result_received(result, job_id):
            total_completed.value += 1
        def push(vent_port, sink_port, worker_pool, worker_id):
            worker, close, run_job = parallel.construct_worker(worker_pool, {'vent_port': vent_port, 'sink_port': sink_port, 'worker_id': worker_id})
            for i in range(NUM_JOBS):
                run_job(wait_job, (WAIT_TIME))
            worker(result_received)

        total_completed.value = 0
        port = 5000
        worker_ids = []
        worker_ids.append(uuid.uuid4())
        thread.start_new_thread(push, (port, port + 1, WORKER_POOL, worker_ids[-1]))
        for i in range(NUM_WORKERS - 1):
            port += 2
            worker_ids.append(uuid.uuid4())
            thread.start_new_thread(testing_lib.work, (port, port + 1, WORKER_POOL, worker_ids[-1]))
        if not testing_lib.check_for_completion(total_completed, NUM_JOBS, get_timeout(NUM_WORKERS)):
            self.fail('Not all jobs received: %d / %d' % (total_completed.value, NUM_JOBS))
        if not check_load_balance(worker_ids):
            self.fail('Not all workers utilized.')

if __name__ == '__main__':
    unittest.main()
