#!/usr/bin/env python

import parallel
import unittest
import thread
import testing_lib
import time
import uuid
import Queue
import config
from multiprocessing import RawValue
from multiprocessing import Queue as MultiQueue

def wait_job(ms):
    time.sleep(ms * 0.001)
    return 1

def get_timeout(num_workers):
    transportation_time = testing_lib.TRANSPORT_MS * config.NUM_JOBS + 1000
    working_time = config.WAIT_TIME * config.NUM_JOBS

    return working_time + transportation_time

def check_load_balance(job_processors):
    used_workers = []
    while True:
        try:
            worker = job_processors.get_nowait()
            if not worker in used_workers:
                used_workers.append(worker)
        except Queue.Empty:
            break
    return len(used_workers) >= min(config.NUM_JOBS, len(config.WORKER_ADDRESSES))

def send_jobs(run_job, args):
    for i in range(config.NUM_JOBS):
        run_job(wait_job, (config.WAIT_TIME))

def on_recv_result(result, job_info, args):
    total_completed, job_processors = args
    job_processors = args[1]
    total_completed.value += 1
    job_processors.put(job_info.worker_id)

class TestParallel(unittest.TestCase):
    def test_multiprocessing(self):
        total_completed = RawValue('i')
        job_processors = MultiQueue()
        total_completed.value = 0

        start_workers, kill_workers = testing_lib.construct_worker_pool(config.num_local_workers(), config.WORKER_ADDRESSES, send_jobs, (), on_recv_result, (total_completed, job_processors))
        start_workers()
        completion = testing_lib.check_for_completion(total_completed, config.NUM_JOBS, get_timeout(len(config.WORKER_ADDRESSES)))
        kill_workers()
        if not completion:
            self.fail('Not all jobs received: %d / %d' % (total_completed.value, config.NUM_JOBS))
        if not check_load_balance(job_processors):
            self.fail('Not all workers utilized.')

    def test_threading(self):
        total_completed = RawValue('i')
        job_processors = MultiQueue()
        def push(vent_port, sink_port, worker_pool):
            worker, close, run_job = parallel.construct_worker(worker_pool, {'vent_port': vent_port, 'sink_port': sink_port})
            for i in range(config.NUM_JOBS):
                run_job(wait_job, (config.WAIT_TIME))
            worker(on_recv_result, (total_completed, job_processors))

        total_completed.value = 0
        port = 5000
        thread.start_new_thread(push, (port, port + 1, config.WORKER_ADDRESSES))
        for i in range(config.num_local_workers() - 1):
            port += 2
            thread.start_new_thread(testing_lib.work, (port, port + 1, config.WORKER_ADDRESSES))
        if not testing_lib.check_for_completion(total_completed, config.NUM_JOBS, get_timeout(len(config.WORKER_ADDRESSES))):
            self.fail('Not all jobs received: %d / %d' % (total_completed.value, len(config.WORKER_ADDRESSES)))
        if not check_load_balance(job_processors):
            self.fail('Not all workers utilized.')

if __name__ == '__main__':
    unittest.main()
