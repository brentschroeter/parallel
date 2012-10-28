#!/usr/bin/env python

import parallel
import unittest
import thread
import testing_lib
import time
import uuid
import config
from multiprocessing import RawValue

def wait_job(ms):
    time.sleep(ms * 0.001)
    return 1

def get_timeout(num_workers):
    transportation_time = testing_lib.TRANSPORT_MS * config.NUM_JOBS * config.NUM_PUSHERS + 1000
    working_time = config.WAIT_TIME * config.NUM_JOBS * config.NUM_PUSHERS

    return working_time + transportation_time

def send_jobs(run_job, args):
    for i in range(config.NUM_JOBS):
        run_job(wait_job, (config.WAIT_TIME))

def on_recv_result(result, job_info, args):
    total_completed, = args
    total_completed.value += 1

class TestParallel(unittest.TestCase):
    def test_multipush(self):
        total_completed = RawValue('i')
        total_completed.value = 0

        start_workers, kill_workers = testing_lib.construct_worker_pool(config.num_local_workers(), config.WORKER_ADDRESSES, send_jobs, (), on_recv_result, (total_completed,), num_pushers=config.NUM_PUSHERS)
        start_workers()
        completion = testing_lib.check_for_completion(total_completed, config.NUM_JOBS * config.NUM_PUSHERS, get_timeout(len(config.WORKER_ADDRESSES)))
        kill_workers()
        if not completion:
            self.fail('Not all jobs received: %d / %d' % (total_completed.value, config.NUM_JOBS * config.NUM_PUSHERS))

if __name__ == '__main__':
    unittest.main()
