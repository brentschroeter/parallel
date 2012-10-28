#!/usr/bin/env python

import parallel
import unittest
import string
import random
import testing_lib
import sys
import config
from multiprocessing import RawValue
from guppy import hpy

def str_job(s):
    # reverse the string
    return s[::-1]

def generate_str(length):
    return ''.join(random.choice(string.ascii_uppercase) for x in range(length))

def get_timeout():
    transportation_time = testing_lib.TRANSPORT_MS * config.NUM_STRINGS + 1000
    computation_time = 0.00004 * config.NUM_STRINGS * config.STR_LEN
    return transportation_time + computation_time

def send_jobs(run_job, args):
    test_str, = args
    hp = hpy()
    test_str = generate_str(config.STR_LEN)
    for i in range(config.NUM_STRINGS):
        run_job(str_job, (test_str))
    size = hp.heap().size
    if size > config.MEM_LIMIT:
        self.fail('Heap size exceeded %d bytes (%d).' % (config.MEM_LIMIT, size))

def on_recv_result(result, job_info, args):
    total_completed, = args
    total_completed.value += 1

class TestParallel(unittest.TestCase):
    def test_mem(self):
        '''Tests that memory required to transfer jobs does not exceed the given amount.'''
        total_completed = RawValue('i')
        print 'Constructing test string (this could take some time).'
        test_str = generate_str(config.STR_LEN)
        print 'Done. Starting test.'
        total_completed.value = 0

        start_workers, kill_workers = testing_lib.construct_worker_pool(config.num_local_workers(), config.WORKER_ADDRESSES, send_jobs, (test_str,), on_recv_result, (total_completed,))
        start_workers()
        completion = testing_lib.check_for_completion(total_completed, config.NUM_STRINGS, get_timeout())
        kill_workers()
        if not completion:
            self.fail('Not all jobs received: %d / %d' % (total_completed.value, config.NUM_STRINGS))

if __name__ == '__main__':
    unittest.main()
