#!/usr/bin/env python

import parallel
import unittest
import random
import os
import testing_lib
import config
from multiprocessing import RawValue

def file_job(file_contents):
    path = 'testing_files/%s' % str(random.randint(0, 999999)) + '.txt'
    f = open(path, 'w')
    f.write(file_contents)
    f.close()
    return path

def get_timeout(num_workers):
    transportation_time = testing_lib.TRANSPORT_MS * config.NUM_FILES + 1000
    working_time = 10 * config.NUM_FILES

    return working_time + transportation_time

class TestParallel(unittest.TestCase):
    def test_files(self):
        total_completed = RawValue('i')
        total_completed.value = 0
        def on_recv_result(result, job_info):
            try:
                f = open(result)
                file_contents = f.read()
                f.close()
                os.remove(result)
                self.assertEquals(file_contents, config.TESTING_STR)
            except:
                self.fail('File not present.')
            total_completed.value += 1
        def send_jobs(run_job):
            for i in range(config.NUM_FILES):
                run_job(file_job, (config.TESTING_STR))

        if not os.path.exists(os.path.join(os.path.dirname(__file__), 'testing_files/')):
            os.mkdir('testing_files')
        start_workers, kill_workers = testing_lib.construct_worker_pool(config.num_local_workers(), config.WORKER_ADDRESSES, send_jobs, on_recv_result)
        start_workers()
        if not testing_lib.check_for_completion(total_completed, config.NUM_FILES, get_timeout(len(config.WORKER_ADDRESSES))):
            self.fail('Not all jobs received: %d / %d' % (total_completed.value, config.NUM_FILES))
        kill_workers()
        os.rmdir('testing_files')

if __name__ == '__main__':
    unittest.main()
