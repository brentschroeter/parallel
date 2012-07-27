#!/usr/bin/env python

import parallel
import unittest
import multiprocessing
import time
import random
import os

TESTING_STR = 'Testing testing testing.'

def file_job(file_contents):
    path = 'testing_files/%s' % str(random.randint(0, 999999)) + '.txt'
    f = open(path, 'w')
    f.write(file_contents)
    f.close()
    return path

class TestParallel(unittest.TestCase):
    def test_files(self):
        def push(vent_port, sink_port, results_queue, file_contents):
            def result_received(result, job_id):
                results_queue.put(result)
                
            worker, close, run_job = parallel.construct_worker([('localhost:5000', 'localhost:5001'), ('localhost:5002', 'localhost:5003'), ('localhost:5004', 'localhost:5005')], {'vent_port': vent_port, 'sink_port': sink_port})
            for i in range(5):
                run_job(file_job, (file_contents))
            time.sleep(1.5)
            worker(result_received)

        def work(vent_port, sink_port):
            def result_received(job_id, result):
                pass

            worker, close, run_job = parallel.construct_worker([('localhost:5000', 'localhost:5001'), ('localhost:5002', 'localhost:5003'), ('localhost:5004', 'localhost:5005')], {'vent_port': vent_port, 'sink_port': sink_port})
            worker(result_received)
        
        if not os.path.exists(os.path.join(os.path.dirname(__file__), 'testing_files/')):
            os.mkdir('testing_files')
        queue = multiprocessing.Queue()
        p1 = multiprocessing.Process(target=push, args=('5000', '5001', queue, TESTING_STR))
        p2 = multiprocessing.Process(target=work, args=('5002', '5003'))
        p3 = multiprocessing.Process(target=work, args=('5004', '5005'))
        p1.start()
        p2.start()
        p3.start()
        time.sleep(6)
        p1.terminate()
        p2.terminate()
        p3.terminate()
        total_results = 0
        while True:
            try:
                result = queue.get(False)
            except:
                break
            total_results += 1
            try:
                f = open(result)
                file_contents = f.read()
                f.close()
                os.remove(result)
                self.assertEquals(file_contents, TESTING_STR)
            except:
                self.fail('File not present.')
        os.rmdir('testing_files')
        self.assertEquals(total_results, 5)

if __name__ == '__main__':
    unittest.main()
