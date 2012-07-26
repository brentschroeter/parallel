#!/usr/bin/env python

import parallel
import unittest
import multiprocessing
import time

def wait_job(ms):
    time.sleep(ms * 0.001)
    return 1

def get_queue_len(queue):
    length = 0
    while True:
        try:
            queue.get(False)
            length += 1
        except:
            break
    return length

class TestParallel(unittest.TestCase):
    def test_multiprocessing(self):
        def push(vent_port, sink_port, results_queue):
            def result_received(result, job_id):
                results_queue.put(result)
                
            worker, close, run_job = parallel.construct_worker([('localhost:5000', 'localhost:5001'), ('localhost:5002', 'localhost:5003'), ('localhost:5004', 'localhost:5005')], {'vent_port': vent_port, 'sink_port': sink_port})
            for i in range(5):
                run_job(wait_job, (1000))
            time.sleep(1.5)
            worker(result_received)

        def work(vent_port, sink_port):
            def result_received(job_id, result):
                pass

            worker, close, run_job = parallel.construct_worker([('localhost:5000', 'localhost:5001'), ('localhost:5002', 'localhost:5003'), ('localhost:5004', 'localhost:5005')], {'vent_port': vent_port, 'sink_port': sink_port})
            worker(result_received)
            
        queue = multiprocessing.Queue()
        p1 = multiprocessing.Process(target=push, args=('5000', '5001', queue))
        p2 = multiprocessing.Process(target=work, args=('5002', '5003'))
        p3 = multiprocessing.Process(target=work, args=('5004', '5005'))
        p1.start()
        p2.start()
        p3.start()
        time.sleep(6)
        q_len = get_queue_len(queue)
        p1.terminate()
        p2.terminate()
        p3.terminate()
        self.assertEqual(q_len, 5)

if __name__ == '__main__':
    unittest.main()
