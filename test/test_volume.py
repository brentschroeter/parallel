#!/usr/bin/env python

import parallel
import unittest
import multiprocessing
import time
import threading

WORKER_POOL = [('localhost:5000', 'localhost:5001'), ('localhost:5002', 'localhost:5003'), ('localhost:5004', 'localhost:5005')]

def get_queue_len(queue):
    length = 0
    while True:
        try:
            queue.get(False)
            length += 1
        except:
            break
    return length


def wait_job(ms):
    time.sleep(ms * 0.001)
    return 1


def run_job_trickle(run_job):
    for i in range(5):
        time.sleep(5)
        run_job(wait_job, (1000))

def run_job_spike(run_job):
    for i in range(500):
        run_job(wait_job, (10))

def run_jobs_steadily(run_job):
    for i in range(5):
        time.sleep(3)
        for j in range(100):
            run_job(wait_job, (100))

def run_jobs_with_pattern(run_job, usage_pattern):
    if usage_pattern == 'trickle':
        run_job_trickle(run_job)
    elif usage_pattern == 'spike':
        run_job_spike(run_job)
    else:
        run_jobs_steadily(run_job)

def push(vent_port, sink_port, results_queue, usage_pattern):
    def result_received(result, job_id):
        results_queue.put(result)
        
    worker, close, run_job = parallel.construct_worker(WORKER_POOL, {'vent_port': vent_port, 'sink_port': sink_port})
    t = threading.Thread(target=worker, args=[result_received])
    t.start()
    run_jobs_with_pattern(run_job, usage_pattern)
    time.sleep(1.5)


def work(vent_port, sink_port):
    def result_received(job_id, result):
        pass

    worker, close, run_job = parallel.construct_worker(WORKER_POOL, {'vent_port': vent_port, 'sink_port': sink_port})
    worker(result_received)


class TestParallel(unittest.TestCase):
#   def test_trickle(self):
#       queue = multiprocessing.Queue()
#       p1 = multiprocessing.Process(target=push, args=('5000', '5001', queue, 'trickle'))
#       p2 = multiprocessing.Process(target=work, args=('5002', '5003'))
#       p3 = multiprocessing.Process(target=work, args=('5004', '5005'))
#       p1.start()
#       p2.start()
#       p3.start()
#       time.sleep(30)
#       q_len = get_queue_len(queue)
#       p1.terminate()
#       p2.terminate()
#       p3.terminate()
#       self.assertEqual(q_len, 5)

    def test_spike(self):
        print 'WARNING: Test not functioning properly.'
        queue = multiprocessing.Queue()
        p1 = multiprocessing.Process(target=push, args=('5000', '5001', queue, 'spike'))
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
        self.assertEqual(q_len, 500)

#   def test_steady(self):
#       queue = multiprocessing.Queue()
#       p1 = multiprocessing.Process(target=push, args=('5000', '5001', queue, 'steady'))
#       p2 = multiprocessing.Process(target=work, args=('5002', '5003'))
#       p3 = multiprocessing.Process(target=work, args=('5004', '5005'))
#       p1.start()
#       p2.start()
#       p3.start()
#       time.sleep(20)
#       q_len = get_queue_len(queue)
#       p1.terminate()
#       p2.terminate()
#       p3.terminate()
#       self.assertEqual(q_len, 500)

if __name__ == '__main__':
    unittest.main()
