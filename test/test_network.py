#!/usr/bin/env python

import parallel
import time

def wait_job(ms):
    time.sleep(ms * 0.001)
    return 1

def main():
    worker_pool = []
    print 'Enter the IP addresses of other clients. Enter a blank line when you are finished.'
    address = 'localhost'
    while address != '':
        worker_pool.append(('%s:5000' % address, '%s:5001' % address))
        address = raw_input('Address: ')

    worker, close, run_job = parallel.construct_worker(worker_pool)

    for i in range(60):
        run_job(wait_job, (1000))

    def process_reply(result, job_id):
        print 'Job completed: ', job_id

    worker(process_reply)

if __name__ == '__main__':
    main()
