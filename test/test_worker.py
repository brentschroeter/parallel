#!/usr/bin/env python

import parallel
import zmq
import Queue

def main():
    worker, close, run_job = parallel.construct_worker([])
    for i in range(10):
        run_job('Test job: %d' % i)
    worker()

if __name__ == '__main__':
    main()