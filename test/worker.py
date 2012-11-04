#!/usr/bin/env python

import parallel
import testing_lib
import sys

def main():
    '''
    Runs creates and runs one worker.
    example usage: ./worker.py localhost 10.0.1.3 10.0.1.4
    '''
    worker_addresses = testing_lib.worker_addresses(sys.argv[1:])
    worker, close, run_job = parallel.construct_worker(worker_addresses)
    print 'Worker running with default ports...'
    worker(None)

if __name__ == '__main__':
    main()
