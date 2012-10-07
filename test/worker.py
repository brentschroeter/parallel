#!/usr/bin/env python

import parallel
import testing_lib
import sys

def main():
    worker_addresses = testing_lib.worker_addresses(sys.argv[1:])
    worker, close, run_job = parallel.construct_worker(worker_pool)
    print 'Worker running with default ports...'
    worker(None)

if __name__ == '__main__':
    main()
