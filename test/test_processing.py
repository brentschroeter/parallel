#!/usr/bin/env python

import parallel

def job(args):
    return 1

def main():
    worker, close, run_job = parallel.construct_worker(['localhost'])

    def process(result, job_id):
        print 'Result returned.'

    for i in range(10):
        run_job(job)

    worker(process)

if __name__ == '__main__':
    main()