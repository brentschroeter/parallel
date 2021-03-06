Python Parallel Processing Library
========

The `parallel` Python library provides a set of functions for simple distributed computation. It uses the ZeroMQ socket library for reliable data transport across any size of network.

Usage
-----

    import parallel

    # list containing (ventilator, sink) address pairs
    worker_addresses = [('localhost:5000', 'localhost:5001'), ('10.0.1.34:5000', '10.0.1.34:5001')]
    # construct functions to control parallel processing
    worker, close, run_job = parallel.construct_worker(worker_addresses)

    # job function
    def sample_job(num):
        return num + 3
    
    # called when a job is done processing
    def on_recv_result(result, job_info, args):
        print 'Job #', job_info.job_id, ' returned the result: ', result

    # run lots of jobs
    for num in range(100):
        run_job(sample_job, (num,))

    # send jobs, pull and process jobs, and handle completed jobs
    worker(on_recv_result, ())

Example Flow
----

Workers are constructed and run on two different machines with `parallel.construct_worker()` and the returned `worker()` function. Another worker is constructed on a third machine. After (or before) the `worker()` function is called jobs are added to the queue with `run_job()`. Because there are three workers, the third worker sends the first three jobs and waits for replies. All three of the workers are watching for jobs and they each grab one of the first three jobs. When one finishes processing the job it sends back the result. The third worker receives the result and sends out another job.
