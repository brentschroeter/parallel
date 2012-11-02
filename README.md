Parallel
========

Simple ZMQ-driven multiprocessing library.

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
