Parallel
========

Simple ZMQ-driven multiprocessing library.

Usage
-----

    import parallel

    worker_addresses = [('localhost:5000', 'localhost:5001'), ('10.0.1.34:5000', '10.0.1.34:5001')]
    worker, close, run_job = parallel.construct_worker(worker_addresses)

    def sample_job(num):
        # really time-consuming calculation
        return num + 3
    
    def on_recv_result(result, job_info, args):
        print 'Job #', job_info[2], ' returned the result: ', result

    for num in range(100):
        run_job(sample_job, (num,))

    worker(on_recv_result, ())
