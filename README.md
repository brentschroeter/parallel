Parallel
========

Parallel task processing using Python and ZeroMQ.

Overview
-------------

Every ImgPlusClient object launches a new worker on a new thread that connects to a provided list of addresses (which includes the client's own address) to listen for work. Every ImgPlusClient object also has the ability to push jobs to any connected workers.

Simple Testing
-------------------

To run a test, add the source directory to your PYTHONPATH and run test_parallel.py in the test directory with two arguments. The first argument is a port number. The script will start with this port and then count up until it reaches the number of ports it needs. The second argument is the number of clients to start. On my machine, the maximum is 10 due to Python's open file limitations. For example, typing `./test_parallel.py 5000 10` will cause the following chain of events:

-   10 clients are created, occupying ports 5000-5019. As many threads are started, each with its own worker.
-   All of the clients sit quietly as the script sleeps for 5 seconds.
-   The script calls the `distribute()` method of the first client, which pushes 1000 jobs (each of which sleeps for about half a second).
-   The 10 workers begin pulling jobs and running them in parallel. As they finish, they push back a response (always 1 in this case) to the original client.
- The client waits until all of the jobs have been received or have timed out and been retried sufficiently.
- The script then quits.