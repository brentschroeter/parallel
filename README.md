Parallel
========

Parallel task processing using Python and ZeroMQ.

Overview
-------------

Every ParallelClient object launches a new worker on a new thread that connects to a provided list of addresses (which includes the client's own address) to listen for work. Every ParallelClient object also has the ability to push jobs to any connected workers.

Testing on One Machine
-------------------

To run a one-machine test, add the source directory to your PYTHONPATH and run test_parallel.py in the test directory with two arguments. The first argument is a port number. The script will start with this port and then count up until it reaches the number of ports it needs. The second argument is the number of clients to start. On my machine, the maximum is 10 due to Python's open file limitations. For example, typing `./test_parallel.py 5000 10` will cause the following chain of events:

-   10 clients are created, occupying ports 5000-5019. As many threads are started, each with its own worker.
-   All of the clients sit quietly as the script sleeps for 5 seconds.
-   The script calls the `distribute()` method of the first client, which pushes 1000 jobs (each of which sleeps for about half a second).
-   The 10 workers begin pulling jobs and running them in parallel. As they finish, they push back a response (always 1 in this case) to the original client.
- The client waits until all of the jobs have been received or have timed out and been retried sufficiently.
- The script then quits.

Testing on Many Machines
----------------------------------

Make sure that all of the computers are networked and that any firewalls are down. Install ZeroMQ and fire up `./test_network.py [vent port] [sink port] [IP addresses]` on each computer. The vent and sink ports should be the same on each computer and should not be 5000 (port 5000 is used internally). IP addresses is just what it sounds like: a list of IP addresses for the other servers (not including the address of the current machine; that is added automatically). Note that a server does not have to be running before it is added to the list. Each computer now has a worker running and is awaiting commands. Type `push` at the prompt on any computer to begin pushing jobs similarly to the one-machine test.