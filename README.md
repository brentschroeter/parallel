Parallel
========

Parallel task processing using Python and ZeroMQ.

Overview
-------------

Every ParallelClient object launches a ParallelWorker on a different thread that connects to a provided list of addresses (which includes the client's own address) in order to listen for work. If more than one CPU core is available, more ParallelWorkers will be created. Every ParallelClient object has the ability to push jobs to connected workers.

Testing on Many Machines
----------------------------------

Make sure that all of the computers are networked and that any firewalls are down. Install ZeroMQ and fire up `./test_network.py [vent port] [sink port] [IP addresses]` on each computer. The vent and sink ports should be the same on each computer and should not be 5000 (port 5000 is used internally). IP addresses is just what it sounds like: a list of IP addresses for the other servers (not including the address of the current machine; that is added automatically). Note that a server does not have to be running before it is added to the list. Each computer now has a worker running and is awaiting commands. Type `push` at the prompt on any computer to begin pushing jobs similarly to the one-machine test.