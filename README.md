Parallel
========

The Parallel project allows for easy parallel processing on networked clients and on multi-core machines.

Overview
-------------

Every ParallelClient object launches a ParallelWorker on a different thread that connects to a provided list of addresses (which includes the client's own address) in order to listen for work. If more than one CPU core is available, more ParallelWorkers will be created. Every ParallelClient object has the ability to push jobs to connected workers.

Testing on Many Machines
----------------------------------

Make sure that all of the computers are networked and that all firewalls are configured correctly. **This could mean disabling the firewall completely.** Install ZeroMQ and fire up `./test_network.py` on each computer. Each computer now has a worker running and is awaiting commands. Type `push` at the prompt on any computer to begin pushing jobs. Each job sleeps for about a half of a second and then returns 1.