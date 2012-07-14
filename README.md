Parallel
========

Parallel task processing using Python and ZeroMQ.

Overview
-------------

Every ImgPlusClient object launches a new worker on a new thread that connects to a provided list of addresses (which includes the client's own address) to listen for work. Every ImgPlusClient object also has the ability to push jobs to any connected workers.