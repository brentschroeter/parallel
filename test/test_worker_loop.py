#!/usr/bin/env python

import parallel
import zmq
import Queue

def main():
    context = zmq.Context()
    queue = Queue.Queue()
    parallel.worker_loop(context, queue)

if __name__ == '__main__':
    main()