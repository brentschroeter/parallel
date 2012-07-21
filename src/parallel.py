#!/usr/bin/env python

# Created by Brent Schroeter with Luke Carmichael.

import zmq
import uuid
import pickle
import time
import Queue
from zmq.core.error import ZMQError

# TODO make these configurable
VENT_PORT_DEFAULT = '5000'
SINK_PORT_DEFAULT = '5001'

def worker_loop(context, queue, addresses, callback, vent_port, sink_port):
    vent_sender = context.socket(zmq.PUSH)
    vent_sender.bind('tcp://*:%s' % vent_port)
    time.sleep(0.5)
    poller = zmq.Poller()
    sink_receiver = context.socket(zmq.PULL)
    sink_receiver.bind('tcp://*:%s' % sink_port)
    poller.register(sink_receiver, zmq.POLLIN)
    connections = []
    for address in addresses:
        vent_addr = 'tcp://%s:%s' % (address, vent_port)
        sink_addr = 'tcp://%s:%s' % (address, sink_port)
        tmp_receiver = context.socket(zmq.PULL)
        tmp_receiver.connect(vent_addr)
        poller.register(tmp_receiver, zmq.POLLIN)
        tmp_sender = context.socket(zmq.PUSH)
        tmp_sender.connect(sink_addr)
        connections.append((address, tmp_receiver, tmp_sender))
    while True:
        while not queue.empty():
            job_tuple = queue.get()
            vent_sender.send(pickle.dumps(job_tuple))
        while True:
            try:
                s = sink_receiver.recv(zmq.NOBLOCK)
                result, job_id = pickle.loads(s)
                callback(result, job_id)
            except ZMQError:
                break
        socks = dict(poller.poll(500))
        for address, receiver, sender in connections:
            if socks.get(receiver):
                s = receiver.recv()
                job, job_id = pickle.loads(s)
                result = job()
                reply = (result, job_id)
                sender.send(pickle.dumps(reply))

def construct_worker(addresses, config={}):
    vent_port = config.get('vent_port', VENT_PORT_DEFAULT)
    sink_port = config.get('sink_port', SINK_PORT_DEFAULT)

    context = zmq.Context()
    queue = Queue.Queue()

    def worker(callback):
        worker_loop(context, queue, addresses, callback, vent_port, sink_port)

    def close():
        # should we destroy???
        context.destroy()

    def run_job(job):
        job_id = str(uuid.uuid4())
        queue.put((job, job_id))
        return job_id

    return worker, close, run_job