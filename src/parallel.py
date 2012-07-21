#!/usr/bin/env python

# Created by Brent Schroeter with Luke Carmichael.

import zmq
import uuid
import pickle
import time
import Queue
from zmq.core.error import ZMQError

VENT_PORT_DEFAULT = '5000'
SINK_PORT_DEFAULT = '5001'

# push any jobs waiting in the queue
def send_pending_jobs(queue, sender):
    while not queue.empty():
        job_tuple = queue.get()
        sender.send(pickle.dumps(job_tuple))

# run a job and return the result. assumes that there is a message queued for the receiver.
def process_job(receiver, sender):
    s = receiver.recv()
    job, job_id = pickle.loads(s)
    result = job()
    reply = (result, job_id)
    sender.send(pickle.dumps(reply))

# add a set of sockets to the poller and list of connections
def add_connection(context, poller, connections, address, vent_port, sink_port):
    vent_addr = 'tcp://%s:%s' % (address, vent_port)
    sink_addr = 'tcp://%s:%s' % (address, sink_port)
    tmp_receiver = context.socket(zmq.PULL)
    tmp_receiver.connect(vent_addr)
    poller.register(tmp_receiver, zmq.POLLIN)
    tmp_sender = context.socket(zmq.PUSH)
    tmp_sender.connect(sink_addr)
    connections.append((tmp_receiver, tmp_sender))

# push jobs, handle replies, and process incoming jobs
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
        add_connection(context, poller, connections, address, vent_port, sink_port)
    while True:
        send_pending_jobs(queue, vent_sender)
        while True:
            try:
                s = sink_receiver.recv(zmq.NOBLOCK)
                result, job_id = pickle.loads(s)
                callback(result, job_id)
            except ZMQError:
                break
        socks = dict(poller.poll(500))
        for receiver, sender in connections:
            if socks.get(receiver):
                process_job(receiver, sender)

# construct basic functions for handling parallel processing
def construct_worker(addresses, config={}):
    vent_port = config.get('vent_port', VENT_PORT_DEFAULT)
    sink_port = config.get('sink_port', SINK_PORT_DEFAULT)

    context = zmq.Context()
    queue = Queue.Queue()

    def worker(callback):
        worker_loop(context, queue, addresses, callback, vent_port, sink_port)

    def close():
        context.destroy()

    def run_job(job):
        job_id = str(uuid.uuid4())
        queue.put((job, job_id))
        return job_id

    return worker, close, run_job