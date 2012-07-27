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
def send_pending_jobs(pending_q, sender, sent_jobs, max_sent_jobs):
    for i in range(max_sent_jobs - len(sent_jobs)):
        if pending_q.empty():
            break
        job_tuple = pending_q.get()
        sender.send(pickle.dumps(job_tuple))
        sent_jobs.append(job_tuple[2])

# run a job and return the result. assumes that there is a message queued for the receiver.
def process_job(receiver, sender):
    s = receiver.recv()
    job, args, job_id = pickle.loads(s)
    result = job(args)
    reply = (result, job_id)
    sender.send(pickle.dumps(reply))

# add a set of sockets to the poller and list of connections
def add_connection(context, vent_addr, sink_addr):
    vent_addr = 'tcp://%s' % vent_addr
    sink_addr = 'tcp://%s' % sink_addr
    receiver = context.socket(zmq.PULL)
    receiver.connect(vent_addr)
    sender = context.socket(zmq.PUSH)
    sender.connect(sink_addr)
    return receiver, sender

# push jobs, handle replies, and process incoming jobs
def worker_loop(context, pending_q, addresses, callback, vent_port, sink_port):
    vent_sender = context.socket(zmq.PUSH)
    vent_sender.bind('tcp://*:%s' % vent_port)
    time.sleep(0.5)
    poller = zmq.Poller()
    sink_receiver = context.socket(zmq.PULL)
    sink_receiver.bind('tcp://*:%s' % sink_port)
    poller.register(sink_receiver, zmq.POLLIN)
    connections = []
    for vent_addr, sink_addr in addresses:
        tmp_receiver, tmp_sender = add_connection(context, vent_addr, sink_addr)
        connections.append((tmp_receiver, tmp_sender))
        poller.register(tmp_receiver, zmq.POLLIN)
    approx_workers = len(connections)
    sent_jobs = []
    while True:
        send_pending_jobs(pending_q, vent_sender, sent_jobs, approx_workers)
        while True:
            try:
                s = sink_receiver.recv(zmq.NOBLOCK)
                result, job_id = pickle.loads(s)
                if job_id in sent_jobs:
                    callback(result, job_id)
                    sent_jobs.remove(job_id)
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
    pending_q = Queue.Queue()

    def worker(callback):
        worker_loop(context, pending_q, addresses, callback, vent_port, sink_port)

    def close():
        context.destroy()

    def run_job(job, args=()):
        job_id = str(uuid.uuid4())
        pending_q.put((job, args, job_id))
        return job_id

    return worker, close, run_job
