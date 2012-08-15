#!/usr/bin/env python

# Created by Brent Schroeter with Luke Carmichael.

import zmq
import uuid
import pickle
import time
import Queue
import os.path
from zmq.core.error import ZMQError

VENT_PORT_DEFAULT = '5000'
SINK_PORT_DEFAULT = '5001'

# push any jobs waiting in the queue
def send_pending_jobs(queue, sender, sent_jobs, max_sent_jobs):
    for i in range(max_sent_jobs - len(sent_jobs)):
        try:
            job_tuple = queue.get(False)
            sender.send(pickle.dumps(job_tuple))
            sent_jobs.append(job_tuple[2])
        except Queue.Empty:
            break

# run a job and return the result. assumes that there is a message queued for the receiver.
def process_job(receiver, sender, log_path):
    try:
        s = receiver.recv(zmq.NOBLOCK)
        job, args, job_id = pickle.loads(s)
        result = job(args)
        reply = (result, job_id)
        sender.send(pickle.dumps(reply))
        log_txt = 'Successful: %s' % job_id
    except ZMQError:
        log_txt = 'Unsuccessful: %s' % job_id
    if log_path:
        try: # just in case someone deletes the file (you never know!)
            log_file = open(log_path, 'a')
            log_file.write(log_txt + '\n')
            log_file.close()
        except IOError:
            pass

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
def worker_loop(context, queue, addresses, callback, vent_port, sink_port, log_path=None):
    vent_sender = context.socket(zmq.PUSH)
    vent_sender.bind('tcp://*:%s' % vent_port)
    time.sleep(0.1)
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
        while True:
            try:
                s = sink_receiver.recv(zmq.NOBLOCK)
                result, job_id = pickle.loads(s)
                if job_id in sent_jobs:
                    callback(result, job_id)
                    sent_jobs.remove(job_id)
            except ZMQError:
                break
        send_pending_jobs(queue, vent_sender, sent_jobs, approx_workers)
        socks = dict(poller.poll(500))
        for receiver, sender in connections:
            if socks.get(receiver):
                process_job(receiver, sender, log_path)

# construct basic functions for handling parallel processing
def construct_worker(addresses, config={}):
    vent_port = config.get('vent_port', VENT_PORT_DEFAULT)
    sink_port = config.get('sink_port', SINK_PORT_DEFAULT)
    worker_id = config.get('worker_id', None)

    context = zmq.Context()
    queue = Queue.Queue()

    def worker(callback):
        if worker_id != None:
            if not os.path.exists('worker_logs/'):
                os.mkdir('worker_logs')
            log_path = 'worker_logs/%s.txt' % worker_id
            log_file = open(log_path, 'w')
            log_file.close()
        else:
            log_path = None
        worker_loop(context, queue, addresses, callback, vent_port, sink_port, log_path)

    def close():
        context.destroy()

    def run_job(job, args=()):
        job_id = str(uuid.uuid4())
        queue.put((job, args, job_id))
        return job_id

    return worker, close, run_job
