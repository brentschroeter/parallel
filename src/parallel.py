#!/usr/bin/env python

# Created by Brent Schroeter with Luke Carmichael.

import zmq
import uuid
import pickle
import time
import Queue
import os
from collections import namedtuple
from zmq.core.error import ZMQError

VENT_PORT_DEFAULT = '5000'
SINK_PORT_DEFAULT = '5001'

JobInfo = namedtuple('JobInfo', 'job_id worker_id')

def send_pending_jobs(queue, sender, sent_jobs, max_sent_jobs):
    '''Sends all jobs in the queue.'''
    for i in range(max_sent_jobs - len(sent_jobs)):
        try:
            job_tuple = queue.get(False)
            sender.send(pickle.dumps(job_tuple))
            sent_jobs.append(job_tuple[2])
        except Queue.Empty:
            break

def process_job(receiver, sender, worker_id):
    '''Loads and runs the first job in receiver's queue and sends back the result.'''
    try:
        s = receiver.recv(zmq.NOBLOCK)
        job, args, job_id = pickle.loads(s)
        result = job(args)
        job_info = JobInfo(job_id, worker_id)
        reply = (result, job_info)
        sender.send(pickle.dumps(reply))
    except ZMQError:
        pass

def add_connection(context, vent_addr, sink_addr):
    '''Adds a ventilator and sink to both the poller and the list of active connections.'''
    vent_addr = 'tcp://%s' % vent_addr
    sink_addr = 'tcp://%s' % sink_addr
    receiver = context.socket(zmq.PULL)
    receiver.connect(vent_addr)
    sender = context.socket(zmq.PUSH)
    sender.connect(sink_addr)
    return receiver, sender

def worker_loop(context, queue, addresses, callback, callback_args, vent_port, sink_port, worker_id=None):
    '''Sends jobs, handles replies, and processes incoming jobs from other servers.'''
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
                result, job_info = pickle.loads(s)
                if job_info.job_id in sent_jobs:
                    if callback != None:
                        callback(result, job_info, callback_args)
                    sent_jobs.remove(job_info.job_id)
            except ZMQError:
                break
        send_pending_jobs(queue, vent_sender, sent_jobs, approx_workers)
        socks = dict(poller.poll(500))
        for receiver, sender in connections:
            if socks.get(receiver):
                process_job(receiver, sender, worker_id)

def construct_worker(addresses, config={}):
    '''Constructs basic functions to run and stop the worker and run jobs.'''
    vent_port = config.get('vent_port', VENT_PORT_DEFAULT)
    sink_port = config.get('sink_port', SINK_PORT_DEFAULT)
    worker_id = uuid.uuid4()

    context = zmq.Context()
    queue = Queue.Queue()

    def worker(callback, callback_args):
        worker_loop(context, queue, addresses, callback, callback_args, vent_port, sink_port, worker_id)

    def close():
        context.destroy()

    def run_job(job, args=()):
        job_id = str(uuid.uuid4())
        queue.put((job, args, job_id))
        return job_id

    return worker, close, run_job
