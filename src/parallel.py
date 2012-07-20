#!/usr/bin/env python

# Created by Brent Schroeter with Luke Carmichael.

import zmq
import uuid
import pickle
import tasks
import time
from zmq.core.error import ZMQError

VENT_PORT = '5000'
SINK_PORT = '5001'
CONNECTION_RETRIES = 5

def get_vent():
    context = zmq.Context()
    sender = context.socket(zmq.PUSH)
    for i in range(CONNECTION_RETRIES):
        try:
            sender.bind('tcp://*:%s' % VENT_PORT)
            break
        except ZMQError:
            time.sleep(0.5)
    else:
        sender.close()
        raise ZMQError('Could not bind socket.')
    time.sleep(0.5)
    return sender

def get_sink():
    context = zmq.Context()
    receiver = context.socket(zmq.PULL)
    for i in range(CONNECTION_RETRIES):
        try:
            receiver.bind('tcp://*:%s' % SINK_PORT)
            break
        except ZMQError:
            time.sleep(0.5)
    else:
        sender.close()
        raise ZMQError('Could not bind socket.')
    time.sleep(0.5)
    return receiver

def close_vent(sender):
    time.sleep(0.5)
    sender.close()

def close_sink(receiver):
    receiver.close()

def run_job(job, sender=None):
    job_id = str(uuid.uuid4())
    msg = [job_id, pickle.dumps(job)]
    manual_close = False
    if not sender:
        manual_close = True
        sender = get_vent()
    sender.send_multipart(msg)
    if manual_close:
        close_vent(sender)
    return job_id

# for much faster pushing of multiple jobs
def run_jobs(jobs):
    job_ids = []
    sender = get_vent()
    for i in jobs:
        job_ids.append(run_job(i, sender))
    close_vent(sender)
    return job_ids

def get_job(job_id, callback=None, finished_jobs={}):
    if not job_id in finished_jobs:
        # open the receiver
        receiver = get_sink()
        # gather all completed jobs
        while True:
            try:
                msg = receiver.recv_multipart(zmq.NOBLOCK)
            except ZMQError:
                break
            tmp_id = msg[0]
            tmp_result = pickle.loads(msg[1])
            finished_jobs[tmp_id] = tmp_result
            if callback:
                callback(tmp_id, tmp_result)
        # close the receiver
        receiver.close()
    # if job with job_id is finished, return the job result
    if job_id in finished_jobs:
        result = finished_jobs[job_id]
        return result
    # otherwise, return None
    return None

def accept_work(worker_pool, timeout=None):
    context = zmq.Context()
    poller = zmq.Poller()
    connections = []
    for i in worker_pool:
        receiver = context.socket(zmq.PULL)
        receiver.connect('tcp://%s:%s' % (i, VENT_PORT))
        poller.register(receiver, zmq.POLLIN)
        connections.append([i, receiver, None])
    while True:
        socks = dict(poller.poll(timeout))
        if len(socks) == 0:
            break
        for i in connections:
            if socks.get(i[1]) == zmq.POLLIN:
                msg = i[1].recv_multipart()
                job_id = msg[0]
                job = pickle.loads(msg[1])
                result = job.run()
                result = pickle.dumps(result)
                request = [job_id, result]
                if i[2] == None:
                    sender = context.socket(zmq.PUSH)
                    sender.connect('tcp://%s:%s' % (i[0], SINK_PORT))
                    i[2] = sender
                i[2].send_multipart(request)
    # Is this right?
    context.destroy()