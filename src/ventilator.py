#!/usr/bin/env python

import zmq
import pickle
import tasks
import time
import sys

def main(ventport, sinkport):
	context = zmq.Context()
	sender = context.socket(zmq.PUSH)
	sender.bind('tcp://*:%s' % ventport)

	receiver = context.socket(zmq.PULL)
	receiver.bind('tcp://*:%s' % sinkport)

	time.sleep(0.5)

	for i in range(500):
		task = tasks.WaitTask(100)
		assert task.id
		sender.send(pickle.dumps(task))

	for i in range(500):
		s = receiver.recv()
		try:
			result = pickle.loads(s)
			print 'Completed job: ', result
		except:
			pass

main(sys.argv[1], sys.argv[2])