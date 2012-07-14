#!/usr/bin/env python

import zmq
import pickle
import tasks
import time
import sys

def test(string):
	print string

def main(port):
	context = zmq.Context()

	receiver = context.socket(zmq.PULL)
	receiver.bind('tcp://*:%s' % port)

	s = receiver.recv()

	for i in range(500):
		s = receiver.recv()
		try:
			result = pickle.loads(s)
			print 'Completed job: ', result
		except:
			pass

	time.sleep(0.5)

main(sys.argv[1])