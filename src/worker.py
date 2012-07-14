#!/usr/bin/env python

import zmq
import pickle
import sys
import tasks

def main(addresses):
	context = zmq.Context()

	poller = zmq.Poller()

	connections = []

	for i in addresses:
		receiver = context.socket(zmq.PULL)
		receiver.connect(i)

		sender = context.socket(zmq.PUSH)
		sender.connect(i)

		poller.register(receiver, zmq.POLLIN)

		connections.append((receiver, sender))

	while True:
		socks = dict(poller.poll())

		for i in connections:
			if socks.get(i[0]) == zmq.POLLIN:
				s = i[0].recv()

				job = pickle.loads(s)

				result = job.run()
				request = (job.id, result)

				i[1].send(pickle.dumps(result))

main(sys.argv[1:])