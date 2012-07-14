#!/usr/bin/env python

import zmq
import pickle
import tasks
import time
import sys
import thread
import random
try:
	import resource
	R_MOD_PRESENT = True
except:
	R_MOD_PRESENT = False

RETRIES = 3

class ImgPlusClient(object):
	def __init__(self, vent_port, sink_port, control_port, addresses):
		''' Parameter 'addresses' is a dictionary of sinks referenced to ventilators.
			Parameter 'control_port' represents a port that can command the worker to stop accepting work or add a new set of addresses to its list. '''

		if R_MOD_PRESENT:
			resource.setrlimit(resource.RLIMIT_NOFILE, (500, -1))

		self.vent_port = vent_port
		self.sink_port = sink_port
		self.control_port = control_port
		self.addresses = addresses

		self.start_worker()

	def generate_jobs(self):
		jobs = []
		for i in range(1000):
			new_job = tasks.WaitTask(random.randint(399, 499))
			jobs.append(new_job)
		return jobs

	def start_distribution(self):
		thread.start_new_thread(self.distribute, ())

	def distribute(self):
		ctx = zmq.Context()

		#self.kill_worker(ctx)

		sender = ctx.socket(zmq.PUSH)
		sender.bind('tcp://*:%s' % self.vent_port)

		#import ipdb; ipdb.set_trace()

		receiver = ctx.socket(zmq.PULL)
		receiver.bind('tcp://*:%s' % self.sink_port)

		time.sleep(0.5)

		print 'Pushing jobs.'

		max_timeout = 0
		jobs = self.generate_jobs()
		# every value in checklist takes the form (bool completed, int retries_left)
		checklist = {}
		for i in jobs:
			max_timeout = max(max_timeout, i.req_timeout)
			checklist[i.id] = [False, RETRIES]
			sender.send(pickle.dumps(i))

		poller = zmq.Poller()
		poller.register(receiver, zmq.POLLIN)
		jobs_completed = 0
		# WARNING: potential for infinite loop
		while jobs_completed < len(jobs):
			socks = dict(poller.poll(max_timeout))
			if socks.get(receiver) == zmq.POLLIN:
				s = receiver.recv()
				try:
					job_id, result = pickle.loads(s)
					if checklist.get(job_id) != None:
						if not checklist[job_id][0]:
							checklist[job_id][0] = True
							print 'Job completed: %s' % job_id
							jobs_completed += 1
					else:
						print 'Error: foreign job received.'
				except:
					print 'Error: response not pickled.'
					jobs_completed += 1
			else:
				print 'Error: timed out, retrying.'
				for i in jobs:
					if not checklist[i.id][0]:
						if checklist[i.id][1] > 0:
							checklist[i.id][1] -= 1
							try:
								sender.send(pickle.dumps(i), zmq.NOBLOCK)
							except zmq.core.error.ZMQError:
								pass
						else:
							jobs_completed += 1

		sender.close()
		receiver.close()

		#self.start_worker()

	def start_worker(self):
		thread.start_new_thread(self.work, ())

	def kill_worker(self, context=zmq.Context()):
		controller = context.socket(zmq.PUB)
		controller.connect('tcp://localhost:%s' % self.control_port)
		request = ['0']
		controller.send_multipart(request)
		time.sleep(0.5)
		controller.close()

	def subscribe(self, vent_addr, sink_addr):
		ctx = zmq.Context()

		controller = ctx.socket(zmq.PUB)
		controller.connect('tcp://localhost:%s')
		request = ['1', vent_addr, sink_addr]
		controller.send_multipart(request)
		time.sleep(0.5)
		controller.close()

	def add_receiver(self, context, connections, poller, addr):
		receiver = context.socket(zmq.PULL)
		receiver.connect(addr)

		poller.register(receiver, zmq.POLLIN)
		connections.append([receiver, None, addr])

		return connections, poller

	def work(self):
		ctx = zmq.Context()

		poller = zmq.Poller()
		connections = []

		controller = ctx.socket(zmq.SUB)
		controller.bind('tcp://*:%s' % self.control_port)

		poller.register(controller, zmq.POLLIN)

		for i in self.addresses:
			connections, poller = self.add_receiver(ctx, connections, poller, i)

		while True:
			socks = dict(poller.poll())

			for i in connections:
				if socks.get(i[0]) == zmq.POLLIN:
					s = i[0].recv()
					job = pickle.loads(s)

					result = job.run()
					request = (job.id, result)

					if i[1] == None:
						sender = ctx.socket(zmq.PUSH)
						sender.connect(self.addresses[i[2]])
						i[1] = sender

					i[1].send(pickle.dumps(request))

			if socks.get(controller) == zmq.POLLIN:
				msg = controller.recv_multipart()
				cmd = msg[0]

				if cmd == '0':
					# if the command equals '0' then stop accepting work
					break
				elif cmd == '1':
					# otherwise, add a new set of addresses to the list
					vent_addr = msg[1]
					sink_addr = msg[2]

					self.addresses[vent_addr] = sink_addr
					connections, poller = self.add_receiver(ctx, connections, poller, vent_addr)