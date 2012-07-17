#!/usr/bin/env python

# Created by Brent Schroeter with Luke Carmichael.

import zmq
from zmq.core.error import ZMQError
import pickle
import tasks
import time
import sys
import thread
import socket
import struct
try:
	from multiprocessing import cpu_count
	PROCESSING_MOD_PRESENT = True
except:
	PROCESSING_MOD_PRESENT = False
import random
try:
	import resource
	R_MOD_PRESENT = True
except:
	R_MOD_PRESENT = False

RETRIES = 3

class ParallelWorker(object):
	def __init__(self, addresses, control_port=5000):
		''' Parameter 'addresses' is a dictionary of sinks referenced to ventilators.
			Parameter 'control_port' represents a port that can command the worker to stop accepting work or add a new set of addresses to its list. '''

		if R_MOD_PRESENT:
			resource.setrlimit(resource.RLIMIT_NOFILE, (500, -1))

		self.control_port = control_port
		self.addresses = addresses

	def start_worker(self):
		thread.start_new_thread(self.work, ())

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
		controller.connect('tcp://localhost:%s' % self.control_port)
		controller.setsockopt(zmq.SUBSCRIBE, '')

		poller.register(controller, zmq.POLLIN)

		for i in self.addresses:
			connections, poller = self.add_receiver(ctx, connections, poller, i)

		while True:
			socks = dict(poller.poll())

			for i in connections:
				if socks.get(i[0]) == zmq.POLLIN:
					s = i[0].recv()
					task = pickle.loads(s)

					result = task.run()
					request = (task.id, result)

					if i[1] == None:
						sender = ctx.socket(zmq.PUSH)
						sender.connect(self.addresses[i[2]])
						i[1] = sender

					try:
						i[1].send(pickle.dumps(request), zmq.NOBLOCK)
					except ZMQError:
						pass

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

class ParallelClient(object):
	def __init__(self, vent_port=5001, sink_port=5002, control_port=5000):
		''' Parameter 'addresses' is a dictionary of sinks referenced to ventilators.
			Parameter 'control_port' represents a port that can command the worker to stop accepting work or add a new set of addresses to its list. '''

		self.addresses = {'tcp://localhost:%s' % vent_port: 'tcp://localhost:%s' % sink_port}
		self.vent_port = vent_port
		self.sink_port = sink_port
		self.control_port = control_port

		if PROCESSING_MOD_PRESENT:
			num_cpus = cpu_count()
		else:
			num_cpus = 1

		self.workers = []
		for i in range(num_cpus):
			new_worker = ParallelWorker(self.addresses, control_port)
			new_worker.start_worker()
			self.workers.append(new_worker)

		thread.start_new_thread(self.broadcast_address, ())
		thread.start_new_thread(self.listen_for_clients, ())

	def generate_tasks(self):
		task_list = []
		for i in range(1000):
			new_task = tasks.WaitTask(random.randint(399, 499))
			task_list.append(new_task)
		return task_list

	def start_distribution(self):
		thread.start_new_thread(self.distribute, ())

	def retry(self, task_list, checklist, tasks_completed, sender):
		for i in task_list:
			if not checklist[i.id][0]:
				if checklist[i.id][1] > 0:
					checklist[i.id][1] -= 1
					try:
						sender.send(pickle.dumps(i), zmq.NOBLOCK)
					except ZMQError:
						pass
				else:
					tasks_completed += 1
		return checklist, tasks_completed

	def distribute(self):
		ctx = zmq.Context()

		sender = ctx.socket(zmq.PUSH)
		sender.bind('tcp://*:%s' % self.vent_port)

		receiver = ctx.socket(zmq.PULL)
		receiver.bind('tcp://*:%s' % self.sink_port)

		time.sleep(0.5)

		print 'Pushing tasks.'

		max_timeout = 0
		task_list = self.generate_tasks()
		# every value in checklist takes the form [bool completed, int retries_left]
		checklist = {}
		for i in task_list:
			max_timeout = max(max_timeout, i.req_timeout)
			checklist[i.id] = [False, RETRIES]
			sender.send(pickle.dumps(i))

		poller = zmq.Poller()
		poller.register(receiver, zmq.POLLIN)
		tasks_completed = 0

		# WARNING: potential for infinite loop
		while tasks_completed < len(task_list):
			socks = dict(poller.poll(max_timeout))
			if socks.get(receiver) == zmq.POLLIN:
				s = receiver.recv()
				try:
					task_id, result = pickle.loads(s)
					if checklist.get(task_id) != None:
						if not checklist[task_id][0]:
							checklist[task_id][0] = True
							print 'task completed: %s' % task_id
							tasks_completed += 1
					else:
						print 'Error: foreign task received.'
				except IndexError:
					print 'Error: Response not pickled.'
			else:
				print 'Error: Response timed out. Retrying.'
				checklist, tasks_completed = self.retry(task_list, checklist, tasks_completed, sender)

		sender.close()
		receiver.close()

	def kill_workers(self):
		''' Gracefully kill workers. '''
		ctx = zmq.Context()

		try:
			controller = ctx.socket(zmq.PUB)
			controller.bind('tcp://*:%s' % self.control_port)
			time.sleep(0.5)
			request = ['0']
			controller.send_multipart(request)
			time.sleep(0.5)
			controller.close()
		except ZMQError:
			pass

	def subscribe(self, vent_addr, sink_addr):
		ctx = zmq.Context()

		try:
			controller = ctx.socket(zmq.PUB)
			controller.bind('tcp://*:%s' % self.control_port)
			time.sleep(0.5)
			self.addresses[vent_addr] = sink_addr
			request = ['1', vent_addr, sink_addr]
			controller.send_multipart(request)
			time.sleep(0.5)
			controller.close()
		except ZMQError:
			pass

	def listen_for_clients(self, mcast_grp='228.3.9.7', mcast_port=5003):
		sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
		sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		sock.bind(('', mcast_port))
		mreq = struct.pack('4sl', socket.inet_aton(mcast_grp), socket.INADDR_ANY)

		sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

		while True:
			try:
				my_addr = socket.gethostbyname(socket.getfqdn())
			except:
				my_addr = socket.gethostbyname(socket.gethostname())
			s = sock.recv(10240)
			msg = s.split()
			if len(msg) != 2 or msg[0] != 'pl_ping' or msg[1] == my_addr:
				continue
			new_addr = msg[1]
			new_vent_addr = 'tcp://%s:%s' % (new_addr, self.vent_port)
			if new_vent_addr in self.addresses:
				continue
			new_sink_addr = 'tcp://%s:%s' % (new_addr, self.sink_port)
			self.subscribe(new_vent_addr, new_sink_addr)
			print 'New client: %s' % new_addr

	def broadcast_address(self, mcast_grp='228.3.9.7', mcast_port=5003):
		sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
		sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)

		while True:
			try:
				my_addr = socket.gethostbyname(socket.getfqdn())
			except:
				my_addr = socket.gethostbyname(socket.gethostname())
			msg = 'pl_ping %s' % my_addr
			sock.sendto(msg, (mcast_grp, mcast_port))
			time.sleep(2)

	def restart_workers(self):
		self.kill_workers()
		for i in self.workers:
			i.start_worker()