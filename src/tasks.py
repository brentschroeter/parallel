#!/usr/bin/env python

import time
import uuid

class Task(object):
	def __init__(self, req_timeout=5000):
		self.id = uuid.uuid4()
		self.req_timeout = req_timeout

	def run(self):
		raise NotImplementedError('Method not implemented.')

class WaitTask(Task):
	def __init__(self, time=200):
		super(WaitTask, self).__init__(time + 2000)
		self.wait_time = time
		self.id = uuid.uuid4()

	def run(self):
		time.sleep(float(self.wait_time) * 0.001)
		return 1