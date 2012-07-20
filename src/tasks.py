#!/usr/bin/env python

import time
import uuid
import os
import random

class Task(object):
	def __init__(self, req_timeout=5000):
		self.req_timeout = req_timeout

	def run(self):
		raise NotImplementedError('Method not implemented.')

class WaitTask(Task):
	def __init__(self, time=200):
		Task.__init__(self, time + 2000)
		self.wait_time = time

	def run(self):
		time.sleep(float(self.wait_time) * 0.001)
		print random.randint(0, 100)
		return 'Slept for: %d ms' % self.wait_time