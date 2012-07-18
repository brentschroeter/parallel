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
		Task.__init__(self, time + 2000)
		self.wait_time = time

	def run(self):
		time.sleep(float(self.wait_time) * 0.001)
		return 1

class ImgTask(Task):
	def __init__(self, img_path, req_timeout=5000):
		Task.__init__(self, req_timeout)
		self.img_path = img_path

	def eval_img(self, img_file):
		raise NotImplementedError

	def run(self):
		img_file = open(img_path)
		result_file_path = self.eval_img(img_file)
		img_file.close()
		return result_file_path