#!/usr/bin/env python

import time
import uuid
import os

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

class ImgOp(object):
	def eval_img(self, img_data):
		raise NotImplementedError

class FullImgTask(Task):
	''' This class holds all of the image data so that the data and operation in the same message. '''
	def __init__(self, img_data, operation, req_timeout=5000):
		Task.__init__(self, req_timeout)
		self.img_data = img_data
		self.op = operation

	def run(self):
		result_data = self.op.eval_img(self.img_data)
		return result_data

class LiteImgTask(Task):
	''' Like FullImgTask, but instead of containing the actual image data it only has the path to a shared image in order to make networking more flexible. '''
	def __init__(self, img_path, operation, req_timeout=5000):
		Task.__init__(self, req_timeout)
		self.img_path = img_path
		self.op = operation
		self.op_args = op_args

	def run(self):
		f = open(self.img_path)
		img_data = f.read()
		f.close()
		result_data = self.op.eval_img(img_data)
		result_dir = os.path.join(os.path.dirname(self.img_path), 'results/')
		if not os.path.exists(result_dir):
			os.mkdir(result_dir)
		result_file_path = os.path.join(result_dir, os.path.basename(self.img_path))
		f = open(result_file_path, 'w')
		f.write(result_data)
		f.close()
		return result_file_path