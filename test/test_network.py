#!/usr/bin/env python

import time
import sys
import parallel
import tasks
import random

def main():
	worker_pool = []
	print 'Enter the IP addresses of other clients. Enter "done" to finish.'
	address = 'localhost'
	while address != 'done':
		worker_pool.append(address)
		address = raw_input('Address: ')

	print ''
	print 'Commands:'
	print 'work: Accept work from other clients.'
	print 'push: Generate and push jobs.'
	print 'quit: Quit.'
	print ''

	while True:
		cmd = raw_input('Command: ')
		if cmd == 'work':
			parallel.accept_work(worker_pool)
		elif cmd == 'push':
			push_jobs()
		elif cmd == 'quit':
			break
		else:
			print 'Command not recognized.'

def push_jobs():
	jobs = []
	for i in range(1000):
		jobs.append(tasks.WaitTask(random.randint(300, 500)))
	job_ids = parallel.run_jobs(jobs)
	finished_jobs = {}
	for i in job_ids:
		result = None
		while result == None:
			result = parallel.get_job(finished_jobs=finished_jobs)
		print result

main()