#!/usr/bin/env python

import time
import sys
import parallel

CONTROL_PORT = 5000

def main():
	client = parallel.ParallelClient()

	print 'Enter the command "push" (without quotes) to begin pushing tasks. Enter "kill" to kill the worker and client.'

	while True:
		usr_in = raw_input('> ')
		if usr_in == 'kill':
			client.kill_workers()
			break
		elif usr_in == 'push':
			client.distribute()
		else:
			print 'Command not recognized.'

main()