#!/usr/bin/env python

import time
import sys
import parallel

CONTROL_PORT = 5000

def main():
	client = parallel.ParallelClient()

	print 'Enter the command "push" (without quotes) to begin pushing tasks. Enter "kill" to kill the worker and client. Enter "subscribe [IP address]" to begin pulling from a client at the given IP address.'

	while True:
		usr_in = raw_input('> ')
		if usr_in == 'kill':
			client.kill_workers()
			break
		elif usr_in == 'push':
			client.distribute()
		elif usr_in[:9] == 'subscribe':
			new_ip = usr_in[10:]
			vent_addr = 'tcp://%s:%s' % (new_ip, vent_port)
			sink_addr = 'tcp://%s:%s' % (new_ip, sink_port)
			client.subscribe(vent_addr, sink_addr)
		else:
			print 'Command not recognized.'

main()