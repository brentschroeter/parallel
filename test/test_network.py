#!/usr/bin/env python

import time
import sys
import resource
from client import ImgPlusClient

CONTROL_PORT = 5000

def main(vent_port, sink_port, ip_addrs):
	addresses = {('tcp://localhost:%s' % vent_port): ('tcp://localhost:%s' % sink_port)}
	for i in ip_addrs:
		addresses[('tcp://%s:%s' % (i, vent_port)): ('tcp://%s:%s' % (i, sink_port))]
	client = ImgPlusClient(vent_port, sink_port, CONTROL_PORT, addresses)

	print 'Enter the command "push" (without quotes) to begin pushing jobs. Enter "kill" to kill the worker and client. Enter "subscribe [IP address]" to begin pulling from a client at the given IP address.'

	while True:
		usr_in = raw_input('> ')
		if usr_in == 'kill':
			client.kill_worker()
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

main(sys.argv[1], sys.argv[2], sys.argv[3:])