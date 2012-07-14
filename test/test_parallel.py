#!/usr/bin/env python

import time
import sys
from client import ImgPlusClient

def main(starting_port, num_clients):
	client_info = []

	port = starting_port
	addresses = {}
	for i in range(num_clients):
		tmp = (port, port + 1, port + 2)
		port += 3
		client_info.append(tmp)
		addresses['tcp://localhost:' + str(tmp[0])] = 'tcp://localhost:' + str(tmp[1])

	clients = []
	for i in client_info:
		clients.append(ImgPlusClient(i[0], i[1], i[2], addresses))

	time.sleep(5)

	clients[0].distribute()

main(int(sys.argv[1]), int(sys.argv[2]))