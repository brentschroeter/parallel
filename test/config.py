#!/usr/bin/env python

from collections import namedtuple

UsagePattern = namedtuple('UsagePattern', 'sets set_sleep set_reps job_sleep')
WORKER_ADDRESSES = [('localhost:5000', 'localhost:5001'), ('localhost:5002', 'localhost:5003'), ('localhost:5004', 'localhost:5005')]

def num_local_workers():
    counter = 0
    for vent_addr, sink_addr in WORKER_ADDRESSES:
        if len(vent_addr) > 9 and vent_addr[:10] == 'localhost:':
            counter += 1
    return counter

# for test_files.py
NUM_FILES = 5
TESTING_STR = 'Testing testing testing.'

# for test_mem.py
NUM_STRINGS = 200
STR_LEN = 100000000
MEM_LIMIT = 206000000

# for test_multipush.py
NUM_PUSHERS = 3

# for test_volume.py
USAGE_PATTERNS = (UsagePattern(5, 5000, 1, 1000), UsagePattern(500, 0, 1, 10), UsagePattern(5, 3000, 100, 100))

# for timeout tests
NUM_JOBS = 20
WAIT_TIME = 100
