#!/usr/bin/env python

from collections import namedtuple

UsagePattern = namedtuple('UsagePattern', 'sets set_sleep set_reps job_sleep')
WORKER_ADDRESSES = [('localhost:5000', 'localhost:5001'), ('localhost:5002', 'localhost:5003'), ('localhost:5004', 'localhost:5005')]

# for test_mem.py
NUM_STRINGS = 200
STR_LEN = 100000000
MEM_LIMIT = 206000000

# for test_volume.py
USAGE_PATTERNS = (UsagePattern(5, 5000, 1, 1000), UsagePattern(500, 0, 1, 10), UsagePattern(5, 3000, 100, 100))

# for timeout tests
NUM_JOBS = 20
WAIT_TIME = 100
