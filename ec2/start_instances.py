#!/usr/bin/env python

import boto
import time
try:
    from config import KEY_ID, SECRET_KEY, KEYPAIR_NAME, AMI_ID, INSTANCE_TYPE, SEC_GROUPS, NUM_INSTANCES
except:
    print 'Failed to import config.py.'

def main():
    conn = boto.connect_ec2(KEY_ID, SECRET_KEY)
    conn.run_instances(AMI_ID, min_count=NUM_INSTANCES, max_count=NUM_INSTANCES, key_name=KEYPAIR_NAME, instance_type=INSTANCE_TYPE, security_groups=SEC_GROUPS)

    for i in range(len(conn.get_all_instances()[0].instances)):
        print 'Looking for instance...'
        while not conn.get_all_instances()[-1].instances[i].ip_address:
            time.sleep(1)
        print 'Instance found: ', conn.get_all_instances()[-1].instances[i].ip_address
        print 'Done.'

if __name__ == '__main__':
    main()
