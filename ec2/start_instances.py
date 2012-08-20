#!/usr/bin/env python

import boto
import time
import config

def main():
    conn = boto.connect_ec2(KEY_ID, SECRET_KEY)
    conn.run_instances(AMI_ID, min_count=NUM_INSTANCES, max_count=NUM_INSTANCES, key_name=KEYPAIR_NAME, instance_type=INSTANCE_TYPE, security_groups=SEC_GROUPS)

    for instance in conn.get_all_instances()[0].instances:
        while not instance.ip_address:
            time.sleep(1)
        print instance.ip_address

if __name__ == '__main__':
    main()
