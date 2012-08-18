#!/usr/bin/env python

import boto
import time

AMI_ID = 'ami-82fa58eb'
INSTANCE_TYPE = 't1.micro'
SEC_GROUP = None

def main():
    access_key_id = raw_input('Access key ID: ')
    secret_access_key = raw_input('Secret access key: ')
    key_name = raw_input('Key name: ')
    num_instances = int(raw_input('Number of instances: '))
    SEC_GROUP = raw_input('Security group: ')

    conn = boto.connect_ec2(access_key_id, secret_access_key)
    conn.run_instances(AMI_ID, min_count=num_instances, max_count=num_instances, key_name=key_name, instance_type=INSTANCE_TYPE, security_groups=[SEC_GROUP])

    for instance in conn.get_all_instances()[0].instances:
        while not instance.ip_address:
            time.sleep(1)
        print instance.ip_address

if __name__ == '__main__':
    main()
