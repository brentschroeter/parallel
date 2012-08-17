#!/usr/bin/env python

from subprocess import call

def sh(cmd):
    call(cmd, shell=True)

def main():
    sh('sudo apt-get -y install python-pip')
    sh('sudo apt-get update')
    sh('sudo apt-get -y install gcc g++')
    sh('sudo pip install pyzmq-static')

if __name__ == '__main__':
    main()
