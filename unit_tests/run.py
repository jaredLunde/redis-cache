#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import os
import sys


cd = os.path.dirname(os.path.abspath(__file__))
path = cd.split('redis-cache')[0] + 'redis-cache'
sys.path.insert(0, path)


if __name__ == '__main__':
    # Unit test
    from unit_tests import configure
    configure.run_discovered(cd)
