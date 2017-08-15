#!/usr/bin/env python

import os
import sys
import redis

sys.path.append("Hadoop/python-hadoop")
from hadoop.io import SequenceFile

file_list = ["full/part-%05d" % x for x in range(1,5)]
redis_conn = redis.Redis()

for ef in file_list:
    reader = SequenceFile.Reader(ef)
    kc = reader.getKeyClass()
    vc = reader.getValueClass()

    k,v = kc(), vc()

    while reader.next(k,v):
        ks = k.toString()
        if not redis_conn.get(ks):
            redis_conn.set(ks, v.toString())
            print("%s" % ks)
        else:
            print("Skipping %s." % ks)
