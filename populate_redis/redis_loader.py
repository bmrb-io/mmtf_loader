#!/usr/bin/env python

import os
import sys
import zlib
import redis

sys.path.append("Hadoop/python-hadoop")
from hadoop.io import SequenceFile

file_list = ["full/" + x for x in filter(lambda x:"part-" in x, os.listdir("full"))]
redis_conn = redis.Redis()

for ef in file_list:
    print(ef)
    reader = SequenceFile.Reader(ef)
    kc = reader.getKeyClass()
    vc = reader.getValueClass()

    k,v = kc(), vc()

    while reader.next(k,v):
        ks = k.toString()
        redis_conn.set(ks, v.toString())
        print("%s" % ks)
