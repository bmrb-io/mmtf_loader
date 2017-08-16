#!/usr/bin/env python

import re
import sys
import msgpack
import redis

r = redis.Redis()
sequences = {}

with open("ss.txt", "r") as ss_data, open("ss.msg","w") as output:

    mode = True
    for line in ss_data:
        if "sequence" in line:
            pdb = line[1:5]
            seq = ""
            mode = True
        elif "secstr" in line:
            mode = False
            if r.get(pdb):
                sequences.setdefault(seq,[]).append(pdb)
            else:
                print("Skipping %s not in redis." % pdb)
        else:
            if mode:
                seq += line.strip()

    msgpack.dump(sequences, msg)
