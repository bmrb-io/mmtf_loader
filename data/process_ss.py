#!/usr/bin/env python

import re
import sys
import msgpack
import redis

r = redis.Redis()
sequences = {}

ids = {line.rstrip().upper():True for line in open('selected_ids_20_2')}

with open("ss.txt", "r") as ss_data, open("../ss.msg","w") as output:

    mode = True
    for line in ss_data:
        if "A:sequence" in line:
            pdb = line[1:5]
            seq = ""
            mode = True
        elif "A:secstr" in line:
            mode = False
            if r.get(pdb):
                if pdb not in ids:
                    #print("Skipping by virtue of MolProbity: %s" % pdb)
                    pass
                else:
                    sequences.setdefault(seq,[]).append(pdb)
            else:
                print("Skipping %s not in redis." % pdb)
        else:
            if mode:
                seq += line.strip()

    msgpack.dump(sequences, output)
