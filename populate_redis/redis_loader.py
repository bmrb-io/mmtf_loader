#!/usr/bin/env python

import os
import sys
import zlib
import redis
import msgpack
from collections import deque
import multiprocessing


from mmtf import parse_gzip, MMTFDecoder

sys.path.append("Hadoop/python-hadoop")
from hadoop.io import SequenceFile


def extract_important(mmtf):
    """ Returns the data we want from the MMTF."""

    m = MMTFDecoder()
    mmtf = zlib.decompress(mmtf, 16+zlib.MAX_WBITS)
    mmtf = msgpack.unpackb(mmtf)
    m.decode_data(mmtf)

    coords = deque(m.get_coords())

    # Only go up to the atoms in the first model
    max_res = m.groups_per_chain[0]

    residue_list = []
    for res in m.group_type_list:
        atom_list = []
        for atom in m.group_list[res]['atomNameList']:
            atom_list.append([atom, coords.popleft()])
        residue_list.append([m.group_list[res]['groupName'], atom_list])
        max_res = max_res - 1
        if max_res == 0:
            return zlib.compress(msgpack.dumps(residue_list))

def chunker(l, desired_sublists):
    """ Breaks a list in x sublists of roughly equal size. """

    res = [[] for x in range(0,desired_sublists)]
    c = desired_sublists

    while len(l) > 0:
        res[c-1].append(l.pop())
        c = (c - 1) % desired_sublists
    return res

redis_conn = redis.Redis()
file_list = ["full/" + x for x in filter(lambda x:"part-" in x, os.listdir("full"))]
cores = multiprocessing.cpu_count()
chunked_list = chunker(file_list, cores)

for x in range(0, cores):
    pid = os.fork()
    if pid == 0:

        for ef in chunked_list[x]:
            print("Proc %s doing %s" % (x, ef))
            reader = SequenceFile.Reader(ef)
            kc = reader.getKeyClass()
            vc = reader.getValueClass()

            k,v = kc(), vc()

            while reader.next(k,v):
                ks = k.toString()
                redis_conn.set(ks, extract_important(v.toString()))
                print("%s" % ks)
        sys.exit(0)
