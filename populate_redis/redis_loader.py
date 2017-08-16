#!/usr/bin/env python

import os
import sys
import zlib
import redis
import msgpack
from collections import deque

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
        redis_conn.set(ks, extract_important(v.toString()))
        print("%s" % ks)
