#!/usr/bin/env python

import os
import sys
import zlib
import redis
import msgpack
import multiprocessing
from collections import deque

from mmtf import parse_gzip, MMTFDecoder

sys.path.append("Hadoop/python-hadoop")
from hadoop.io import SequenceFile

res_mapper = {'PRO':'P', 'GLY':'G', 'ALA':'A', 'ARG':'R', 'ASN':'N',
              'ASP':'D', 'CYS':'C', 'GLN':'Q', 'GLU':'E', 'HIS':'H',
              'ILE':'I', 'LEU':'L', 'LYS':'K', 'MET':'M', 'PHE':'F',
              'SER':'S', 'THR':'T', 'TRP':'W', 'TYR':'Y', 'VAL':'V'}

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
        atom_list = {}
        for atom in m.group_list[res]['atomNameList']:
            # Only add used atoms
            if "C" in atom or "N" in atom or "H" in atom:
                atom_list[atom] = coords.popleft()
            else:
                coords.popleft()
        residue_list.append([res_mapper.get(m.group_list[res]['groupName'], "X"), atom_list])
        max_res = max_res - 1
        if max_res == 0:
            return msgpack.dumps(residue_list)

def chunker(l, desired_sublists):
    """ Breaks a list in x sublists of roughly equal size. """

    res = [[] for x in range(0,desired_sublists)]
    c = desired_sublists

    while len(l) > 0:
        res[c-1].append(l.pop())
        c = (c - 1) % desired_sublists
    return res

redis_conn = redis.Redis()
ids = {line.rstrip().upper():True for line in open('selected_ids_20_2')}
file_list = ["full/" + x for x in filter(lambda x:"part-" in x, os.listdir("full"))]
cores = multiprocessing.cpu_count()
chunked_list = chunker(file_list[0:cores], cores)

pids = []

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
                if ks in ids:
                    print("    setting %s" % ks)
                    redis_conn.set(ks, extract_important(v.toString()))
                else:
                    print("Not setting %s" % ks)

        sys.exit(0)
    else:
        pids.append(pid)

print "Parent alive, starting to wait..."
for pid in pids:
    print "Waiting for PID %s to finish..." % pid
    os.waitpid(pid, 0)
print "Done!"
