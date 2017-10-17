#!/usr/bin/env python

import re
import os
import sys
import tarfile

import msgpack
import redis

r = redis.Redis()
sequences = {}

ids = {line.rstrip().upper():True for line in open('selected_ids_20_2')}

res_mapper = {'PRO':'P', 'GLY':'G', 'ALA':'A', 'ARG':'R', 'ASN':'N',
              'ASP':'D', 'CYS':'C', 'GLN':'Q', 'GLU':'E', 'HIS':'H',
              'ILE':'I', 'LEU':'L', 'LYS':'K', 'MET':'M', 'PHE':'F',
              'SER':'S', 'THR':'T', 'TRP':'W', 'TYR':'Y', 'VAL':'V'}

def get_seq(pdb):
    coords = msgpack.loads(r.get(pdb))
    seq = "".join([res_mapper.get(x[0], "X") for x in coords])
    return seq

count = 0
for pdb in r.keys():

    count += 1
    if pdb not in ids:
        raise ValueError("Invalid PDB in DB: %s" % pdb)
    else:
        sequences.setdefault(get_seq(pdb),[]).append(pdb)

    if count > 100:
        break

os.chdir("..")
with open("ss.msg","w") as output:
    msgpack.dump(sequences, output)

# Write out the tarfile for searching
tar = tarfile.open("search.tgz", "w:gz")
tar.add("search.py")
tar.add("ss.msg")
tar.close()
