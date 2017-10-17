#!/usr/bin/env python

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
    return "".join([res_mapper.get(x[0], "X") for x in msgpack.loads(r.get(pdb))])

for pdb in r.keys():

    if pdb not in ids and pdb != "seq_dict":
        raise ValueError("Invalid PDB in DB: %s" % pdb)
    else:
        sequences.setdefault(get_seq(pdb),[]).append(pdb)

# Dump the sequence table to redis
r.set("seq_dict", msgpack.dumps(sequences))
