#!/usr/bin/env python

import msgpack
import redis

r = redis.Redis()
sequences = {}

ids = {line.rstrip().upper():True for line in open('selected_ids_20_2')}


def get_seq(pdb_obj):
    return "".join([x[0] for x in pdb_obj])

everything = {}
keys = r.keys()
counter = 0

for x,pdb in enumerate(keys):

    counter += 1
    if pdb == "seq_dict" or pdb == "all_pdbs":
        continue
    elif pdb not in ids:
        raise ValueError("Invalid PDB in DB: %s" % pdb)
    else:
        pdb_obj = msgpack.loads(r.get(pdb))
        sequences.setdefault(get_seq(pdb_obj),[]).append(pdb)
        everything[pdb] = pdb_obj

    if counter % 100 == 0:
        print "%2d percent" % (float(x)/len(keys)*100)

# Dump the everything object to disk
with open("/raid/trdistance/archive.msg", "wb") as ac:
    ac.write(msgpack.dumps([sequences, everything]))
