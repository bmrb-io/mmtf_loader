#!/usr/bin/env python

import re
import sys
import json
import redis
from mmtf import parse_gzip

sequences = json.load(open("data/ss.json",'r'))

def contains(str1, distance, str2):
    """ Check if search strings exist in DB separated by distance. """

    matches = []

    for seq in sequences:
        if str1 in seq and str2 in seq:
            s1idx = [m.start() for m in re.finditer('(?=%s)' % str1, seq)]
            for idx in s1idx:
                start = idx+int(distance)+len(str1)
                end = start + len(str2)
                if seq[start:end] == str2:
                    matches.extend(sequences[seq])

    return sorted(list(set(matches)))

def get_mmtfs(str1, distance, str2):
    """ Returns a list of mmtf objects for PDB IDs that have str1 separated
    from str2 by distance."""

    pdbs = contains(str1, distance, str2)
    results = []

    red = redis.Redis()
    for pdb in pdbs:
        print("Fetching %s from red." % pdb)
        gzipped_mmtf = red.get(pdb)
        if not gzipped_mmtf:
            print("Missing %s" % pdb)
            #raise ValueError("Could not find PDB %s in Redis!" % pdb)
            continue
        results.append(parse_gzip(gzipped_mmtf))

    return results
