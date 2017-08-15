#!/usr/bin/env python

import re
import sys
import zlib
import json
import redis
import msgpack
from StringIO import StringIO
from mmtf import parse_gzip, MMTFDecoder

sequences = json.load(open("data/ss.json",'r'))

def contains(str1, distance, str2):
    """ Check if search strings exist in DB separated by distance. """

    matches = []

    str1 = str1.upper()
    str2 = str2.upper()

    for seq in sequences:
        if str1 in seq and str2 in seq:
            s1idx = [m.start() for m in re.finditer('(?=%s)' % str1, seq)]
            for idx in s1idx:
                start = idx+int(distance)+len(str1)
                end = start + len(str2)
                if seq[start:end] == str2:
                    matches.extend(sequences[seq])

    return sorted(list(set(matches)))

def get_mmtfs(str1, distance, str2, parsed=True):
    """ Returns a list of mmtf objects for PDB IDs that have str1 separated
    from str2 by distance."""

    pdbs = contains(str1, distance, str2)

    red = redis.Redis()
    for pdb in pdbs:
        print("Fetching %s from red." % pdb)
        mmtf = red.get(pdb)

        if not mmtf:
            print("Missing %s" % pdb)
            #raise ValueError("Could not find PDB %s in Redis!" % pdb)
            continue

    if parsed:
        yield parse_mmtf(mmtf)
    else:
        yield mmtf

def parse_mmtf(mmtf):
    """ Loads the MMTF objects. """

    mmtf_decoder = MMTFDecoder()
    mmtf = zlib.decompress(mmtf, 16+zlib.MAX_WBITS)
    mmtf = msgpack.unpackb(mmtf)
    mmtf_decoder.decode_data(mmtf)
    yield mmtf_decoder
