#!/usr/bin/env python

import re
import sys
import zlib
import msgpack
from StringIO import StringIO

import redis
from mmtf import parse_gzip, MMTFDecoder

sequences = msgpack.load(open("data/ss.msg",'r'))

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

def fake_redis(pdbs):
    """ Debug method to use FS rather than Redis."""

    for pdb in pdbs:
        try:
            yield open("/zfs/mmtfs/%s" % pdb,"r").read()
        except IOError:
            yield None

def get_mmtfs(str1, distance, str2, parsed=True):
    """ Returns a list of mmtf objects for PDB IDs that have str1 separated
    from str2 by distance."""

    pdbs = contains(str1, distance, str2)

    try:
        mmtfs = redis.Redis().mget(pdbs)
    except Exception:
        mmtfs = fake_redis(pdbs)

    for x,pdb in enumerate(mmtfs):

        if not pdb:
            raise ValueError("Could not find PDB %s in Redis!" % pdb)

        if parsed:
            yield parse_mmtf(pdb)
        else:
            yield pdb

def parse_mmtf(mmtf):
    """ Loads the MMTF objects. """

    mmtf_decoder = MMTFDecoder()
    mmtf = zlib.decompress(mmtf, 16+zlib.MAX_WBITS)
    mmtf = msgpack.unpackb(mmtf)
    mmtf_decoder.decode_data(mmtf)
    return mmtf_decoder

if __name__ == "__main__":
    list(get_mmtfs("XXX", 6, "AAA", parsed=False))
