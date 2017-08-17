#!/usr/bin/env python

import re
import sys
import zlib
import msgpack

import redis

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

def get_mmtfs(str1, distance, str2):
    """ Returns a list of mmtf objects for PDB IDs that have str1 separated
    from str2 by distance."""

    pdbs = contains(str1, distance, str2)

    try:
        mmtfs = redis.Redis().mget(pdbs)
    except Exception:
        mmtfs = fake_redis(pdbs)

    for x,pdb in enumerate(mmtfs):

        if not pdb:
            raise ValueError("Could not find PDB %s in Redis!" % pdbs[x])

        yield extract_coords(pdb, pdbs[x])

def extract_coords(data, pdb):
    """ Turns the compressed msgpack data into something useful. """

    try:
        return msgpack.loads(zlib.decompress(data))
    except Exception:
        print ("Error: %s" % pdb)

if __name__ == "__main__":
    list(get_mmtfs("AAA", 6, "AAA"))

#https://stackoverflow.com/questions/30057240/whats-the-fastest-way-to-save-load-a-large-list-in-python-2-7
#https://docs.scipy.org/doc/numpy/reference/generated/numpy.array.html
