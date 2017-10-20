#!/usr/bin/env python

import sys
import zlib
from re import finditer

import msgpack
from collections import deque

sequences, pdb_archive = msgpack.load(open("/raid/trdistance/archive.msg", "rb"))

def _contains(str1, str2, distance_min=None, distance_max=None):
    """ Check if search strings exist in DB separated by distance. """

    matches = {}

    str1 = str1.upper()
    str2 = str2.upper()
    str1l = len(str1)
    str2l = len(str2)

    # Set the defaults
    if distance_min is None:
        distance_min = 0

    if distance_max is None:
        distance_max = distance_min
    distance_max += 1

    for seq in sequences:
        if str1 in seq and str2 in seq:
            s1idx = [m.start() for m in finditer('(?=%s)' % str1, seq)]
            s2idx = deque([m.start()+s1idx[0]+str1l for m in finditer('(?=%s)' % str2, seq[s1idx[0]+str1l:s1idx[-1] + str2l])])

            # If no matching 2nd 3-mer after first one...
            if len(s2idx) == 0:
                continue

            # Go through the matching first residues
            for idx in s1idx:

                # Remove any non-possible 2nd 3-mer, if none remain we are done
                try:
                    while idx > s2idx[0]:
                        s2idx.popleft()
                except IndexError:
                    break

                # Check each possible distance pair
                for s2item in s2idx:
                    distance = s2item - idx - str1l
                    if distance >= distance_min and distance < distance_max:
                        for pdb in set(sequences[seq]):
                            try:
                                matches[pdb].add((idx, distance))
                            except KeyError:
                                matches[pdb] = set([(idx, distance)])

    clean_res = []

    for pdb in sorted(matches.keys()):
        clean_res.append((pdb, sorted(matches[pdb])))

    return clean_res


def get_coords(str1, str2, distance_min=None, distance_max=None, debug=False):
    """ Returns a list of mmtf objects for PDB IDs that have str1 separated
    from str2 by distance_min, or if distance_max is provided, that have str1
    separated from str2 by any distance in the range
    [distance_min - distance_max]."""

    pdbs = _contains(str1, str2, distance_min, distance_max)

    if debug:
        yield pdbs

    for x in pdbs:
        yield [x, pdb_archive[x[0]]]

    return results

if __name__ == "__main__":
    list(get_mmtfs("AAA", "AAA", 6))

#https://stackoverflow.com/questions/30057240/whats-the-fastest-way-to-save-load-a-large-list-in-python-2-7
#https://docs.scipy.org/doc/numpy/reference/generated/numpy.array.html
