#!/usr/bin/env python

import re
import sys
import json

sequences = json.load(open("ss.json",'r'))

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
