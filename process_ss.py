#!/usr/bin/env python

import re
import sys
import json

sequences = {}

with open("ss.txt", "r") as ss_data, open("ss.json", "w") as output:

    mode = True
    for line in ss_data:
        if "sequence" in line:
            pdb = line[1:5]
            seq = ""
            mode = True
        elif "secstr" in line:
            mode = False
            sequences.setdefault(seq,[]).append(pdb)
        else:
            if mode:
                seq += line.strip()

    json.dump(sequences, output)
