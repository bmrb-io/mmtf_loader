#!/usr/bin/python

import json

sequences = {}

with open("ss.txt", "r") as input, open("ss.json", "w") as output:
  for line in input:
    if "sequence" in line:
      pdb = line[1:5]
      seq = ""
    elif "secstr" in line:
      try:
        sequences[seq].append(pdb)
      except KeyError:
        sequences[seq] = [pdb]
    else:
      seq += line.strip()

  output.write(json.dumps(sequences))
