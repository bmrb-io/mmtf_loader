#!/usr/bin/env python

from itertools import product

aa_dict = {}
for x in ("".join(x) for x in product("arndbceqghilkmfpstwyv", repeat=3)):
  aa_dict[x] = []

