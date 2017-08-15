#!/bin/sh

wget https://cdn.rcsb.org/etl/kabschSander/ss.txt.gz
gzip -d ss.txt.gz
./process_ss.py
rm ss.txt
