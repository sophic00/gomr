#!/usr/bin/env python3
"""
Word count reducer.

Reads sorted key\tvalue lines from stdin (all values for the same key are
consecutive), emits aggregated key\tcount lines to stdout.
"""
import sys

current_key = None
count = 0

for line in sys.stdin:
    key, value = line.strip().split('\t', 1)
    if key == current_key:
        count += int(value)
    else:
        if current_key is not None:
            print(f"{current_key}\t{count}")
        current_key = key
        count = int(value)

if current_key is not None:
    print(f"{current_key}\t{count}")
