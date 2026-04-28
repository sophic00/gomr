#!/usr/bin/env python3
"""
Word count mapper.

Reads text from stdin, emits key\tvalue lines to stdout.
Each word is emitted as: word\t1
"""
import sys

for line in sys.stdin:
    for word in line.strip().split():
        cleaned = ''.join(c for c in word.lower() if c.isalpha())
        if cleaned:
            print(f"{cleaned}\t1")
