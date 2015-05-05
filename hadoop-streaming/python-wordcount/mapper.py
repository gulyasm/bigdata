#!/usr/bin/python

import sys

for line in sys.stdin:
    line = line.strip()
    words = line.split()
    if len(line) > 1:
        for word in words:
            print '%s\t%d' % (word, 1)
