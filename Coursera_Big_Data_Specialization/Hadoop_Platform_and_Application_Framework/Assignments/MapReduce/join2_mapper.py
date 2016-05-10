#!/usr/bin/env python
import sys

for line in sys.stdin:
    key_value  = line.strip().split(",")
    key        = key_value[0]
    value      = key_value[1]

    if value == "ABC":           
        print("%s\t%s" % (key, value))
    elif value.isdigit():
        print("%s\t%s" % (key, value))
