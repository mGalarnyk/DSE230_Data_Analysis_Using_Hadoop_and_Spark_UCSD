import sys

previus = None
total = 0
isABC = 0

for line in sys.stdin:
    key_value  = line.strip().split("\t")

    key     = key_value[0]
    value   = key_value[1]

    if (previus != key):
        if (isABC == 1):
            print("%s\t%s" % (previus, total))
            isABC = 0
            total = 0
        if (value != "ABC"):
            total = int(value)
    elif (value != "ABC"):
            total = int(total) + int(value)
    if (value == "ABC"):
        isABC = 1

    previus = key
