#!/usr/bin/env python

# ---------------------------------------------------------------
#This reducer code will input a line of text and 
#    output <word, total-count>
# ---------------------------------------------------------------
import sys

last_key      = None              #initialize these variables
running_total = 0

# -----------------------------------
# Loop thru file
#  --------------------------------
for input_line in sys.stdin:
    input_line = input_line.strip()

    # --------------------------------
    # Get Next Word    # --------------------------------
    this_key, value = input_line.split("\t", 1)  #the Hadoop default is tab separates key value
                          #the split command returns a list of strings, in this case into 2 variables
    value = int(value)           #int() will convert a string to integer (this program does no error checking)
 
    # ---------------------------------
    # Key Check part
    #    if this current key is same 
    #          as the last one Consolidate
    #    otherwise  Emit
    # ---------------------------------
    if last_key == this_key:     #check if key has changed ('==' is                                   #      logical equalilty check
        running_total += value   # add value to running total

    else:
        if last_key:             #if this key that was just read in
                                 #   is different, and the previous 
                                 #   (ie last) key is not empy,
                                 #   then output 
                                 #   the previous <key running-count>
            print( "{0}\t{1}".format(last_key, running_total) )
                                 # hadoop expects tab(ie '\t') 
                                 #    separation
        running_total = value    #reset values
        last_key = this_key

if last_key == this_key:
    print( "{0}\t{1}".format(last_key, running_total))
