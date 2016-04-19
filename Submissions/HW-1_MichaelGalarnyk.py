# Name: Michael Galarnyk
# Email: mgalarny@eng.ucsd.edu
# PID: A09134209
from pyspark import SparkContext
sc = SparkContext()

textRDD = sc.newAPIHadoopFile('/data/Moby-Dick.txt',
                              'org.apache.hadoop.mapreduce.lib.input.TextInputFormat',
                              'org.apache.hadoop.io.LongWritable',
                              'org.apache.hadoop.io.Text',
                               conf={'textinputformat.record.delimiter': "\r\n\r\n"}) \
            .map(lambda x: x[1])

sentences=textRDD.flatMap(lambda x: x.split(". "))


def printOutput(n,freq_ngramRDD):
    top=freq_ngramRDD.take(5)
    print '\n============ %d most frequent %d-grams'%(5,n)
    print '\nindex\tcount\tngram'
    for i in xrange(5):
        print '%d.\t%d: \t"%s"'%(i+1,top[i][0],' '.join(top[i][1]))

import string
import re
regex = re.compile('[%s]' % re.escape(string.punctuation))

def clean(s):  
    return regex.sub('', s)

# n grams using generator comprehension
def BetterNgrams(input_list, n):
    return zip(*(input_list[i:] for i in range(n)))


for n in xrange(1,6):
    # Put your logic for generating the sorted n-gram RDD here and store it in freq_ngramRDD variable
    
    freq_ngramRDD = sentences.map(lambda x: clean(x)).flatMap(lambda x: BetterNgrams(x.split(), n))\
    .map(lambda x: (x, 1))\
    .reduceByKey(lambda x,y:x+y)\
    .map(lambda x:(x[1],x[0]))\
    .sortByKey(False)
    printOutput(n,freq_ngramRDD)   




