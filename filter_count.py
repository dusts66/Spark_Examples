#!/usr/bin/python

import sys

from operator import add
from pyspark import SparkContext


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: filter_count <HDFS file path>"
        exit(-1)

    #Giving a Name and using the local Spark Master
    sc = SparkContext(appName="Python Filter Count App")

    lines = sc.textFile(sys.argv[1], 1)


    '''
	Below is where the magic happens.  We are working with the 
	input file from HDFS.  I will outline the steps:

	1. filter - looking at each line we are seeing if 'Lorem ipsum' appears

	2. count - counting the number of lines that 'Lorem ipsum' appears

	'''

    count = lines.filter(lambda line: 'Lorem ipsum' in line).count()

    
    #Outputing the stdout
    print 'Lorem ipsum is in %s lines' % str(count)
