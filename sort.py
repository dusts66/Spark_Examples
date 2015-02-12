#!/usr/bin/python

import sys

from operator import add
from pyspark import SparkContext


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: sort <HDFS file path>"
        exit(-1)

    #Giving a Name and using the local Spark Master
    sc = SparkContext(appName="Python Sort Wordcount App")

    lines = sc.textFile(sys.argv[1], 1)


    '''
	Below is where the magic happens.  We are working with the 
	input file from HDFS.  I will outline the steps:

	1. flatMap - this allows us to take each word(creates a list from the split function)
		         and pass to the map function.

	2. map - this is the map phrase of map/reduce. We are emitting a key value pair of the 
	         word and the int 1 as a key value pair which goes to the reduce function.

	3. reduceByKey - The results from the map function are being aggregated.  We are taking
	                 each occurance of a word and adding each int value of 1 so we can get
	                 the total number of occurances.

	4. sort - Takes the key(which is the word in this case) and sorts
	'''

    sort = lines.flatMap(lambda x: x.split(' ')) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(add) \
        .sortByKey(lambda x: x)


    # This is just a demo on how to bring all the sorted data back to a single node.
    # In reality, we wouldn't want to collect all the data to the driver node.
    output = sort.collect()

    for (name, count) in output:
        print '%s : %s' % (name,str(count))
