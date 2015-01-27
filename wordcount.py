#!/usr/bin/python

import sys

from operator import add
from pyspark import SparkContext


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: wordcount <HDFS file path>"
        exit(-1)

    #Giving a Name and using the local Spark Master
    sc = SparkContext("local", "Python Wordcount App")

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

	'''

    counts = lines.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)


    '''
	Several options for the output.

	1. We can use the collect function to create an iterable object so we can dump the 
	   records to stdout.  Always be careful if large datasets are returned since this
	   goes into memory.

	2. Save the results back to a file in HDFS.  Note the output path given will be a directory
	   so the results will be partitioned within that directory.  For example, if "/results/wordcount"
	   is the parameter given to the saveAsTextFile function then the resultant file is 
	   "/results/wordcount/part-00000".

	'''

	#Option 1
    output = counts.collect()
    for (word, count) in output:
        print "%s: %i" % (word, count)



	#Option 2
	#wc.saveAsTextFile("/testing/wc_out")
    

    sc.stop()