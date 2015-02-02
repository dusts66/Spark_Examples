#!/usr/bin/python

import sys

from operator import add
from pyspark import SparkContext
from pyspark.sql import SQLContext


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: app <first HDFS file path> <second HDFS file path>"
        exit(-1)

    #Giving a Name and using the local Spark Master
    sc = SparkContext(appName="Python Spark SQL Join App")
    sql_Context = SQLContext(sc)


    #Reading the Weight File
    file_weight = sc.textFile(sys.argv[1], 1)

    '''
    	Couple of things going on here.  

    	1. The file is comma delimited so I split in order to map the data to a schema.
    	2. Using the split I map the data to JSON which will be used to create the Spark Schema.
    	3. Lastly I register a table based on the Schema I created 

    '''
    weight = file_weight.map(lambda l: l.split(",")) \
                        .map(lambda p:{"name": p[0],"weight":p[1]})

    weight_table = sql_Context.inferSchema(weight)
    weight_table.registerAsTable("weight")


    #Reading the Age File
    file_age = sc.textFile(sys.argv[2], 1)


    '''
    	Same explanation as above on the workflow
    '''

    age = file_age.map(lambda l: l.split(",")) \
                        .map(lambda p:{"name": p[0],"age":p[1]})

    age_table = sql_Context.inferSchema(age)
    age_table.registerAsTable("age")


    '''
    	Two things happening
    	1. Executing SQL to get the data based on my Schema definitions
    	2. Collecting the data which pulls in from all workers into this process
    		and creates an iterable object I can dump to stdout
    '''

    people_weight = sql_Context.sql("SELECT name,weight FROM weight").collect()
    people_age = sql_Context.sql("SELECT name,age FROM age").collect()

    for (name,weight) in people_weight:
        print "Name: %s, Weight: %s" % (name, str(weight))

    for (name,age) in people_age:
        print "Name: %s, Age: %s" % (name, str(age))

    


    '''
    	Creating a JOIN between the two tables
    '''
    join_people = sql_Context.sql("SELECT w.name, w.weight, a.age FROM weight w JOIN age a ON w.name = a.name").collect()

    for (name,weight,age) in join_people:
        print "Name: %s Weight: %s Age: %s" % (name,str(weight),str(age))