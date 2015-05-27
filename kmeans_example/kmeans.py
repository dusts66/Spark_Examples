
import sys, re
from datetime import datetime
import time

from pyspark import SparkConf, SparkContext
from pyspark.mllib.clustering import KMeans
from pyspark.sql import HiveContext,SQLContext
from numpy import array


'''
Global Vars(probably put some in CLI args)
'''
job_id = 23

start_time = datetime.now()
#App run time
create_time = datetime.utcnow()
#App author
author = 'ddavidson@livingsocial.net'
#Hive table to import Data
hive_table = 'kmeans_category'
#HDFS Temp Parent directory(this directory should reside in /tmp if not create one)
output_dir = 'spark_ddavidson'
#HDFS Temp Child directory(dynamic directory name based on Unix timestamp inside 'output_dir')
temp_dir = str(int(time.time()))
#Array to store final output of tuples
lat_longs_array = []

#KMeans settings
num_of_clusters = 2
max_iterations = 20
num_of_runs = 10

#Regex to Parse Latitudes and Longitudes
lat_pattern = re.compile('\[(\d{1,2}\.\d+?),')
long_pattern = re.compile(',-?(\d{1,2}\.\d+?)\]')


'''
WORKFLOW

Overview: I predicting the geo location(lat, long) center using KMeans clustering by Pulling
the 'Beauty/Health' category of deals.

1. Pull lat/long data out of local Hive table.

2. Extract out the lat/long using regex and store in a temp array.

3. Convert to a numpy array(needed for KMenas training method).

4. Predict the cluster centers.

5. Write the output to HDFS in /tmp location.

6. Load output from HDFS into local Hive table.
'''


def write_output(line):
    '''
    Creating the HDFS file output format.  This is a tab delimited format and the file will be
    loaded into Hive using the 'LOAD DATA' method.
    '''
    coords = '[%s,%s]' % (str(line[0]),str(line[1]))

    #cluster_centers ARRAY<STRING>,number_of_runs INT,max_iterations INT,clusters INT
    return "%d\t%s\t%d\t%d\t%d\t%s\t%s" % (job_id,coords,num_of_runs,max_iterations,num_of_clusters,create_time, author)


def get_execution_time(start):
    '''
    Calculate the Execution time
    '''
    hours = 0
    minutes = 0
    time_diff = 0
    finish = datetime.now()
    execution_time = finish - start
    str_time = ''

    #Making things readable for the common folk
    if execution_time.seconds > 3600:
        hours = str(execution_time.seconds/3600)
        minutes = str((execution_time.seconds%3600)/60)
        seconds = str((execution_time.seconds%3600)%60)
        str_time = '' + hours + ' hour(s), ' + minutes + ' minute(s), ' + seconds + ' seconds'
    elif execution_time.seconds > 60:
        minutes = str(execution_time.seconds/60)
        seconds = str(execution_time.seconds%60)
        str_time = '' + minutes + ' minute(s), ' + seconds + ' seconds'
    else:
        str_time = '' + str(execution_time.seconds) + ' seconds'

    return str_time




if __name__ == '__main__':
    if (len(sys.argv) != 1):
        print "Usage: spark-submit <Python Code File>"
        sys.exit(1)

    #App name which shows up in the Spark UI
    sc = SparkContext(appName='KMeans Clustering')


    #Context provides connection to Hive metastore
    sqlContext = HiveContext(sc)
    

    '''
    Pulling data out of Hive.  I created a relication of 'watson_bisum_deals' table locally to test.
    '''
    
    rdd = sqlContext.sql("select lat_long_array from watson_bisum_deals where category = 'Beauty/Health' and lat_long_array IS NOT NULL")

    #total records need to organize the numpy array below
    total = rdd.count()
    print 'Total records: %d' % total
    

    #Iterating over the records and Parsing out the Lat and Long.  They were stored as a string so using regex to extract.
    for l in rdd.collect():
        lt = lat_pattern.search(l.lat_long_array[0])
        if lt:
            lats = lt.group(1)
        lg = (long_pattern.search(l.lat_long_array[0]))
        if lg:
            longs = lg.group(1)

        lat_longs_array.append(float(lats))
        lat_longs_array.append(float(longs))
    

    #For the training Model needs to be Numpy Array list so I am reoganizing so there is a list of numpy arrays(ex. array(lat, long))
    data = array(lat_longs_array).reshape(total,2)

    #Trainging the model
    model = KMeans.train(sc.parallelize(data), num_of_clusters, max_iterations, num_of_runs,initializationMode="k-means||")

    #output the cluster centers
    print "Centers:"
    print model.clusterCenters


    '''
    First way to import data into Hive.  Not ideal, but the only way so far because of bugs in 1.2.0
    '''
    
    
    #Drop Table if already exists
    sqlContext.sql("DROP TABLE IF EXISTS %s" % (hive_table))

    #Create Table
    sqlContext.sql("CREATE TABLE IF NOT EXISTS %s(job_id INT,cluster_centers STRING,number_of_runs INT,max_iterations INT,clusters INT,created_at TIMESTAMP,created_by STRING) row format delimited fields terminated by '\t'" % (hive_table))


    #taking the list of Numpy Arrays and creating an RDD so that I can format the output to HDFS
    centers = sc.parallelize(model.clusterCenters)


    #Format the output to write
    dict_rdd = centers.map(write_output)

    #Save output to HDFS
    dict_rdd.saveAsTextFile("/tmp/%s/%s" % (output_dir,temp_dir))

    #Load into Table
    sqlContext.sql("LOAD DATA INPATH '/tmp/%s/%s' into table %s" % (output_dir,temp_dir,hive_table))



    #Get the execution time
    print 'Execution Time: %s' % get_execution_time(start_time)

    #Stopping all cars!
    sc.stop()
    
