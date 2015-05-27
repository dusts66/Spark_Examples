#!/usr/bin/python

import sys
from datetime import datetime
import itertools
import time
from math import sqrt

from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation import ALS, Rating
from pyspark.sql import HiveContext,SQLContext,Row


'''
Global Vars(probably put some in CLI args)
'''
start_time = datetime.now()
#App run time
create_time = datetime.utcnow()
#App author
author = 'ddavidson@livingsocial.net'
#Hive table to import Data
hive_table = 'user_rec'
#HDFS Temp Parent directory(this directory should reside in /tmp if not create one)
output_dir = 'spark_ddavidson'
#HDFS Temp Child directory(dynamic directory name based on Unix timestamp inside 'output_dir')
temp_dir = str(int(time.time()))
#Array to store final output of tuples
user_recs = []
#Number of recommendations to keep per user
recs_to_keep = 5


'''
WORKFLOW
1. Determine a rating based on whether the user paid for the purchase.  Measure by assigning a 1 or 0 on
   whether a purchase was made(1 for purchase, 0 for not).

2. RDD of ratings -- (person_id, deal_id, aasm_state)

    -- make aasm_state a boolean rating: 1 for paid, 0 for not
        ex. (time , (12213,12323,1))
                 person, deal, paid

    -- Create sample datasets to test and find the best model
    

3. Train Implicit feedback model

4. Predict off the best Model
    
    -- Producing the top five recommendations per user

5. Write the output to HDFS in /tmp location

6. Load output from HDFS into local Hive table.
'''


def parse_rating(line):
    '''
    Parses a rating record
    (person_id,deal_id,aasm_state))
    '''
    paid = 1.0 if line.aasm_state == 'paid' or line.aasm_state == 'authorized' else 0.0
    return Rating(int(line.person_id),int(line.deal_id),float(paid))


def write_output(line):
    '''
    Creating the HDFS file output format.  This is a tab delimited format and the file will be
    loaded into Hive using the 'LOAD DATA' method.
    '''

    return "%d\t%d\t%f\t%s\t%s" % (int(line[0]),int(line[1]),float(line[2]),create_time, author)


def computeRmse(model, ratings, n):
    '''
    Compute RMSE (Root Mean Squared Error).
    '''
    testdata = ratings.map(lambda p: (p[0], p[1]))
    predictions = model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))
    ratesAndPreds = ratings.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
    MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).reduce(lambda x, y: x + y)/ratesAndPreds.count()
    return MSE

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
    sc = SparkContext(appName='User Recommendation')


    #Context provides connection to Hive metastore
    sqlContext = HiveContext(sc)
    

    '''
    Pulling data out of Hive.  I created a relication of 'watson_bisum_purchases' table locally to test.
    '''
    
    rdd = sqlContext.sql("SELECT person_id,deal_id,aasm_state FROM watson_bisum_purchases")


    '''
    Creating datasets.  Formating the data and also creating sample datasets in order to create and test the model. 
    '''
    
    #Formating all the data using the 'parse_rating' method above
    all_data = rdd.map(parse_rating)
    rec_list = sc.parallelize(all_data.collect())


    #Grabbing all Unique Users(used for building recommendation list)
    users = rdd.groupBy(lambda x: x.person_id).map(lambda x: x[0]).collect()

    #Grabbing all Unique Deals/Products(used for building recommendation list)
    deals = rdd.groupBy(lambda x: x.deal_id).map(lambda x: x[0]).collect()


    #Making some sample sets of data in order to test and validation the model chosen(used during the iteration below)
    validation = all_data.sample(False,0.3,4)
    test = all_data.sample(False,0.3,6)

    
    #Counts are needed to compute the RMSE(root mean square error)
    numValidation = validation.count()
    numTest = test.count()
    

    #Not sure on What numbers(used to produce a model) to use so these are arbitrary
    ranks = [8,12]
    lambdas = [0.1, 1.0]
    numIters = [10,20]
    bestModel = None
    bestValidationRmse = float('inf')
    bestRank = 0
    bestLambda = -1.0
    bestNumIter = -1


    
    #Developing the best model.  Using different values to find the model with the least Mean Square Error.
    #Once I find that I will use that model to predict results. 
    for rank,lmbda,numIter in itertools.product(ranks,lambdas,numIters):
        
        model = ALS.trainImplicit(rec_list, rank, numIter,lambda_=lmbda,alpha=0.01)
        
        validationRmse = computeRmse(model, validation, numValidation)
        
        #print "RMSE (validation) = %s for the model trained with " % str(validationRmse) + \
        #      "rank = %d, lambda = %f, and numIter = %d." % (rank,lmbda,numIter)
        if (validationRmse < bestValidationRmse):
            bestModel = model
            bestValidationRmse = validationRmse
            bestRank = rank
            bestLambda = lmbda
            bestNumIter = numIter
    

    #Computing Best MSE to display
    testRmse = computeRmse(bestModel, test, numTest)

    #Logging out the model results
    print "The best model was trained with rank = %d and lambda = %.1f, " % (bestRank, bestLambda) \
      + "and numIter = %d, and its RMSE on the test set is %f." % (bestNumIter, testRmse)

    
    '''
    Using the distinct User list, I am iterating through every user and every deal to make a prediction.
    A list of the predictions are store as tuples and then sorted by rank to produce a top {recs_to_keep} number.  
    These records are imported into Hive.
    '''
    for user in users:
        predictions = []
        for deal in deals:
            prediction = bestModel.predict(user,deal)
            predictions.append((user,deal,prediction))

        recommendations = sorted(predictions, key=lambda x: x[2], reverse=True)[:recs_to_keep]
        for i in xrange(len(recommendations)):
            user_recs.append(recommendations[i])
    


    '''
    First way to import data into Hive.  Not ideal, but the only way so far because of bugs in 1.2.0
    '''
    
    #Drop Table if already exists
    sqlContext.sql("DROP TABLE IF EXISTS %s" % (hive_table))

    #Create Table
    sqlContext.sql("CREATE TABLE IF NOT EXISTS %s(user_id INT,deal_id INT,rating_num DOUBLE,created_at TIMESTAMP,created_by STRING) row format delimited fields terminated by '\t'" % (hive_table))

    #taking the list of tuples and creating an RDD so that futher manipulation can happen
    predictions = sc.parallelize(user_recs)

    #Format the output to write
    dict_rdd = predictions.map(write_output)

    #Save output to HDFS
    dict_rdd.saveAsTextFile("/tmp/%s/%s" % (output_dir,temp_dir))

    #Load into Table
    sqlContext.sql("LOAD DATA INPATH '/tmp/%s/%s' into table %s" % (output_dir,temp_dir,hive_table))




    '''
    #Second way to import data.  Parsing of Row object is miss aligning data to columns.  Not sure on the issue.  Still trying to research.

    #Drop Table if already exists
    sqlContext.sql("DROP TABLE IF EXISTS %s" % (hive_table))

    #Create Table
    sqlContext.sql("CREATE TABLE IF NOT EXISTS %s(user_id INT,deal_id INT,rating_num DOUBLE,created_at TIMESTAMP,created_by STRING)" % (hive_table))

    # creates rdd with dictionary values in order to create schemardd
    dict_rdd = predictions.map(lambda x: Row(user_id=int(x[0]),deal_id=int(x[1]),rating=float(x[2]),created_at=create_time,created_by=author)

    # infer the schema
    schema_rdd = sqlContext.inferSchema(dict_rdd)

    # save
    schema_rdd.insertInto(hive_table)
    '''




    '''
    #Third way to import data.  Currently a bug in 1.2.0 that casues 'saveAsTable' method to fail(https://issues.apache.org/jira/browse/SPARK-4825).

    #Drop Table if already exists
    sqlContext.sql("DROP TABLE IF EXISTS %s" % (hive_table))

    # creates rdd with dictionary values in order to create schemardd
    dict_rdd = predictions.map(lambda x: Row(user_id=int(x[0]),deal_id=int(x[1]),rating=float(x[2]),created_at=create_time,created_by=author))
    
    #Infer the schema
    schema_rdd = sqlContext.inferSchema(dict_rdd)

    # save
    schema_rdd.saveAsTable(hive_table)
    '''


    #Get the execution time
    print 'Execution Time: %s' % get_execution_time(start_time)

    #Stopping all cars!
    sc.stop()
    
