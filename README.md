#Pyspark - KMeans

The following example uses KMeans clustering to produce geographic Lat/Long centers based on purchases.  Steps are outlined below to setup the local environment to run the example.  Check out the [Spark](http://spark.apache.org/) docs for the lastest functionality.

**Note:  using Spark 1.2.0 package with CDH 5.3**



###Steps

**The following directions assume you have a Spark environment up and running.  If not, go here [Spark VM](http://code.livingsocial.net/DDavidson/hadoop/tree/spark_vm).**


###Setup Spark Environment
1. I grabbed sample from the watson_bisum_deals to import into my Local Hive table.  I logged onto i75 and pulled the sample data into my home directory.  From there use rsync or scp to pull to your local machine.


  A. Getting sample Data
  
  ```
  hive -e 'select * from watson_bisum_deals limit 10000' > deals_sample_data.tsv
  ```


  B. Download the file

  **Note:  Copying the file to the `/home/hadmin`directory inside the VM.** 
  
  ```
  scp -3 {user name}@{server name}:{path to sample file} hadmin@hadoop-manager:/home/hadmin
  ```


2. I set up a local hive table to mimic a production table 'watson_bisum_deals'.  *Run from inside the VM*.  
  

  A. Use the create_watson_bisum_deals.sql file to create a local hive table(run as hadmin).

  ```
  hive -f /home/hadmin/kmeans_example/create_watson_bisum_deals.sql
  ```


  B. Import the sample data file into the table.  Use the load_data.sql file.

  ```
  hive -f /home/hadmin/kmeans_example/load_data.sql
  ```

  C. Check Hive to sure everything looks good.

  Is the Table there?
  ```
  hive -e 'SHOW TABLES'
  ```

  Check data:
  ```
  hive -e 'select * from watson_bisum_deals limit 5' 
  ```




###Running Spark


1. Log into the VM

  ```
  ssh hadmin@hadoop-manager
  ```



2. Use the spark-submit utility to run the app(always run as hadmin user).

  ```
  spark-submit {path to file}/kmeans.py
  ```

####Notes about the Running of the Spark App
  - Currently the app uses two centers in order the cluster the data points. The purchases are filtered by the 'Beauty/Health' category so results find the center lat/long based on 'Beauty/Health' purchases
  - Data for the input are pulled from your local hive table 'watson_bisum_deals'
  - The results are the Lat/Long of two centers where the data points are clustered
  - The results are written to HDFS temporarily(tab delimited file) in the /tmp/{output_dir}/{unix_timestamp}.  The {output_dir} is set by the user while the {unix_timestamp} is a dynamic directory in order to avoid errors.  **Note: the {output_dir} must exist prior to running to avoid errors.
  - The results are finally loaded into Hive.  The table name is set by the user {hive_table}






###Using Pyspark Repl

The code contained with the 'kmeans.py' file can also be executed use the pyspark repl.  This utility acts just like a python repl, and is good just to test and experiment

Start the repl:

```
pyspark
```
