from operator import add
import sys
from pyspark import SparkContext

#Giving a Name and using the local Spark Master
sc = SparkContext(appName="LZO Wordcount")


if __name__ == "__main__":
	if len(sys.argv) != 2:
		print >> sys.stderr, """
		Usage: wordcount_lzo_file.py <data_file>
		Run with example jar:
		spark-submit --driver-class-path /home/spark/lib/hadoop-lzo.jar /path/to/examples/wordcount_lzo_file.py <data_file>
		"""
		exit(-1)

	path = sys.argv[1]
	print path
	conf = None

	#Reading a file in HDFS(use absolute path)
	csv = sc.newAPIHadoopFile(path,"com.hadoop.mapreduce.LzoTextInputFormat","org.apache.hadoop.io.LongWritable","org.apache.hadoop.io.Text").count()

	print csv
    
    #for k in output:
    #    print k
