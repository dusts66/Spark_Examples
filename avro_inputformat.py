import sys

from pyspark import SparkContext

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print >> sys.stderr, """
        Usage: avro_inputformat.py <data_file>
        Run with example jar:
        spark-submit --driver-class-path /path/to/example/jar /path/to/examples/avro_inputformat.py <data_file>
        """
        exit(-1)

    path = sys.argv[1]
    sc = SparkContext(appName="AvroKeyInputFormat")

    conf = None

    avro_rdd = sc.newAPIHadoopFile(
        path,
        "org.apache.avro.mapreduce.AvroKeyInputFormat",
        "org.apache.avro.mapred.AvroKey",
        "org.apache.hadoop.io.NullWritable",
        keyConverter="org.apache.spark.examples.pythonconverters.AvroWrapperToJavaConverter",
        conf=conf)
    output = avro_rdd.map(lambda x: x[0]).collect()
    for k in output:
        print k

    sc.stop()