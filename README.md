Spark Examples
==============

This repo is meant to provide a working VM and some code examples to learn Spark.



## Instructions


####1. Git Clone the Spark_Examples Repo
    
```
git clone http://code.livingsocial.net/DDavidson/Spark_Examples.git
```


####2. Install Virtual Box (Pick your OS)
	
http://www.oracle.com/technetwork/server-storage/virtualbox/downloads/index.html



####3. Install Docker - installs the software to run a VM
	
https://docs.docker.com/installation/

If using Windows or Mac OSx the instructions will ask you to install
boot2docker which runs a VM with the docker daemon.



####4. Pull down the docker container for Spark

```
docker pull ddavidson/spark
```

This will download the container we will use.  It will have Spark and Hadoop installed.

```
docker build --rm -t ddavidson/spark .
```

This will build the container.


####5. Run boot2docker

You can run boot2docker either by:

A. Execute the program so for Mac users click on the icon in Applications or for Window Users find the shortcut in Programs.
	
B. Execute via Mac Terminal CLI.

```
$ boot2docker init
$ boot2docker start
```

This will start up the VM.  If you see instructions like this:

```
To connect the Docker client to the Docker daemon, please set:
    export DOCKER_CERT_PATH=/Users/dustindavidson/.boot2docker/certs/boot2docker-vm
    export DOCKER_TLS_VERIFY=1
    export DOCKER_HOST=tcp://192.168.59.103:2376
```
Execute the commands so the proper environmental variables are set.

See documentation for additional questions 
Mac - https://docs.docker.com/installation/mac/
Windows - https://docs.docker.com/installation/windows/ 


####6. Run the Spark docker container

```
docker run -i -P -t -v <path to Spark_Examples repo>:/home/spark  -h sandbox ddavidson/spark /etc/bootstrap.sh -bash
```
	
This will startup the Spark container and log you into a shell.  The command also mounts your local Spark_Example repo onto the docker container.  This allows you to modify files and code within your local repo and the Spark container will mirror the updates to '/home/spark/' directory.

You can run these examples by using the "spark-submit".  You will need to dump the example data into HDFS and indicate the path in the CLI.  

```
 spark-submit <python file> <HDFS data file>
```