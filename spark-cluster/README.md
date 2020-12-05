# Spark Cluster

## Why a standalone cluster?

* This is intended to be used for test purposes, basically a way of running distributed spark apps on your laptop or desktop.

* TODO future: have a cluster set using Kubernetes

* This will be useful to use CI/CD pipelines for your spark apps

## Docker compose will create:

container|Ip address
---|---
spark-master|10.5.0.2
spark-worker-1|10.5.0.3
spark-worker-2|10.5.0.4
spark-worker-3|10.5.0.5

## Installation
The following steps will make you run your spark cluster's container.

## Build the images 
Execute in terminal:
```
chmod +x build-images.sh
./build-images.sh
```

This will create the following docker images:
- spark-base:2.3.1: Base image on java:alpine-jdk-8 with scala, python and spark 2.3.1.
- spark-master:2.3.1: Image based on the previously image, used to create Spark master container.
- spark-worker:2.3.1: Image based on the previously image, used to create Spark worker container.
- spark-submit:2.3.1: Image based on the previously spark image, used to create Spark submit containers(run, deliver driver and die gracefully).

## Run docker-compose
Final step to create the test cluster:
```
docker-compose up --scale spark-worker=3
```

## Validate your cluster
Just validate your cluster accessing the spark UI on each worker and master URL:

- Spark Master
http://10.5.0.2:8080/

### Spark Worker 2

http://10.5.0.4:8081/

### Spark Worker 3

http://10.5.0.5:8081/

## Resource Allocation
This cluster is shipped with three workers and one spark master, each of these has a particular set of resource allocation(CPU and RAM cores allocation).

- The default CPU core allocation for each Spark worker is 1 core.
- The default RAM for each spark-worker is 1024 MB.
- The default RAM allocation for spark executirs is 256MB.
- The default RAM allocation for spark driver is 128MB.

If you wish to modify this allocation, please edit env/spark-worker.sh file.

## Binded Volumes
To make app running easier, there are two volumes mounts:

Host Mount|Container Mount|Purposse
---|---|---
/mnt/spark-apps|/opt/spark-apps|Used to make available your app's jars on all workers & master
/mnt/spark-data|/opt/spark-data| Used to make available your app's data on all workers & master

## Run a sample application:
Valid spark sumit:

## Create a Scala spark app
... Soon

## Ship your jar & dependencies on the Workers and Master
A necessary step to make **spark-submit** is to copy your application bundle into all workers, also configuration file or input files needed.

As we're using Docker volumes, just copy your app and configs into /mnt/spark-apps, and your input files into /mnt/spark-files.

```
#Copy spark application into all workers' app folder
cp /home/workspace/YOUR_SPARK_APP/build/libs/YOUR_SPARK_APP.jar /mnt/spark-apps

#Copy spark application configs into all workers' app folder
cp -r /home/workspace/YOUR_SPARK_APP/config /mnt/spark-apps

#Copy the file to be processed to all workers' data folder
cp /home/MY_SUPER_DATA.csv /mnt/spark-files
```

## Check the successful copy of the data and app jar
You can check that the app code and files are in place before running the spark-submit:

```
# Worker 1 Validation
docker exec -it spark-worker-1 ls -l /opt/spark-apps
docker exec -it spark-worker-1 ls -l /opt/spark-data

# Worker 2 Validation
docker exec -it spark-worker-2 ls -l /opt/spark-apps
docker exec -it spark-worker-2 ls -l /opt/spark-data

# Worker 3 Validations
docker exec -it spark-worker-3 ls -l /opt/spark-apps
docker exec -it spark-worker-3 ls -l /opt/spark-data
```
After running one of this commands you have to see your app's jar and files.

## Docker spark-submit
```
# Creating some variables to make docker run command more readable
# App jar environment used by the spark-submit image
SPARK_APPLICATION_JAR_LOCATION="/opt/spark-apps/MY_SUPER_APP.jar"
# App main class environment used by the spark-submit image
SPARK_APPLICATION_MAIN_CLASS="org.sparklab.applications.MySuperApp"
# Extra submit args used by the spark-submit image
SPARK_SUBMIT_ARGS="--conf spark.executor.extraJavaOptions='-Dconfig-path=/opt/spark-apps/dev/config.conf'"

# We have to use the same network as the spark cluster(internally the iage resolves spark master as spark://spark-master:7077)
docker run --network docker-spark-cluster_spark-network \
-v /mnt/spark-apps:/opt/spark-apps \
--env SPARK_APPLICATION_JAR_LOCATION=$SPARK_APPLICATION_JAR_LOCATION \
--env SPARK_APPLICATION_MAIN_CLASS=$SPARK_APPLICATION_MAIN_CLASS \
spark-submit:2.3.1
```

