### SparkLab

What Spark is?

Unified computing engine & libraries for distributed data processing which supports:
- data loading
- SQL queries
- machine learning
- streaming

#### Misconceptions 
Spark is not concerned with data sources:
- files
- Azure
- S3
- Hadoop/HDFS
- Cassandra
- Postgres
- Kafka

Spark is not part of Hadoop.

### Running
Build and start PostgreSQL container that will be used to interact with from Spark:
```
docker-compose up
```

Connect to DB: 
```
chmod +x ./psql.sh
./psql.sh
```


Go to spark-cluster in another terminal and run docker-compose up --scale spark-worker=3:
```
chmod +x build-images.sh
./build-images.sh
```

#### Architecture

[Applications] Streaming | ML | GraphX | Other libraries
[High-level (structured) APIS] Data Frames | Datasets | Spark SQL
[Low-Level] RDDs | Distributed variables