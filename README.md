### SparkLab

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