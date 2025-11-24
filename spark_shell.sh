#!/bin/bash
# Launch PySpark interactive shell in Docker container

docker exec -it spark-analytics pyspark \
  --conf spark.jars.packages="org.postgresql:postgresql:42.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.3.0,org.mongodb.spark:mongo-spark-connector_2.12:10.1.1" \
  --conf spark.cassandra.connection.host=cassandra \
  --conf spark.cassandra.connection.port=9042 \
  --conf spark.mongodb.read.connection.uri=mongodb://mongodb:27017 \
  --conf spark.mongodb.write.connection.uri=mongodb://mongodb:27017
