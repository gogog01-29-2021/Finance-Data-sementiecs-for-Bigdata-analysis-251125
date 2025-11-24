#!/bin/bash
# Start PySpark Shell with all database connectors loaded

echo "=========================================="
echo "Starting PySpark Shell"
echo "=========================================="
echo "This will download JARs on first run..."
echo ""

export PYSPARK_PYTHON=/c/Users/user/miniconda3/envs/python=3.10/python
export PYSPARK_DRIVER_PYTHON=/c/Users/user/miniconda3/envs/python=3.10/python

/c/Users/user/miniconda3/envs/python=3.10/python -m pyspark \
  --packages org.postgresql:postgresql:42.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.3.0,org.mongodb.spark:mongo-spark-connector_2.12:10.1.1 \
  --conf spark.cassandra.connection.host=localhost \
  --conf spark.cassandra.connection.port=9042 \
  --conf spark.mongodb.read.connection.uri=mongodb://localhost:27017 \
  --conf spark.mongodb.write.connection.uri=mongodb://localhost:27017
