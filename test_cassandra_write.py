#!/usr/bin/env python3
"""
Test Cassandra Write - Simplified test to verify Spark can write to Cassandra
"""
import os
from dotenv import load_dotenv
load_dotenv()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from datetime import datetime

print("=" * 80)
print("CASSANDRA WRITE TEST")
print("=" * 80)

# Initialize Spark
print("\n[1] Initializing Spark...")
spark = SparkSession.builder \
    .appName("CassandraWriteTest") \
    .master("local[*]") \
    .config("spark.jars.packages",
           "org.postgresql:postgresql:42.5.0,"
           "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
    .config("spark.sql.catalog.cassandra", "com.datastax.spark.connector.datasource.CassandraCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✅ Spark initialized")

# Create test DataFrame
print("\n[2] Creating test data...")
test_data = [
    ("BTC-KRW", datetime.now(), "BTC", "upbit", 144700500.0, "KR",
     0.688, "binance", 144800000.0, "GLOBAL", 68.8)
]

df = spark.createDataFrame(test_data, [
    "symbol", "timestamp", "base", "buy_exchange", "buy_price", "buy_region",
    "profit_pct", "sell_exchange", "sell_price", "sell_region", "spread_bps"
])

print("✅ Test data created:")
df.show(truncate=False)

# Try to write to Cassandra
print("\n[3] Writing to Cassandra...")
try:
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .option("keyspace", "financial_data") \
        .option("table", "arbitrage_opportunities") \
        .save()

    print("✅ Successfully wrote to Cassandra!")

    # Verify by reading back
    print("\n[4] Reading back from Cassandra...")
    read_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "financial_data") \
        .option("table", "arbitrage_opportunities") \
        .load()

    print(f"✅ Found {read_df.count()} records in Cassandra")
    read_df.show(5, truncate=False)

except Exception as e:
    print(f"❌ Write failed: {e}")
    import traceback
    traceback.print_exc()

finally:
    spark.stop()
    print("\n[5] Spark stopped")

print("=" * 80)
print("TEST COMPLETE")
print("=" * 80)
