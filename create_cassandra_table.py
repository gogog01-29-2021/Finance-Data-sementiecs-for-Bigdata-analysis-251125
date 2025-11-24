#!/usr/bin/env python3
"""
Create unified Cassandra table schema
Works in Docker with Python 3.11 + Linux
"""

from cassandra.cluster import Cluster
import os
from dotenv import load_dotenv
import time

load_dotenv()

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))

print("Connecting to Cassandra...")
# Retry connection up to 5 times (for Docker startup)
for attempt in range(5):
    try:
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect()
        break
    except Exception as e:
        if attempt < 4:
            print(f"Connection attempt {attempt + 1} failed, retrying in 5s...")
            time.sleep(5)
        else:
            print(f"Failed to connect to Cassandra: {e}")
            raise

print("Creating keyspace...")
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS financial_data
    WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")

session.set_keyspace('financial_data')

print("Creating unified analytics table...")
session.execute("""
    CREATE TABLE IF NOT EXISTS analytics_unified (
        analytics_type text,
        date_bucket text,
        timestamp timestamp,
        symbol text,
        exchange text,
        subreddit text,
        base text,
        quote text,
        value1 double,
        value2 double,
        value3 double,
        count_metric bigint,
        signal_type text,
        category text,
        metadata map<text, text>,
        alert_detected boolean,
        PRIMARY KEY ((analytics_type, date_bucket), timestamp, symbol, exchange, subreddit)
    ) WITH CLUSTERING ORDER BY (timestamp DESC, symbol ASC, exchange ASC, subreddit ASC)
""")

print("Creating indexes...")
session.execute("CREATE INDEX IF NOT EXISTS idx_unified_symbol ON analytics_unified (symbol)")
session.execute("CREATE INDEX IF NOT EXISTS idx_unified_exchange ON analytics_unified (exchange)")
session.execute("CREATE INDEX IF NOT EXISTS idx_unified_subreddit ON analytics_unified (subreddit)")
session.execute("CREATE INDEX IF NOT EXISTS idx_unified_signal_type ON analytics_unified (signal_type)")
session.execute("CREATE INDEX IF NOT EXISTS idx_unified_alert ON analytics_unified (alert_detected)")

print("✓ Unified table created successfully!")
cluster.shutdown()
