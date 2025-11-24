#!/usr/bin/env python3
"""
Query All Databases - View All Your Financial Data
"""

import sys
sys.path.insert(0, '/c/Users/user/miniconda3/envs/python=3.10/lib/python3.13/site-packages')

import requests
from pymongo import MongoClient
from neo4j import GraphDatabase
import lmdb
import json
from pathlib import Path

print("=" * 80)
print("FINANCIAL DATA PIPELINE - ALL DATA VIEWER")
print("=" * 80)

# 1. QUESTDB - Orderbook Data
print("\n" + "=" * 80)
print("1. QUESTDB - Crypto/Stock Orderbook Data")
print("=" * 80)

try:
    # Total count
    response = requests.get("http://localhost:9000/exec", params={
        "query": "SELECT COUNT(*) as total FROM orderbook"
    }, timeout=5)

    if response.status_code == 200:
        total = response.json()['dataset'][0][0]
        print(f"Total Records: {total:,}")

        # By venue type
        response = requests.get("http://localhost:9000/exec", params={
            "query": "SELECT venue_type, COUNT(*) as count FROM orderbook GROUP BY venue_type"
        })
        print("\nBy Type:")
        for row in response.json()['dataset']:
            print(f"  {row[0]}: {row[1]:,} records")

        # By exchange (top 10)
        response = requests.get("http://localhost:9000/exec", params={
            "query": "SELECT exchange, COUNT(*) as count FROM orderbook GROUP BY exchange ORDER BY count DESC LIMIT 10"
        })
        print("\nTop 10 Exchanges:")
        for row in response.json()['dataset']:
            print(f"  {row[0]}: {row[1]:,} records")

        # Recent samples
        response = requests.get("http://localhost:9000/exec", params={
            "query": "SELECT timestamp, exchange, symbol, mid_price FROM orderbook ORDER BY timestamp DESC LIMIT 5"
        })
        print("\nRecent Samples:")
        for row in response.json()['dataset']:
            print(f"  {row[1]:10} {row[2]:15} ${row[3]:>10.2f} @ {row[0]}")

except Exception as e:
    print(f"Error: {e}")

# 2. LMDB - Reddit Sentiment
print("\n" + "=" * 80)
print("2. LMDB - Reddit Sentiment Data")
print("=" * 80)

try:
    DB_PATH = Path("data/reddit/reddit_sentiment_lmdb.db")

    if DB_PATH.exists():
        env = lmdb.open(str(DB_PATH), readonly=True)

        with env.begin() as txn:
            cursor = txn.cursor()

            posts = comments = sentiments = 0
            for key, _ in cursor:
                key_str = key.decode()
                if key_str.startswith("post:"):
                    posts += 1
                elif key_str.startswith("comment:"):
                    comments += 1
                elif key_str.startswith("sentiment:"):
                    sentiments += 1

            print(f"Posts: {posts}")
            print(f"Comments: {comments}")
            print(f"Sentiments: {sentiments}")
            print(f"Total: {posts + comments + sentiments}")

            # Sample sentiments
            print("\nRecent Sentiments:")
            cursor.first()
            count = 0
            for key, value in cursor:
                if key.decode().startswith("sentiment:") and count < 5:
                    data = json.loads(value.decode())
                    sent = data.get('sentiment', {})
                    print(f"  r/{data['subreddit']:20} {sent.get('label', 'N/A'):10} (polarity: {sent.get('polarity', 0):.2f})")
                    count += 1

        env.close()
    else:
        print("No data yet")

except Exception as e:
    print(f"Error: {e}")

# 3. MONGODB - Analytics Results
print("\n" + "=" * 80)
print("3. MONGODB - Analytics Results")
print("=" * 80)

try:
    client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=5000)
    db = client['financial_analytics']

    collections = db.list_collection_names()
    print(f"Collections: {collections}\n")

    for coll_name in collections:
        coll = db[coll_name]
        count = coll.count_documents({})
        print(f"{coll_name}: {count} documents")

        # Show sample
        if count > 0:
            sample = coll.find_one()
            print(f"  Sample keys: {list(sample.keys())[:10]}")

    client.close()

except Exception as e:
    print(f"Error: {e}")

# 4. CASSANDRA - Analytics Results
print("\n" + "=" * 80)
print("4. CASSANDRA - Analytics Results")
print("=" * 80)

try:
    # Use eventlet for Cassandra driver
    import os
    os.environ['EVENTLET_NO_GREENDNS'] = 'yes'
    import eventlet
    eventlet.monkey_patch()

    from cassandra.cluster import Cluster
    from cassandra.io.eventletreactor import EventletConnection
    from cassandra import cluster as cassandra_cluster
    cassandra_cluster.connection_class = EventletConnection

    cluster = Cluster(['localhost'], port=9042)
    session = cluster.connect()
    session.execute("USE financial_data")

    tables = ['arbitrage_opportunities', 'price_predictions']

    for table in tables:
        result = session.execute(f"SELECT COUNT(*) FROM {table}")
        count = result.one()[0]
        print(f"{table}: {count} records")

        # Show sample
        if count > 0:
            result = session.execute(f"SELECT * FROM {table} LIMIT 3")
            print(f"  Columns: {result.column_names[:10]}")

    cluster.shutdown()

except Exception as e:
    print(f"Error: {e}")

# 5. NEO4J - Graph Analytics
print("\n" + "=" * 80)
print("5. NEO4J - Graph Analytics")
print("=" * 80)

try:
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))

    with driver.session() as session:
        # Total nodes
        result = session.run("MATCH (n) RETURN count(n) as count")
        count = result.single()["count"]
        print(f"Total Nodes: {count}")

        # Node types
        result = session.run("MATCH (n) RETURN DISTINCT labels(n) as labels, count(*) as count")
        print("\nNode Types:")
        for record in result:
            print(f"  {record['labels']}: {record['count']}")

        # Relationships
        result = session.run("MATCH ()-[r]->() RETURN type(r) as type, count(*) as count")
        print("\nRelationships:")
        for record in result:
            print(f"  {record['type']}: {record['count']}")

    driver.close()

except Exception as e:
    print(f"Error: {e}")

print("\n" + "=" * 80)
print("DONE")
print("=" * 80)
