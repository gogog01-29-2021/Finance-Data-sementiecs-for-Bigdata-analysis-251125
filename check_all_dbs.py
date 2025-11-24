#!/usr/bin/env python3
"""
Check Status of All Databases in Pipeline
"""

import lmdb
import json
from pathlib import Path
import subprocess
import sys

def check_questdb():
    """Check QuestDB status"""
    print("\n" + "="*80)
    print("QUESTDB (Crypto/Stock Data)")
    print("="*80)

    try:
        import requests
        response = requests.get("http://localhost:9000/exec", params={
            "query": "SELECT COUNT(*) as total FROM orderbook"
        }, timeout=5)

        if response.status_code == 200:
            data = response.json()
            if 'dataset' in data and data['dataset']:
                count = data['dataset'][0][0]
                print(f"Status: CONNECTED")
                print(f"Orderbook records: {count}")

                # Get breakdown by type
                response2 = requests.get("http://localhost:9000/exec", params={
                    "query": "SELECT venue_type, COUNT(*) FROM orderbook GROUP BY venue_type"
                }, timeout=5)

                if response2.status_code == 200:
                    data2 = response2.json()
                    if 'dataset' in data2 and data2['dataset']:
                        print(f"\nBreakdown:")
                        for row in data2['dataset']:
                            print(f"  {row[0]}: {row[1]} records")
            else:
                print(f"Status: CONNECTED")
                print(f"No data yet (table may not exist)")
        else:
            print(f"Status: ERROR - HTTP {response.status_code}")

    except requests.exceptions.ConnectionError:
        print("Status: NOT CONNECTED (is container running?)")
        print("View at: http://localhost:9000")
    except Exception as e:
        print(f"Status: ERROR - {e}")
        print("View at: http://localhost:9000")

def check_lmdb():
    """Check LMDB status"""
    print("\n" + "="*80)
    print("LMDB (Reddit Sentiment Data)")
    print("="*80)

    DB_PATH = Path("data/reddit/reddit_sentiment_lmdb.db")

    if not DB_PATH.exists():
        print(f"Status: NO DATA")
        print(f"Database not found at: {DB_PATH}")
        print("Run reddit_sentiment.py to collect data")
        return

    try:
        env = lmdb.open(str(DB_PATH), readonly=True)

        with env.begin() as txn:
            cursor = txn.cursor()

            # Count items
            posts = comments = sentiments = 0
            for key, _ in cursor:
                key_str = key.decode()
                if key_str.startswith("post:"):
                    posts += 1
                elif key_str.startswith("comment:"):
                    comments += 1
                elif key_str.startswith("sentiment:"):
                    sentiments += 1

            print(f"Status: CONNECTED")
            print(f"\nData breakdown:")
            print(f"  Posts: {posts}")
            print(f"  Comments: {comments}")
            print(f"  Sentiments: {sentiments}")
            print(f"  Total: {posts + comments + sentiments}")

        env.close()

    except Exception as e:
        print(f"Status: ERROR - {e}")

def check_mongodb():
    """Check MongoDB status"""
    print("\n" + "="*80)
    print("MONGODB")
    print("="*80)

    try:
        from pymongo import MongoClient
        client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=5000)

        # List databases
        dbs = client.list_database_names()
        print(f"Status: CONNECTED")
        print(f"Databases: {', '.join(dbs)}")

        client.close()

    except Exception as e:
        print(f"Status: ERROR - {e}")

def check_neo4j():
    """Check Neo4j status"""
    print("\n" + "="*80)
    print("NEO4J")
    print("="*80)

    try:
        from neo4j import GraphDatabase

        driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))

        with driver.session() as session:
            result = session.run("MATCH (n) RETURN count(n) as count")
            count = result.single()["count"]
            print(f"Status: CONNECTED")
            print(f"Total nodes: {count}")

        driver.close()

    except Exception as e:
        print(f"Status: ERROR - {e}")
        print("View at: http://localhost:7474 (default: neo4j/neo4j)")

def check_cassandra():
    """Check Cassandra status"""
    print("\n" + "="*80)
    print("CASSANDRA")
    print("="*80)

    try:
        from cassandra.cluster import Cluster

        cluster = Cluster(['localhost'], port=9042)
        session = cluster.connect()

        # List keyspaces
        rows = session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
        keyspaces = [row.keyspace_name for row in rows]

        print(f"Status: CONNECTED")
        print(f"Keyspaces: {', '.join(keyspaces)}")

        cluster.shutdown()

    except Exception as e:
        print(f"Status: ERROR - {e}")

def main():
    print("="*80)
    print("DATABASE STATUS CHECK")
    print("="*80)

    # Check each database
    check_questdb()
    check_lmdb()
    check_mongodb()
    check_neo4j()
    check_cassandra()

    print("\n" + "="*80)
    print("DONE")
    print("="*80)

if __name__ == "__main__":
    main()
