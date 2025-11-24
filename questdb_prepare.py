#!/usr/bin/env python3
"""
QuestDB Setup Script for Orderbook Streaming
Creates necessary tables and verifies connection
"""

import requests
import time
from datetime import datetime

QUESTDB_HOST = "localhost"
QUESTDB_PORT = 9000
BASE_URL = f"http://{QUESTDB_HOST}:{QUESTDB_PORT}"

def execute_sql(query):
    """Execute SQL query via QuestDB HTTP endpoint"""
    try:
        response = requests.get(
            f"{BASE_URL}/exec",
            params={"query": query}
        )
        return response.json() if response.text else {"status": "ok"}
    except Exception as e:
        return {"error": str(e)}

def create_orderbook_table():
    """Create the orderbook table with proper schema"""
    
    # First, drop the table if it exists (for clean slate)
    print("Dropping existing orderbook table if exists...")
    drop_result = execute_sql("DROP TABLE IF EXISTS orderbook")
    print(f"Drop result: {drop_result}")
    
    # Create the new table
    print("\nCreating orderbook table...")
    create_query = """
    CREATE TABLE orderbook (
        exchange SYMBOL capacity 256 CACHE,
        symbol SYMBOL capacity 256 CACHE,
        base SYMBOL capacity 256 CACHE,
        quote SYMBOL capacity 256 CACHE,
        region SYMBOL capacity 256 CACHE,
        venue_type SYMBOL capacity 256 CACHE,
        instance SYMBOL capacity 256 CACHE,
        seq LONG,
        best_bid DOUBLE,
        best_ask DOUBLE,
        mid_price DOUBLE,
        spread_bps DOUBLE,
        recv_ts_s DOUBLE,
        depth_json STRING,
        raw_json STRING,
        ts TIMESTAMP
    ) timestamp(ts) PARTITION BY DAY
    """
    
    create_result = execute_sql(create_query)
    print(f"Create result: {create_result}")
    
    return create_result

def verify_table():
    """Verify the table exists and show its structure"""
    print("\nVerifying table existence...")
    
    # Check if table exists
    tables_query = "SHOW TABLES"
    tables_result = execute_sql(tables_query)
    print(f"Tables in database: {tables_result}")
    
    # Show table structure
    structure_query = "SHOW COLUMNS FROM orderbook"
    structure_result = execute_sql(structure_query)
    print(f"\nOrderbook table structure:")
    if isinstance(structure_result, dict) and "dataset" in structure_result:
        for col in structure_result["dataset"]:
            print(f"  - {col}")
    else:
        print(structure_result)
    
    return structure_result

def insert_test_data():
    """Insert a test row to verify everything works"""
    print("\nInserting test data...")
    
    # Use ILP (InfluxDB Line Protocol) for insertion
    from questdb.ingress import Sender, TimestampNanos
    
    try:
        with Sender.from_conf(f"http::addr={QUESTDB_HOST}:{QUESTDB_PORT};") as sender:
            # Create test timestamp
            test_ts = TimestampNanos(int(time.time() * 1_000_000_000))
            
            sender.row(
                "orderbook",
                symbols={
                    "exchange": "test",
                    "symbol": "BTC-USD",
                    "base": "BTC",
                    "quote": "USD",
                    "region": "GLOBAL",
                    "venue_type": "SPOT",
                    "instance": "TEST",
                },
                columns={
                    "seq": 1,
                    "best_bid": 106000.0,
                    "best_ask": 106100.0,
                    "mid_price": 106050.0,
                    "spread_bps": 9.43,
                    "recv_ts_s": time.time(),
                    "depth_json": '{"test": true}',
                    "raw_json": '{"test": true}',
                },
                at=test_ts
            )
            sender.flush()
        print("Test data inserted successfully!")
        
        # Query to verify
        time.sleep(1)  # Give QuestDB time to process
        verify_query = "SELECT * FROM orderbook WHERE exchange = 'test' LIMIT 1"
        verify_result = execute_sql(verify_query)
        print(f"Test data verification: {verify_result}")
        
        # Clean up test data
        cleanup_query = "DELETE FROM orderbook WHERE exchange = 'test'"
        cleanup_result = execute_sql(cleanup_query)
        print(f"Test data cleanup: {cleanup_result}")
        
    except Exception as e:
        print(f"Error inserting test data: {e}")

def main():
    print("=" * 60)
    print("QuestDB Setup for Orderbook Streaming")
    print("=" * 60)
    print(f"QuestDB URL: {BASE_URL}")
    print(f"Time: {datetime.now()}")
    print("=" * 60)
    
    # Test connection
    print("\nTesting QuestDB connection...")
    try:
        response = requests.get(f"{BASE_URL}/")
        if response.status_code == 200:
            print("✓ QuestDB is running and accessible")
        else:
            print(f"✗ QuestDB returned status code: {response.status_code}")
            return
    except Exception as e:
        print(f"✗ Cannot connect to QuestDB: {e}")
        print("\nMake sure QuestDB is running on port 9000")
        print("Start it with: docker run -p 9000:9000 -p 9009:9009 questdb/questdb")
        return
    
    # Create table
    create_orderbook_table()
    
    # Verify table
    verify_table()
    
    # Test with sample data
    insert_test_data()
    
    print("\n" + "=" * 60)
    print("Setup complete! You can now run the orderbook streamer.")
    print("=" * 60)

if __name__ == "__main__":
    main()