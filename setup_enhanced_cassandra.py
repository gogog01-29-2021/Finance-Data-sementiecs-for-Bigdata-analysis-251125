#!/usr/bin/env python3
"""
Setup script for enhanced Cassandra analytics tables
Creates all tables needed for the enhanced analytics pipeline
"""

import os
import sys
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from dotenv import load_dotenv

load_dotenv()

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "financial_data")


def create_keyspace_and_tables():
    """Connect to Cassandra and create all tables"""
    print(f"[setup] Connecting to Cassandra at {CASSANDRA_HOST}:{CASSANDRA_PORT}...")

    try:
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect()

        # Create keyspace
        print(f"[setup] Creating keyspace: {CASSANDRA_KEYSPACE}...")
        session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
            WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """)

        session.set_keyspace(CASSANDRA_KEYSPACE)
        print(f"[setup] Using keyspace: {CASSANDRA_KEYSPACE}")

        # Read and execute CQL file
        cql_file = "create_enhanced_cassandra_tables.cql"
        print(f"[setup] Reading CQL file: {cql_file}...")

        with open(cql_file, 'r') as f:
            cql_content = f.read()

        # Split by semicolon and execute each statement
        statements = [stmt.strip() for stmt in cql_content.split(';') if stmt.strip()]

        for i, statement in enumerate(statements):
            # Skip comments and USE statements
            if statement.startswith('--') or statement.startswith('USE'):
                continue

            try:
                print(f"[setup] Executing statement {i+1}/{len(statements)}...")
                session.execute(statement)
            except Exception as e:
                print(f"[WARNING] Failed to execute statement: {e}")
                print(f"Statement: {statement[:100]}...")
                continue

        print("\n" + "=" * 80)
        print("✅ CASSANDRA SETUP COMPLETED SUCCESSFULLY")
        print("=" * 80)
        print(f"\nKeyspace: {CASSANDRA_KEYSPACE}")
        print("\nCreated tables:")
        print("  A) Original Analytics:")
        print("     - arbitrage_opportunities")
        print("     - sentiment_correlations")
        print("     - price_predictions")
        print("\n  B) Volume & Liquidity:")
        print("     - order_depth")
        print("     - vwap")
        print("     - bid_ask_imbalance")
        print("\n  C) Volatility & Patterns:")
        print("     - rolling_volatility")
        print("     - bollinger_bands")
        print("     - flash_events")
        print("\n  D) Enhanced Sentiment:")
        print("     - sentiment_momentum")
        print("     - sentiment_divergence")
        print("\n  E) Cross-Asset:")
        print("     - correlation_matrix")
        print("\n" + "=" * 80)

        # Verify tables
        print("\n[verify] Checking created tables...")
        rows = session.execute(f"SELECT table_name FROM system_schema.tables WHERE keyspace_name='{CASSANDRA_KEYSPACE}'")
        tables = [row.table_name for row in rows]
        print(f"[verify] Found {len(tables)} tables: {', '.join(tables)}")

        cluster.shutdown()
        print("\n[setup] Connection closed")

    except Exception as e:
        print(f"\n[ERROR] Setup failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    print("=" * 80)
    print("ENHANCED CASSANDRA SETUP")
    print("=" * 80)
    create_keyspace_and_tables()
