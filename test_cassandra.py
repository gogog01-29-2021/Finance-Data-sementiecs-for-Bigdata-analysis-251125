#!/usr/bin/env python3
"""Test Cassandra connection"""

import os
os.environ['EVENTLET_NO_GREENDNS'] = 'yes'

try:
    # Use eventlet for async operations (required for Python 3.12+)
    import eventlet
    eventlet.monkey_patch()

    from cassandra.cluster import Cluster
    from cassandra.io.eventletreactor import EventletConnection
    from cassandra import cluster as cassandra_cluster
    cassandra_cluster.connection_class = EventletConnection

    print("Connecting to Cassandra...")
    cluster = Cluster(['localhost'], port=9042)
    session = cluster.connect()

    print("[OK] Connected successfully!")

    # List keyspaces
    rows = session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
    keyspaces = [row.keyspace_name for row in rows]
    print(f"\nKeyspaces: {', '.join(keyspaces)}")

    # Check financial_data keyspace
    session.execute("USE financial_data")
    print("\n[OK] Using financial_data keyspace")

    # List tables
    rows = session.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name='financial_data'")
    tables = [row.table_name for row in rows]
    print(f"Tables: {', '.join(tables)}")

    # Count records in each table
    for table in tables:
        result = session.execute(f"SELECT COUNT(*) FROM {table}")
        count = result.one()[0]
        print(f"  {table}: {count} records")

    cluster.shutdown()
    print("\n[OK] Cassandra driver is working correctly!")

except Exception as e:
    print(f"[ERROR] {e}")
    import traceback
    traceback.print_exc()
