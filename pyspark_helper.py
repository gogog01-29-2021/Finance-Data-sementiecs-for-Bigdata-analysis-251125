"""
PySpark Interactive Helper
Run this inside pyspark shell or use: python -i pyspark_helper.py
"""

print("=" * 80)
print("PYSPARK FINANCIAL DATA HELPER")
print("=" * 80)

# Helper functions for easy data access
def read_questdb(query="SELECT * FROM orderbook LIMIT 1000"):
    """Read data from QuestDB"""
    print(f"Reading from QuestDB: {query[:50]}...")
    return spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:8812/qdb") \
        .option("dbtable", f"({query}) AS data") \
        .option("user", "admin") \
        .option("password", "quest") \
        .option("driver", "org.postgresql.Driver") \
        .load()

def read_cassandra(table):
    """Read from Cassandra financial_data keyspace"""
    print(f"Reading from Cassandra: {table}")
    return spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(keyspace="financial_data", table=table) \
        .load()

def read_mongodb(collection):
    """Read from MongoDB financial_analytics database"""
    print(f"Reading from MongoDB: {collection}")
    return spark.read \
        .format("mongodb") \
        .option("database", "financial_analytics") \
        .option("collection", collection) \
        .load()

def show_databases():
    """Show status of all databases"""
    print("\n" + "=" * 80)
    print("DATABASE STATUS")
    print("=" * 80)

    # QuestDB
    try:
        count = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:8812/qdb") \
            .option("dbtable", "(SELECT COUNT(*) as cnt FROM orderbook) AS data") \
            .option("user", "admin") \
            .option("password", "quest") \
            .option("driver", "org.postgresql.Driver") \
            .load() \
            .collect()[0]['cnt']
        print(f"[OK] QuestDB: {count:,} orderbook records")
    except Exception as e:
        print(f"[ERROR] QuestDB: {e}")

    # Cassandra
    try:
        arb = read_cassandra("arbitrage_opportunities").count()
        pred = read_cassandra("price_predictions").count()
        print(f"[OK] Cassandra: {arb} arbitrage, {pred} predictions")
    except Exception as e:
        print(f"[ERROR] Cassandra: {e}")

    # MongoDB
    try:
        from pymongo import MongoClient
        client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=2000)
        db = client['financial_analytics']
        colls = db.list_collection_names()
        print(f"[OK] MongoDB: {len(colls)} collections - {colls}")
    except Exception as e:
        print(f"[ERROR] MongoDB: {e}")

    print("=" * 80 + "\n")

print("\nAvailable helper functions:")
print("  - read_questdb(query)      # Read from QuestDB orderbook")
print("  - read_cassandra(table)    # Read from Cassandra")
print("  - read_mongodb(collection) # Read from MongoDB")
print("  - show_databases()         # Show all database status")
print("\nQuick examples:")
print("  df = read_questdb('SELECT * FROM orderbook LIMIT 100')")
print("  df.show()")
print("  df.filter(\"venue_type = 'SPOT'\").groupBy('exchange').count().show()")
print("=" * 80)
