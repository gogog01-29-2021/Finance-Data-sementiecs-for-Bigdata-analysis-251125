# Database Access & Query Guide

Quick reference for accessing and querying all databases in the Financial Data Pipeline.

---

## 🚀 Quick Access Summary

| Database | Access Method | URL/Command | GUI Available |
|----------|---------------|-------------|---------------|
| **QuestDB** | Web UI | http://localhost:9000 | ✅ YES |
| **Neo4j** | Web UI | http://localhost:7474 | ✅ YES |
| **MongoDB** | CLI | `docker exec -it mongodb mongosh` | ⚠️ Optional (Compass) |
| **Cassandra** | CLI | `docker exec -it cassandra cqlsh` | ❌ NO |
| **LMDB** | Python | `python view_lmdb.py` | ❌ NO |

---

## 1. QuestDB (Time-Series Orderbook Data)

### Web UI Access:
```
http://localhost:9000
```

### Common Queries:
```sql
-- View all data
SELECT * FROM orderbook LIMIT 100;

-- Latest updates
SELECT * FROM orderbook ORDER BY timestamp DESC LIMIT 50;

-- Crypto only
SELECT * FROM orderbook WHERE venue_type = 'SPOT' LIMIT 100;

-- Stocks only
SELECT * FROM orderbook WHERE venue_type = 'STOCK' LIMIT 100;

-- Specific symbol
SELECT * FROM orderbook WHERE symbol = 'BTC-USD' ORDER BY timestamp DESC LIMIT 50;

-- Count by exchange
SELECT exchange, COUNT(*) as count FROM orderbook GROUP BY exchange;

-- Price statistics by symbol
SELECT
    symbol,
    COUNT(*) as records,
    AVG(mid_price) as avg_price,
    MAX(mid_price) as high,
    MIN(mid_price) as low
FROM orderbook
GROUP BY symbol;
```

### CLI Access:
```bash
docker exec -it questdb psql -h localhost -p 8812 -d qdb -U admin
```

---

## 2. Cassandra (Analytics Results)

### CLI Access:
```bash
docker exec -it cassandra cqlsh
```

### Common Queries:
```sql
-- Show keyspaces
DESCRIBE KEYSPACES;

-- Use financial keyspace
USE financial_data;

-- Show tables
DESCRIBE TABLES;

-- View arbitrage opportunities
SELECT * FROM arbitrage_opportunities LIMIT 10;

-- View price predictions
SELECT * FROM price_predictions LIMIT 10;

-- Filter by symbol (needs ALLOW FILTERING)
SELECT * FROM arbitrage_opportunities
WHERE symbol = 'BTC-USD'
LIMIT 50
ALLOW FILTERING;

-- Count records
SELECT COUNT(*) FROM arbitrage_opportunities;
```

### Python Access:
```python
from cassandra.cluster import Cluster

cluster = Cluster(['localhost'])
session = cluster.connect('financial_data')

rows = session.execute('SELECT * FROM arbitrage_opportunities LIMIT 10')
for row in rows:
    print(row)

cluster.shutdown()
```

---

## 3. Neo4j (Graph Relationships)

### Web UI Access:
```
http://localhost:7474

Username: neo4j
Password: password
```

### Common Queries (Cypher):
```cypher
// View all nodes
MATCH (n) RETURN n LIMIT 100;

// View specific node types
MATCH (n:ArbitrageOpportunity) RETURN n LIMIT 50;
MATCH (n:SentimentCorrelation) RETURN n LIMIT 50;

// Count by node type
MATCH (n) RETURN labels(n) as type, COUNT(*) as count;

// High-spread arbitrage
MATCH (n:ArbitrageOpportunity)
WHERE n.spread_bps > 100
RETURN n
ORDER BY n.spread_bps DESC;

// Relationships
MATCH (s:SentimentCorrelation)-[r]->(p)
RETURN s, r, p
LIMIT 25;

// Delete all (cleanup)
MATCH (n) DETACH DELETE n;
```

### CLI Access:
```bash
docker exec -it neo4j cypher-shell -u neo4j -p password
```

### Python Access:
```python
from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687",
                              auth=("neo4j", "password"))

with driver.session() as session:
    result = session.run("MATCH (n:ArbitrageOpportunity) RETURN n LIMIT 10")
    for record in result:
        print(record)

driver.close()
```

---

## 4. MongoDB (Document Analytics)

### CLI Access:
```bash
docker exec -it mongodb mongosh
```

### Common Queries:
```javascript
// Show databases
show dbs

// Use financial database
use financial_analytics

// Show collections
show collections

// View arbitrage opportunities
db.arbitrage_opportunities.find().limit(10).pretty()

// View sentiment correlations
db.sentiment_correlations.find().limit(10).pretty()

// View price predictions
db.price_predictions.find().limit(10).pretty()

// Count documents
db.arbitrage_opportunities.countDocuments()

// Filter by symbol
db.arbitrage_opportunities.find({symbol: "BTC-USD"}).limit(10).pretty()

// High-spread arbitrage
db.arbitrage_opportunities.find({spread_bps: {$gt: 100}})
    .sort({spread_bps: -1})
    .limit(10)
    .pretty()

// Aggregate statistics
db.arbitrage_opportunities.aggregate([
  {$group: {
    _id: "$symbol",
    count: {$sum: 1},
    avg_spread: {$avg: "$spread_bps"}
  }},
  {$sort: {count: -1}}
])
```

### Python Access:
```python
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['financial_analytics']

# Query
for doc in db.arbitrage_opportunities.find().limit(10):
    print(doc)

# Count
count = db.arbitrage_opportunities.count_documents({})
print(f"Total: {count}")

client.close()
```

### MongoDB Compass (GUI):
1. Download: https://www.mongodb.com/try/download/compass
2. Connect: `mongodb://localhost:27017`
3. Browse visually

---

## 5. LMDB (Reddit Sentiment - Local File)

### Python Script Access:

Create `view_lmdb.py`:
```python
import lmdb
import json

DB_PATH = "data/reddit/reddit_sentiment_lmdb.db"

env = lmdb.open(DB_PATH, readonly=True)

with env.begin() as txn:
    cursor = txn.cursor()

    # Count items
    posts = comments = sentiments = 0
    for key, _ in cursor:
        k = key.decode()
        if k.startswith("post:"): posts += 1
        elif k.startswith("comment:"): comments += 1
        elif k.startswith("sentiment:"): sentiments += 1

    print(f"Posts: {posts}")
    print(f"Comments: {comments}")
    print(f"Sentiments: {sentiments}")

    # Show sample data
    cursor.first()
    for key, value in list(cursor)[:10]:
        if key.decode().startswith("post:"):
            data = json.loads(value.decode())
            print(f"\nSubreddit: r/{data['subreddit']}")
            print(f"Title: {data['title'][:60]}...")

env.close()
```

Run: `python view_lmdb.py`

---

## 📊 Monitoring All Databases

### Check if all are running:
```bash
docker-compose ps
```

### View logs:
```bash
# All databases
docker-compose logs -f

# Specific database
docker-compose logs -f questdb
docker-compose logs -f cassandra
docker-compose logs -f neo4j
docker-compose logs -f mongodb
```

### Health checks:
```bash
# QuestDB
curl http://localhost:9000

# Neo4j
curl http://localhost:7474

# MongoDB
docker exec mongodb mongosh --eval "db.adminCommand('ping')"

# Cassandra
docker exec cassandra cqlsh -e "DESCRIBE KEYSPACES"
```

---

## 🔧 Useful Commands

### Restart a database:
```bash
docker-compose restart questdb
docker-compose restart cassandra
docker-compose restart neo4j
docker-compose restart mongodb
```

### Stop all databases:
```bash
docker-compose stop
```

### Start all databases:
```bash
docker-compose start
```

### Remove all data (reset):
```bash
docker-compose down -v
docker-compose up -d
```

---

**Last Updated:** 2025-11-15
**Pipeline Version:** 1.0.0
