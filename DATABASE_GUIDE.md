# Database Viewing Guide

Complete guide to view all databases in the Financial Data Pipeline.

---

## 📊 1. QuestDB (Crypto + Stock Orderbook)

### How to Run QuestDB

```bash
# Start QuestDB via Docker
docker run -d --name questdb -p 9000:9000 -p 8812:8812 questdb/questdb

# Or download and run locally from https://questdb.io/download/
```

### How to View QuestDB

**Web UI (Easiest):**
```
http://localhost:9000
```

### Useful Queries

#### View Entire Table
```sql
SELECT * FROM orderbook;
```

#### View Table Schema
```sql
SELECT * FROM orderbook LIMIT 1;
```

#### Count Total Records
```sql
SELECT COUNT(*) as total_records FROM orderbook;
```

#### Count by Type (Crypto vs Stocks)
```sql
SELECT venue_type, COUNT(*) as count
FROM orderbook
GROUP BY venue_type;
```

#### Count by Exchange
```sql
SELECT exchange, venue_type, COUNT(*) as count
FROM orderbook
GROUP BY exchange, venue_type
ORDER BY count DESC;
```

#### Latest 100 Records
```sql
SELECT * FROM orderbook
ORDER BY timestamp DESC
LIMIT 100;
```

#### Crypto Data Only
```sql
SELECT * FROM orderbook
WHERE venue_type = 'SPOT'
ORDER BY timestamp DESC;
```

#### Stock Data Only
```sql
SELECT * FROM orderbook
WHERE venue_type = 'STOCK'
ORDER BY timestamp DESC;
```

#### Specific Symbol (e.g., Bitcoin)
```sql
SELECT * FROM orderbook
WHERE symbol = 'BTC-USD'
ORDER BY timestamp DESC
LIMIT 50;
```

#### Price Summary by Symbol
```sql
SELECT
    symbol,
    venue_type,
    COUNT(*) as records,
    AVG(mid_price) as avg_price,
    MAX(mid_price) as high_price,
    MIN(mid_price) as low_price,
    AVG(spread_bps) as avg_spread
FROM orderbook
GROUP BY symbol, venue_type
ORDER BY records DESC;
```

#### Recent Data (Last Hour)
```sql
SELECT * FROM orderbook
WHERE timestamp > dateadd('h', -1, now())
ORDER BY timestamp DESC;
```

#### Find Arbitrage Opportunities
```sql
SELECT
    t1.symbol,
    t1.exchange as exchange1,
    t2.exchange as exchange2,
    t1.mid_price as price1,
    t2.mid_price as price2,
    ((t2.mid_price - t1.mid_price) / t1.mid_price * 10000) as spread_bps
FROM orderbook t1
JOIN orderbook t2 ON t1.symbol = t2.symbol AND t1.timestamp = t2.timestamp
WHERE t1.exchange < t2.exchange
  AND t1.venue_type = 'SPOT'
  AND ABS((t2.mid_price - t1.mid_price) / t1.mid_price * 10000) > 50
LIMIT 100;
```

---

## 🗄️ 2. LMDB (Reddit Sentiment Data)

### How to Run LMDB

LMDB is a file-based database - no server needed! It's created automatically when you run `reddit_sentiment.py`.

**Location:** `data/reddit/reddit_sentiment_lmdb.db`

### How to View LMDB

#### Method 1: Python Script

Create `view_lmdb.py`:
```python
import lmdb
import json
from pathlib import Path

DB_PATH = "data/reddit/reddit_sentiment_lmdb.db"

def view_all(limit=20):
    env = lmdb.open(DB_PATH, readonly=True)

    with env.begin() as txn:
        cursor = txn.cursor()

        print("=" * 80)
        print("LMDB DATABASE VIEWER")
        print("=" * 80)

        # Count items
        posts = comments = sentiments = 0
        for key, _ in cursor:
            k = key.decode()
            if k.startswith("post:"): posts += 1
            elif k.startswith("comment:"): comments += 1
            elif k.startswith("sentiment:"): sentiments += 1

        print(f"\nStatistics:")
        print(f"  Posts: {posts}")
        print(f"  Comments: {comments}")
        print(f"  Sentiments: {sentiments}")

        # Show recent posts
        print(f"\nRecent Posts (limit={limit}):")
        print("-" * 80)

        cursor.first()
        count = 0
        for key, value in cursor:
            if key.decode().startswith("post:") and count < limit:
                data = json.loads(value.decode())
                print(f"\n  Subreddit: r/{data['subreddit']}")
                print(f"  Title: {data['title'][:60]}...")
                print(f"  Score: {data['score']}")
                count += 1

    env.close()

if __name__ == "__main__":
    view_all()
```

Run it:
```bash
python view_lmdb.py
```

#### Method 2: One-liner
```bash
python -c "import lmdb, json; env=lmdb.open('data/reddit/reddit_sentiment_lmdb.db', readonly=True); [print(f'{k.decode()}: {json.loads(v.decode())}') for k,v in list(env.begin().cursor())[:10]]; env.close()"
```

#### Method 3: Search Specific Subreddit
```python
import lmdb
import json

env = lmdb.open("data/reddit/reddit_sentiment_lmdb.db", readonly=True)

with env.begin() as txn:
    for key, value in txn.cursor():
        data = json.loads(value.decode())
        if data.get('subreddit') == 'Bitcoin':  # Change subreddit here
            print(data)

env.close()
```

---

## 📦 3. Cassandra (Analytics Time-Series Data)

### How to Run Cassandra

```bash
# Start Cassandra via Docker
docker run -d --name cassandra -p 9042:9042 cassandra:latest

# Wait for Cassandra to start (takes ~30 seconds)
docker logs cassandra -f
```

### How to View Cassandra

#### Access CQL Shell
```bash
# Enter Cassandra container
docker exec -it cassandra cqlsh
```

#### Useful Queries

```sql
-- Show all keyspaces
DESCRIBE keyspaces;

-- Use the financial keyspace
USE financial_data;

-- Show all tables
DESCRIBE tables;

-- View arbitrage opportunities
SELECT * FROM arbitrage_opportunities LIMIT 100;

-- View price predictions
SELECT * FROM price_predictions LIMIT 100;

-- Count records
SELECT COUNT(*) FROM arbitrage_opportunities;

-- Filter by symbol
SELECT * FROM arbitrage_opportunities
WHERE symbol = 'BTC-USD'
LIMIT 50;

-- Latest predictions
SELECT * FROM price_predictions
ORDER BY timestamp DESC
LIMIT 50;
```

#### Alternative: Python Script
```python
from cassandra.cluster import Cluster

cluster = Cluster(['localhost'])
session = cluster.connect('financial_data')

# Query data
rows = session.execute('SELECT * FROM arbitrage_opportunities LIMIT 10')
for row in rows:
    print(row)

cluster.shutdown()
```

---

## 🕸️ 4. Neo4j (Graph Relationships)

### How to Run Neo4j

```bash
# Start Neo4j via Docker
docker run -d --name neo4j \
    -p 7474:7474 -p 7687:7687 \
    -e NEO4J_AUTH=neo4j/password \
    neo4j:latest
```

### How to View Neo4j

#### Web UI (Easiest)
```
http://localhost:7474
```

Login:
- Username: `neo4j`
- Password: `password`

#### Useful Queries (Cypher)

```cypher
// View all nodes
MATCH (n) RETURN n LIMIT 100;

// View arbitrage opportunities
MATCH (n:ArbitrageOpportunity) RETURN n LIMIT 50;

// View sentiment correlations
MATCH (n:SentimentCorrelation) RETURN n LIMIT 50;

// Count nodes by type
MATCH (n) RETURN labels(n) as type, COUNT(*) as count;

// Find high-spread arbitrage
MATCH (n:ArbitrageOpportunity)
WHERE n.spread_bps > 100
RETURN n
ORDER BY n.spread_bps DESC;

// Relationships between sentiment and price
MATCH (s:SentimentCorrelation)-[r]->(p)
RETURN s, r, p
LIMIT 25;

// Delete all data (if needed)
MATCH (n) DETACH DELETE n;
```

#### Alternative: Python Script
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

## 🍃 5. MongoDB (Document Storage)

### How to Run MongoDB

```bash
# Start MongoDB via Docker
docker run -d --name mongodb -p 27017:27017 mongo:latest
```

### How to View MongoDB

#### Access Mongo Shell
```bash
# Enter MongoDB container
docker exec -it mongodb mongosh
```

#### Useful Queries

```javascript
// Show all databases
show dbs

// Use financial database
use financial_analytics

// Show all collections
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

// Find high-spread arbitrage
db.arbitrage_opportunities.find({spread_bps: {$gt: 100}}).sort({spread_bps: -1}).limit(10).pretty()

// Aggregate by symbol
db.arbitrage_opportunities.aggregate([
  {$group: {_id: "$symbol", count: {$sum: 1}, avg_spread: {$avg: "$spread_bps"}}},
  {$sort: {count: -1}}
])

// Delete all data (if needed)
db.arbitrage_opportunities.deleteMany({})
```

#### Alternative: Python Script
```python
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['financial_analytics']

# View arbitrage opportunities
for doc in db.arbitrage_opportunities.find().limit(10):
    print(doc)

# Count documents
count = db.arbitrage_opportunities.count_documents({})
print(f"Total: {count}")

client.close()
```

#### Alternative: Mongo Express (Web UI)
```bash
# Run Mongo Express for web-based viewing
docker run -d --name mongo-express \
    --link mongodb:mongo \
    -p 8081:8081 \
    -e ME_CONFIG_MONGODB_SERVER=mongo \
    mongo-express

# Access: http://localhost:8081
```

---

## 🔬 6. What Does spark_analytics.py Actually Do?

### Overview
`spark_analytics.py` reads data from QuestDB and LMDB, performs complex analytics using PySpark, then writes results to Cassandra, Neo4j, and MongoDB.

### Analytics Performed

#### 1. **Arbitrage Detection** (`detect_arbitrage()`)
**What it does:**
- Finds price differences for the same crypto across different exchanges
- Calculates potential profit from buying on one exchange and selling on another

**Example:**
```
BTC-USD on Binance: $45,000
BTC-USD on Upbit: $45,500
→ Arbitrage opportunity: 1.11% profit (111 bps)
```

**Output:**
- Spread in basis points (bps)
- Buy/sell exchange recommendations
- Potential profit percentage

#### 2. **Sentiment Correlation** (`correlate_sentiment_price()`)
**What it does:**
- Correlates Reddit sentiment with crypto price movements
- Groups data by 1-hour windows
- Calculates: average sentiment, price volatility, correlation

**Example:**
```
r/Bitcoin average sentiment: +0.65 (positive)
BTC price volatility: 2.3%
→ Strong positive sentiment may indicate upcoming price rise
```

**Output:**
- Sentiment score per subreddit
- Price volatility in same time window
- Correlation strength

#### 3. **Price Prediction** (`predict_price_movement()`)
**What it does:**
- Simple trend-based predictions using 10-period moving average
- Detects if price is trending UP, DOWN, or NEUTRAL
- Generates bullish/bearish/neutral predictions

**Example:**
```
BTC current price: $45,300
BTC 10-period MA: $45,100
Trend: UP
Spread: tightening
→ Prediction: BULLISH (confidence: 44%)
```

**Output:**
- Trend direction
- Prediction (BULLISH/BEARISH/NEUTRAL)
- Confidence score

### Data Flow

```
Input:
  QuestDB → Last 24 hours of orderbook data
  LMDB → Reddit sentiment scores

Processing:
  PySpark → Arbitrage detection
  PySpark → Sentiment correlation
  PySpark → Price predictions

Output:
  Cassandra → arbitrage_opportunities table
  Cassandra → price_predictions table
  Neo4j → ArbitrageOpportunity nodes
  Neo4j → SentimentCorrelation relationships
  MongoDB → All analytics as JSON documents
```

### When Does It Run?

- **Automatically:** Every 5 minutes when you run `python main.py`
- **Manually:** Run once with `python main.py --component analytics`
- **Directly:** `python spark_analytics.py`

---

## 🚀 Quick Start Commands

```bash
# 1. Start all databases
docker run -d --name questdb -p 9000:9000 -p 8812:8812 questdb/questdb
docker run -d --name cassandra -p 9042:9042 cassandra:latest
docker run -d --name neo4j -p 7474:7474 -p 7687:7687 -e NEO4J_AUTH=neo4j/password neo4j:latest
docker run -d --name mongodb -p 27017:27017 mongo:latest

# 2. Run the pipeline
python main.py

# 3. View data
# QuestDB: http://localhost:9000
# Neo4j: http://localhost:7474
# MongoDB: docker exec -it mongodb mongosh
# Cassandra: docker exec -it cassandra cqlsh
# LMDB: python view_lmdb.py
```

---

## 📝 Summary Table

| Database | Purpose | View Method | Web UI | Port |
|----------|---------|-------------|--------|------|
| **QuestDB** | Crypto/Stock orderbook | http://localhost:9000 | ✅ YES | 9000 |
| **LMDB** | Reddit sentiment | Python script | ❌ No | - |
| **Cassandra** | Analytics time-series | cqlsh | ❌ No | 9042 |
| **Neo4j** | Graph relationships | http://localhost:7474 | ✅ YES | 7474 |
| **MongoDB** | Analytics documents | mongosh or Express | ✅ YES (Express) | 27017 |

---

**Last Updated:** 2025-11-15
**Version:** 1.0.0
