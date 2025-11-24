# Financial Data Pipeline - Advanced Cross-Analysis System

A comprehensive 3-layer financial analytics pipeline with advanced Reddit-Price cross-analysis, combining cryptocurrency market data and Reddit sentiment to detect causal patterns and time-lagged correlations.

## 🎯 What's New: Advanced Cross-Analysis (G1-G4)

This system now features **sophisticated time-lagged cross-analysis** between Reddit discussions and price movements:

| Analytics | Description | Example Pattern |
|-----------|-------------|-----------------|
| **G1: Words After Price** | Which words spike AFTER price moves? | BTC pumps 5% → "moon", "lambo" spike |
| **G2: Price After Words** | Do prices move AFTER word spikes? | "crash" +300% → BTC drops 3% |
| **G3: Bidirectional Causality** | Is the relationship one-way or bidirectional? | Does "pump" predict price OR vice versa? |
| **G4: Pattern Velocity** | How FAST do reactions occur? | "moon" → price reacts in <15 minutes |

## Architecture Overview

```
┌────────────────────────────────────────────────────────────────┐
│                    LAYER 1: DATA COLLECTION                     │
├────────────────────────────────────────────────────────────────┤
│  • QuestDB (Time-series): Crypto orderbook data (1.7M+ records)│
│  • LMDB: Reddit sentiment with full text (1.6K+ posts)         │
└────────────────────────────────────────────────────────────────┘
                              ↓
┌────────────────────────────────────────────────────────────────┐
│               LAYER 2: PYSPARK ANALYTICS ENGINE                 │
├────────────────────────────────────────────────────────────────┤
│  A) Core Analytics: Arbitrage, Correlation, Prediction        │
│  B) Volume & Liquidity: Order depth, VWAP, Bid-ask imbalance  │
│  C) Volatility: Rolling volatility, Bollinger Bands, Flash     │
│  D) Enhanced Sentiment: Momentum, Divergence                   │
│  E) Cross-Asset: Correlation matrix                            │
│  F) Text Content: Word frequency, Word-price, Word-sentiment   │
│  G) ADVANCED CROSS-ANALYSIS: Reddit ↔ Price Patterns          │
│     ├─ G1: Words appearing after price moves (lagging)         │
│     ├─ G2: Price movements after word spikes (leading)         │
│     ├─ G3: Bidirectional causality detection                   │
│     └─ G4: Pattern velocity (reaction speed)                   │
└────────────────────────────────────────────────────────────────┘
                              ↓
┌────────────────────────────────────────────────────────────────┐
│                   LAYER 3: ANALYTICS STORAGE                    │
├────────────────────────────────────────────────────────────────┤
│  • MongoDB: Unified collection (key-value documents)           │
│  • Neo4j: Bidirectional relationship graphs                    │
│  • Cassandra: Time-series analytics (optional)                 │
└────────────────────────────────────────────────────────────────┘
```

## 🚀 Quick Start (Docker - Recommended)

### Prerequisites
- Docker Desktop installed and running
- 8GB+ RAM available
- Git

### One-Command Setup

**Windows:**
```cmd
git clone <your-repo-url>
cd Financewebsocket251110
start_pipeline.bat
```

**Linux/Mac:**
```bash
git clone <your-repo-url>
cd Financewebsocket251110
chmod +x start_pipeline.sh
./start_pipeline.sh
```

**That's it!** The pipeline automatically:
1. ✅ Starts all 4 databases (QuestDB, Cassandra, MongoDB, Neo4j)
2. ✅ Waits for health checks (60 seconds)
3. ✅ Creates unified schemas
4. ✅ Runs PySpark analytics every 5 minutes
5. ✅ Processes **19 analytics types** including G1-G4 cross-analysis

### Access Layer 3 Databases

| Database | Web UI | Credentials | Purpose |
|----------|--------|-------------|---------|
| **QuestDB** | http://localhost:9000 | - | View raw orderbook data |
| **MongoDB** | http://localhost:8081 | admin/admin | View analytics results |
| **Neo4j** | http://localhost:7474 | neo4j/password | View relationship graphs |
| **Cassandra Web** | http://localhost:3000 | - | View Cassandra tables |

### View Results

**MongoDB (Primary Layer 3 Storage):**
```javascript
// Access Mongo Express: http://localhost:8081 (admin/admin)
// Or use mongosh:
docker exec -it mongodb mongosh financial_analytics

// View analytics types
db.analytics_unified.distinct("analytics_type")

// View G1: Words after price moves
db.analytics_unified.find({analytics_type: "words_after_price_move"}).limit(5)

// View G2: Price after word spikes
db.analytics_unified.find({analytics_type: "price_after_word_spike"}).limit(5)

// View G3: Bidirectional causality
db.analytics_unified.find({analytics_type: "bidirectional_causality"}).limit(5)

// View G4: Pattern velocity
db.analytics_unified.find({analytics_type: "pattern_velocity"}).limit(5)
```

**Neo4j (Graph Relationships):**
```cypher
// Access Neo4j Browser: http://localhost:7474 (neo4j/password)

// View all node types
MATCH (n) RETURN distinct labels(n)

// View bidirectional sentiment-price relationships
MATCH (p:RedditPost)-[r:LEADS_TO]-(m:PriceMovement)
RETURN p, r, m LIMIT 25

// View arbitrage opportunities
MATCH (e1:Exchange)-[r:ARBITRAGE]-(e2:Exchange)
RETURN e1, r, e2 LIMIT 25
```

### Stop Services
```bash
docker-compose down

# Remove all data (WARNING: deletes everything)
docker-compose down -v
```

## 📦 Manual Setup (Without Docker)

### 1. Prerequisites

**Required:**
- Python 3.11 (NOT 3.13 - PySpark incompatibility on Windows)
- Java 17+ (for PySpark)
- Databases: QuestDB, MongoDB, Neo4j (Cassandra optional)

**Install Python 3.11:**
```bash
# Using Conda (recommended)
conda create -n spark_py311 python=3.11 -y
conda activate spark_py311

# Or download from python.org
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

**requirements.txt:**
```
pyspark==3.5.0
python-dotenv==1.0.0
lmdb==1.5.1
cassandra-driver==3.29.2
pymongo==4.6.1
neo4j==5.16.0
pandas==2.1.0
numpy==1.24.0
```

### 3. Start Databases

**MongoDB (Required for Layer 3):**
```bash
docker run -d -p 27017:27017 --name mongodb mongo:latest
```

**Neo4j (Required for graphs):**
```bash
docker run -d -p 7474:7474 -p 7687:7687 \
  --name neo4j \
  -e NEO4J_AUTH=neo4j/password \
  neo4j:latest
```

**QuestDB (Required for Layer 1):**
```bash
docker run -d -p 9000:9000 -p 8812:8812 \
  --name questdb \
  questdb/questdb
```

### 4. Run Analytics Pipeline

```bash
# Activate Python 3.11 environment
conda activate spark_py311

# Run pipeline
python spark_analytics.py
```

**Expected Output:**
```
================================================================================
ENHANCED PYSPARK ANALYTICS PIPELINE STARTED
================================================================================
[spark] Initializing Spark session...
[spark] Spark session initialized

[1] Reading data sources...
[spark] Loaded 1761117 orderbook records
[spark] Loaded 1617 sentiment records

================================================================================
A) ORIGINAL ANALYTICS (IMPROVED)
================================================================================
[spark] Found 3 arbitrage opportunities
...

================================================================================
G) ADVANCED CROSS-ANALYSIS (Reddit ↔ Price Patterns)
================================================================================
[G1] Words appearing after price moves (lagging indicators)...
  ✓ Found X word-after-price patterns

[G2] Price movements after word spikes (leading indicators)...
  ✓ Found X word-spike-to-price patterns

[G3] Bidirectional causality detection...
  ✓ Analyzed causality for X word-symbol pairs

[G4] Pattern velocity (reaction speed measurement)...
  ✓ Measured velocity for X word-symbol pairs

================================================================================
WRITING TO DATABASES
================================================================================
[MongoDB] Writing all analytics to UNIFIED collection (key-value pairs)...
  ✓ Successfully wrote to MongoDB

[Neo4j] Creating graph relationships...
  ✓ Graph relationships created successfully

================================================================================
ANALYTICS PIPELINE COMPLETED SUCCESSFULLY
================================================================================
```

## 🔍 Understanding the Analytics

### G1: Words After Price Moves (Lagging Indicators)

**What it detects:** Which words appear on Reddit AFTER significant price movements

**Use case:** Understanding market reaction language and sentiment patterns

**Example Output:**
```json
{
  "analytics_type": "words_after_price_move",
  "symbol": "BTC-USDT",
  "move_type": "PUMP",
  "price_change_pct": 5.2,
  "price_move_time": "2025-11-21T10:00:00",
  "time_lag_bucket": "0-1hr",
  "word": "moon",
  "word_frequency": 47,
  "avg_sentiment": 0.82,
  "word_rank": 1
}
```

**Interpretation:** After BTC pumped 5.2% at 10:00, the word "moon" appeared 47 times in the next hour with very positive sentiment (0.82).

### G2: Price After Word Spikes (Leading Indicators)

**What it detects:** Price movements FOLLOWING word frequency spikes

**Use case:** Predictive indicators - can certain word spikes predict price moves?

**Example Output:**
```json
{
  "analytics_type": "price_after_word_spike",
  "word": "crash",
  "spike_type": "BEARISH_SPIKE",
  "word_spike_time": "2025-11-21T09:00:00",
  "word_frequency_change_pct": 350.0,
  "symbol": "BTC-USDT",
  "time_lag_bucket": "1-4hr",
  "avg_price_change_pct": -2.8,
  "prediction_accuracy": "CORRECT",
  "sample_count": 15
}
```

**Interpretation:** When "crash" mentions increased 350%, BTC dropped 2.8% in the next 1-4 hours (prediction: CORRECT).

### G3: Bidirectional Causality Detection

**What it detects:** Whether relationships are one-way, bidirectional, or non-existent

**Use case:** Understanding causality direction - does sentiment drive price or vice versa?

**Example Output:**
```json
{
  "analytics_type": "bidirectional_causality",
  "word": "pump",
  "symbol": "ETH-USDT",
  "causality_type": "BIDIRECTIONAL",
  "both_move": 15,
  "word_only": 3,
  "price_only": 4,
  "total_observations": 50
}
```

**Interpretation:** For "pump" and ETH, both price and word frequency move together (bidirectional relationship).

**Causality Types:**
- `BIDIRECTIONAL`: Word ↔ Price (mutual influence)
- `WORD_LEADS_PRICE`: Word → Price (word predicts price)
- `PRICE_LEADS_WORD`: Price → Word (price triggers discussion)
- `NO_CLEAR_RELATIONSHIP`: No significant pattern

### G4: Pattern Velocity (Reaction Speed)

**What it detects:** How QUICKLY markets react to words (or vice versa)

**Use case:** Identifying fast-moving patterns for trading signals

**Example Output:**
```json
{
  "analytics_type": "pattern_velocity",
  "word": "moon",
  "symbol": "DOGE-USDT",
  "avg_reaction_time_seconds": 720,
  "avg_reaction_time_minutes": 12,
  "reaction_speed": "VERY_FAST",
  "occurrence_count": 8
}
```

**Interpretation:** When "moon" is mentioned, DOGE price reacts in an average of 12 minutes (VERY_FAST).

**Speed Categories:**
- `VERY_FAST`: <15 minutes
- `FAST`: 15-60 minutes
- `MODERATE`: 1-3 hours
- `SLOW`: >3 hours

## 📊 Query Layer 3 Databases

### MongoDB Queries

**List all analytics types:**
```javascript
db.analytics_unified.distinct("analytics_type")
```

**Word frequency analysis:**
```javascript
db.analytics_unified.find({
  analytics_type: "word_frequency",
  word: "moon"
}).sort({word_frequency: -1}).limit(10)
```

**Top words after price pumps:**
```javascript
db.analytics_unified.find({
  analytics_type: "words_after_price_move",
  move_type: "PUMP",
  time_lag_bucket: "0-1hr"
}).sort({word_frequency: -1}).limit(20)
```

**Predictive word spikes:**
```javascript
db.analytics_unified.find({
  analytics_type: "price_after_word_spike",
  prediction_accuracy: "CORRECT"
}).sort({avg_price_change_pct: -1}).limit(10)
```

**Bidirectional patterns:**
```javascript
db.analytics_unified.find({
  analytics_type: "bidirectional_causality",
  causality_type: "BIDIRECTIONAL"
})
```

**Fastest reactions:**
```javascript
db.analytics_unified.find({
  analytics_type: "pattern_velocity",
  reaction_speed: "VERY_FAST"
}).sort({avg_reaction_time_minutes: 1})
```

### Neo4j Cypher Queries

**View all relationship types:**
```cypher
MATCH ()-[r]->() RETURN DISTINCT type(r)
```

**Bidirectional sentiment-price relationships:**
```cypher
// Sentiment leads price
MATCH (p:RedditPost)-[r:LEADS_TO {direction: 'sentiment_first'}]->(m:PriceMovement)
RETURN p, r, m LIMIT 25

// Price leads sentiment
MATCH (m:PriceMovement)-[r:LEADS_TO {direction: 'price_first'}]->(p:RedditPost)
RETURN m, r, p LIMIT 25
```

**Arbitrage networks:**
```cypher
MATCH (e1:Exchange)-[r:ARBITRAGE]-(e2:Exchange)
WHERE r.spread_bps > 100
RETURN e1.name, e2.name, r.spread_bps, r.profit_pct
ORDER BY r.spread_bps DESC
LIMIT 10
```

## 🛠️ Configuration

### Environment Variables (.env)

```bash
# Database Connections
QUESTDB_HOST=localhost
QUESTDB_PORT=8812
CASSANDRA_HOST=localhost
CASSANDRA_PORT=9042
MONGODB_URI=mongodb://localhost:27017
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=password

# Analytics Parameters
ARBITRAGE_THRESHOLD_BPS=10
PREDICTION_WINDOW_HOURS=24
```

### Tuning Analytics Thresholds

**Edit `spark_analytics.py` run_pipeline() method:**

```python
# G1: Adjust price move threshold
words_after_price_df = cross_analytics.analyze_words_after_price_moves(
    orderbook_df, sentiment_df,
    price_threshold_pct=2.0  # Change to 1.0 for 1% moves, 5.0 for 5%
)

# G2: Adjust word spike threshold
price_after_words_df = cross_analytics.analyze_price_after_word_spikes(
    sentiment_df, orderbook_df,
    frequency_increase_pct=200.0  # Change to 100.0 for 100% spikes
)

# G3: Adjust lag hours
causality_df = cross_analytics.detect_bidirectional_causality(
    sentiment_df, orderbook_df,
    lag_hours=4  # Change to 1, 2, 6, or 12
)
```

## 📁 Project Structure

```
Financewebsocket251110/
├── Layer 2 (Analytics Engine)
│   ├── spark_analytics.py              # Main PySpark pipeline (19 analytics)
│   ├── advanced_cross_analytics.py     # G1-G4 cross-analysis module
│   └── neo4j_graph_writer.py           # Bidirectional graph writer
│
├── Layer 3 (Storage)
│   ├── create_unified_cassandra_schema.cql  # Unified table schema
│   └── create_cassandra_table.py            # Schema creation script
│
├── Docker Setup
│   ├── docker-compose.yml              # All services orchestration
│   ├── Dockerfile.spark                # Python 3.11 + PySpark image
│   ├── start_pipeline.bat              # Windows startup
│   └── start_pipeline.sh               # Linux/Mac startup
│
├── Configuration
│   ├── .env                            # Environment variables
│   ├── .env.example                    # Template
│   └── requirements.txt                # Python dependencies
│
└── Documentation
    └── README.md                       # This file
```

## 🔧 Troubleshooting

### Python 3.13 + PySpark Issue (Windows)

**Error:** `OSError: [WinError 10038] An operation was attempted on something that is not a socket`

**Solution:**
```bash
# Use Python 3.11 instead
conda create -n spark_py311 python=3.11 -y
conda activate spark_py311
pip install -r requirements.txt
python spark_analytics.py
```

**Or use Docker** (recommended - bypasses Windows issues entirely).

### MongoDB Connection Failed

**Error:** `pymongo.errors.ServerSelectionTimeoutError`

**Solution:**
```bash
# Check MongoDB is running
docker ps | grep mongodb

# Start if not running
docker start mongodb

# Or start fresh
docker run -d -p 27017:27017 --name mongodb mongo:latest
```

### G1-G4 Analytics Show 0 Results

**Cause:** Not enough data or thresholds too high

**Solution:**
1. Check data exists:
   ```bash
   # QuestDB
   curl "http://localhost:9000/exec?query=SELECT count(*) FROM orderbook"

   # MongoDB
   docker exec mongodb mongosh --eval "db.getSiblingDB('financial_analytics').sentiment_data.count()"
   ```

2. Lower thresholds in `spark_analytics.py`:
   ```python
   price_threshold_pct=1.0      # Lower from 2.0
   frequency_increase_pct=100.0  # Lower from 200.0
   ```

### Spark OutOfMemoryError

**Solution:**
```python
# Increase memory in spark_analytics.py
.config("spark.driver.memory", "4g") \
.config("spark.executor.memory", "4g")
```

## 🎓 Advanced Usage

### Run Specific Analytics Only

**Edit `spark_analytics.py` and comment out sections:**

```python
# Skip sections A-E, only run F and G
# Comment out lines for A, B, C, D, E analytics
# Keep only:

# F) TEXT CONTENT ANALYTICS
word_freq_df = self.analyze_word_frequency(sentiment_df)
...

# G) ADVANCED CROSS-ANALYSIS
words_after_price_df = cross_analytics.analyze_words_after_price_moves(...)
...
```

### Custom Word Lists for G4 Velocity

**Edit `advanced_cross_analytics.py` line 375:**

```python
# Add your custom words
significant_words = words_df.filter(
    col("word").isin(
        "crash", "moon", "dump", "pump", "bull", "bear",
        # Add more:
        "hodl", "lambo", "rocket", "dip", "rekt"
    )
)
```

### Export Analytics to CSV

```python
# Add to spark_analytics.py after writing to MongoDB:
words_after_price_df.toPandas().to_csv("words_after_price.csv")
causality_df.toPandas().to_csv("causality.csv")
velocity_df.toPandas().to_csv("velocity.csv")
```

## 📈 Performance Benchmarks

**Test Environment:**
- CPU: Intel i7 (8 cores)
- RAM: 16GB
- Data: 1.7M orderbook + 1.6K sentiment records

**Processing Times:**
- A-E Analytics: ~45 seconds
- F) Text Analytics: ~90 seconds (word tokenization)
- G1-G4 Cross-Analysis: ~180 seconds (time-lagged joins)
- **Total Pipeline:** ~5 minutes

**Memory Usage:**
- Spark Driver: ~2GB
- Spark Executor: ~2GB
- MongoDB: ~500MB
- Neo4j: ~1GB

## 🚀 Future Enhancements

1. **Real-time Streaming:** Replace batch processing with Spark Structured Streaming
2. **ML Models:** Add LSTM/Transformer models for price prediction
3. **Alerting:** Kafka integration for real-time pattern alerts
4. **Dashboard:** Grafana/Streamlit visualization
5. **More Exchanges:** Add Kraken, Coinbase, FTX data
6. **Twitter Integration:** Add Twitter sentiment alongside Reddit

## 📝 License

MIT License

## 🙋 Support

**Issues?**
1. Check logs: `docker-compose logs -f spark-analytics`
2. Verify databases: `docker ps`
3. Test connectivity: `docker exec mongodb mongosh --eval "db.version()"`
4. Review config: `.env` file settings

---

**Last Updated:** 2025-11-22
**Version:** 2.0.0 (Advanced Cross-Analysis)
**Python:** 3.11 (Required)
**PySpark:** 3.5.0

---
