# 📊 ENHANCED ANALYTICS PIPELINE - COMPLETE GUIDE

## 🚀 Overview

This enhanced analytics pipeline processes **396,501 orderbook records** and **1,096 sentiment records** using **MapReduce patterns** in PySpark, generating **12 different analytics** across 5 categories (A-E).

## ✅ What's Been Implemented

### **A) Original Analytics (Improved)**
1. **Arbitrage Detection** - Cross-exchange price differences
   - Threshold: **10 bps** (reduced from 50 bps)
   - Detects profitable arbitrage opportunities
   - MapReduce: Group by (symbol, time) → Calculate spreads

2. **Sentiment-Price Correlation** - Reddit sentiment vs crypto prices
   - Window: **1 hour** aggregation
   - Joins sentiment and price movements
   - MapReduce: Aggregate sentiment + price → Join on time

3. **Price Prediction** - Moving average trend detection
   - Window: **24 hours** (improved from 1 hour)
   - Uses MA-10 for trend detection
   - MapReduce: Group by (symbol, exchange) → Calculate MA

### **B) Volume & Liquidity Analytics**
4. **Order Depth Analysis** - Market liquidity metrics
   - Uses spread as liquidity proxy (tighter = deeper)
   - Calculates liquidity score: `1000 / avg_spread`
   - Classifies: VERY_DEEP, DEEP, MODERATE, SHALLOW

5. **VWAP** - Volume-Weighted Average Price
   - Uses inverse spread as weight proxy
   - Compares VWAP vs simple average
   - Signals: BULLISH_PRESSURE, BEARISH_PRESSURE, NEUTRAL

6. **Bid-Ask Imbalance** - Buy vs sell pressure
   - Calculates imbalance ratio: `(bid_strength - ask_strength) / total`
   - Classifies: STRONG_BUY, MODERATE_BUY, BALANCED, MODERATE_SELL, STRONG_SELL

### **C) Volatility & Pattern Detection**
7. **Rolling Volatility** - Multi-timeframe volatility
   - Windows: **1h, 4h, 24h**
   - Detects volatility spikes (1h > 2x 24h)
   - Trend: INCREASING, DECREASING, STABLE

8. **Bollinger Bands** - Overbought/oversold detection
   - 20-period MA ± 2 standard deviations
   - Band position: 0 (lower) to 1 (upper)
   - Signals: OVERBOUGHT (>0.8), OVERSOLD (<0.2), NEUTRAL

9. **Flash Event Detection** - Rapid price movements
   - Detects >5% moves in <1 minute
   - Types: FLASH_CRASH, FLASH_PUMP, FLASH_VOLATILITY
   - Checks recovery within minute

### **D) Enhanced Sentiment Analytics**
10. **Sentiment Momentum** - Rate of change in sentiment
    - Hour-over-hour sentiment change
    - Types: BULLISH_ACCELERATION, BULLISH_REVERSAL, BEARISH_ACCELERATION, BEARISH_REVERSAL
    - Tracks post count changes

11. **Sentiment-Price Divergence** - When sentiment ≠ price
    - Detects opposite directions (positive sentiment + price drop)
    - Types: BULLISH_SENTIMENT_BEARISH_PRICE, BEARISH_SENTIMENT_BULLISH_PRICE
    - Strength metric: `|sentiment| * |price_change|`

### **E) Cross-Asset Analytics**
12. **Correlation Matrix** - Asset-to-asset correlations
    - Pairwise correlations (BTC-ETH, BTC-SOL, etc.)
    - Strength: VERY_HIGH (>0.8), HIGH (>0.6), MODERATE (>0.4), LOW (>0.2)
    - Market regime classification

---

## 🎯 MapReduce Pattern Summary

| Analytics | MAP Key | REDUCE Operation | Output |
|-----------|---------|------------------|--------|
| A1: Arbitrage | (symbol, time) | Cross-join exchanges → calc spread | Arbitrage opportunities |
| A2: Sentiment Correlation | (time_window) | Aggregate sentiment + price → Join | Correlation metrics |
| A3: Price Prediction | (symbol, exchange) | Moving average window | Price predictions |
| B1: Order Depth | (symbol, exchange) | Sum by price level | Liquidity metrics |
| B2: VWAP | (symbol, time) | Weighted average | VWAP vs simple avg |
| B3: Bid-Ask Imbalance | (symbol, exchange) | Calculate ratio | Buy/sell pressure |
| C1: Rolling Volatility | (symbol, exchange) | Stddev windows (1h, 4h, 24h) | Multi-timeframe vol |
| C2: Bollinger Bands | (symbol, exchange) | MA + 2*stddev | Overbought/oversold |
| C3: Flash Events | (symbol, minute) | Max-min in window | Flash crash detection |
| D1: Sentiment Momentum | (subreddit, hour) | Current - previous | Momentum & acceleration |
| D2: Divergence | (asset, hour) | Compare directions | Divergence signals |
| E1: Correlation Matrix | (time_window) | Pairwise correlations | Correlation matrix |

---

## 🔧 Setup & Installation

### 1. **Create Cassandra Tables**

```bash
# Option 1: Use Python setup script (recommended)
python setup_enhanced_cassandra.py

# Option 2: Use cqlsh directly
cqlsh -f create_enhanced_cassandra_tables.cql
```

This creates **12 tables** + indexes in the `financial_data` keyspace.

### 2. **Verify Data Sources**

Ensure you have data in:
- **QuestDB**: Orderbook data (`orderbook` table)
- **LMDB**: Sentiment data (`data/reddit/reddit_sentiment_lmdb.db`)

Check data:
```bash
python check_all_dbs.py
```

### 3. **Run Enhanced Analytics**

```bash
# Run the enhanced pipeline
python spark_analytics.py
```

Expected output:
```
================================================================================
ENHANCED PYSPARK ANALYTICS PIPELINE STARTED
================================================================================

[1] Reading data sources...
Loaded 396501 orderbook records
Loaded 1096 sentiment records

================================================================================
A) ORIGINAL ANALYTICS (IMPROVED)
================================================================================

[A1] Arbitrage detection (threshold: 10 bps)...
[spark] Found X arbitrage opportunities

[A2] Sentiment-price correlation...
[spark] Generated X sentiment-price correlation records

[A3] Price prediction (24h window)...
[spark] Generated X price predictions

================================================================================
B) VOLUME & LIQUIDITY ANALYTICS
================================================================================

[B1] Order depth analysis...
[spark] Generated X order depth records

[B2] VWAP calculation...
[spark] Generated X VWAP records

[B3] Bid-ask imbalance...
[spark] Generated X bid-ask imbalance records

================================================================================
C) VOLATILITY & PATTERN DETECTION
================================================================================

[C1] Rolling volatility (1h, 4h, 24h)...
[spark] Generated X volatility records

[C2] Bollinger Bands...
[spark] Generated X Bollinger Bands records

[C3] Flash event detection...
[spark] Detected X flash events

================================================================================
D) ENHANCED SENTIMENT ANALYTICS
================================================================================

[D1] Sentiment momentum...
[spark] Generated X sentiment momentum records

[D2] Sentiment-price divergence...
[spark] Detected X sentiment-price divergences

================================================================================
E) CROSS-ASSET ANALYTICS
================================================================================

[E1] Correlation matrix...
[spark] Generated X correlation pairs

================================================================================
WRITING TO DATABASES
================================================================================

[Cassandra] Writing all analytics...
[Neo4j] Writing all analytics...
[MongoDB] Writing all analytics...

================================================================================
ANALYTICS PIPELINE COMPLETED SUCCESSFULLY
================================================================================
```

---

## 📈 Querying Results

### **Cassandra Queries**

```sql
-- A1: Find top arbitrage opportunities
SELECT * FROM arbitrage_opportunities
WHERE symbol = 'BTC-KRW'
ORDER BY timestamp DESC
LIMIT 10;

-- B1: Check market liquidity
SELECT * FROM order_depth
WHERE symbol = 'BTC-KRW' AND exchange = 'upbit'
ORDER BY period_start DESC
LIMIT 10;

-- C1: Check volatility spikes
SELECT * FROM rolling_volatility
WHERE spike_detected = true
ORDER BY timestamp DESC
LIMIT 10;

-- C2: Find overbought/oversold signals
SELECT * FROM bollinger_bands
WHERE signal IN ('OVERBOUGHT', 'OVERSOLD')
ORDER BY timestamp DESC
LIMIT 10;

-- C3: List flash events
SELECT * FROM flash_events
WHERE event_type IN ('FLASH_CRASH', 'FLASH_PUMP')
ORDER BY minute_bucket DESC
LIMIT 10;

-- D2: Find sentiment divergences
SELECT * FROM sentiment_divergence
WHERE divergence_detected = true
ORDER BY period_start DESC
LIMIT 10;

-- E1: Check asset correlations
SELECT * FROM correlation_matrix
WHERE correlation > 0.8
ORDER BY timestamp DESC
LIMIT 10;
```

### **Neo4j Queries**

```cypher
// A1: Visualize arbitrage network
MATCH (a:ArbitrageOpportunity)
WHERE a.spread_bps > 20
RETURN a
LIMIT 50;

// C2: Find overbought assets
MATCH (b:BollingerBands)
WHERE b.signal = 'OVERBOUGHT'
RETURN b.symbol, b.band_position, b.timestamp
ORDER BY b.timestamp DESC
LIMIT 10;

// D2: Visualize sentiment divergences
MATCH (d:SentimentDivergence)
WHERE d.divergence_detected = true
RETURN d
ORDER BY d.divergence_strength DESC
LIMIT 20;
```

### **MongoDB Queries**

```javascript
// A1: Top arbitrage opportunities
db.arbitrage_opportunities.find({
  spread_bps: { $gt: 20 }
}).sort({ timestamp: -1 }).limit(10);

// B3: Buy pressure signals
db.bid_ask_imbalance.find({
  pressure_type: { $regex: /BUY/ }
}).sort({ period_start: -1 }).limit(10);

// C3: Flash events
db.flash_events.find({
  flash_event_detected: true
}).sort({ minute_bucket: -1 }).limit(10);

// D1: Bullish sentiment momentum
db.sentiment_momentum.find({
  momentum_type: { $regex: /BULLISH/ }
}).sort({ hour_start: -1 }).limit(10);
```

---

## 📊 Database Schema

### **Data Distribution**

```
Cassandra: 12 tables (time-series optimized)
├── arbitrage_opportunities
├── sentiment_correlations
├── price_predictions
├── order_depth
├── vwap
├── bid_ask_imbalance
├── rolling_volatility
├── bollinger_bands
├── flash_events
├── sentiment_momentum
├── sentiment_divergence
└── correlation_matrix

Neo4j: 12 node types (graph relationships)
├── ArbitrageOpportunity
├── SentimentCorrelation
├── PricePrediction
├── OrderDepth
├── VWAP
├── BidAskImbalance
├── RollingVolatility
├── BollingerBands
├── FlashEvent
├── SentimentMomentum
├── SentimentDivergence
└── CorrelationMatrix

MongoDB: 12 collections (document storage)
└── (same as Cassandra tables)
```

---

## 🔄 Running Continuously

For production, run the pipeline periodically:

```bash
# Option 1: Cron job (every hour)
0 * * * * cd /path/to/project && python spark_analytics.py >> analytics.log 2>&1

# Option 2: Systemd timer
# Create a service file and timer

# Option 3: Python scheduler
# Use APScheduler or similar
```

---

## ⚙️ Configuration

Edit `.env` file:

```bash
# Arbitrage threshold (basis points)
ARBITRAGE_THRESHOLD_BPS=10

# Database connections
CASSANDRA_HOST=localhost
CASSANDRA_PORT=9042
NEO4J_URI=bolt://localhost:7687
MONGODB_URI=mongodb://localhost:27017
```

---

## 🎯 Key Improvements from Original

| Feature | Original | Enhanced |
|---------|----------|----------|
| Arbitrage threshold | 50 bps | **10 bps** (5x more sensitive) |
| Price prediction window | 1 hour | **24 hours** (better trends) |
| Analytics count | 3 | **12** (4x more insights) |
| Volatility tracking | None | **3 timeframes** (1h, 4h, 24h) |
| Sentiment analysis | Basic | **Momentum + Divergence** |
| Flash detection | None | **<1 min events** |
| Liquidity metrics | None | **Depth + VWAP + Imbalance** |
| Cross-asset | None | **Correlation matrix** |

---

## 📝 Notes

1. **Data Requirements**: Minimum 24 hours of orderbook data for rolling volatility
2. **Sentiment Data**: Optional but enables D1-D2 analytics
3. **Performance**: Processing ~400K records takes 5-10 minutes
4. **Memory**: Requires 4GB+ for Spark (configured in code)
5. **Flash Events**: May be rare in stable markets

---

## 🐛 Troubleshooting

### No orderbook data
```bash
# Run data collection first
python main.py
```

### Cassandra connection failed
```bash
# Check Cassandra is running
docker ps | grep cassandra

# Restart if needed
docker-compose restart cassandra
```

### Spark out of memory
```python
# Increase memory in spark_analytics.py
.config("spark.driver.memory", "8g")
.config("spark.executor.memory", "8g")
```

### No flash events detected
This is normal in stable markets. Flash events require >5% moves in <1 minute.

---

## 📚 Next Steps

1. ✅ **Run setup**: `python setup_enhanced_cassandra.py`
2. ✅ **Verify data**: `python check_all_dbs.py`
3. ✅ **Run analytics**: `python spark_analytics.py`
4. ✅ **Query results**: Use Cassandra/Neo4j/MongoDB queries above
5. 🔄 **Schedule**: Set up periodic execution

---

## 🎉 Summary

You now have **12 production-ready analytics** processing financial data with proper MapReduce patterns, writing to 3 databases, and providing actionable insights for:

- Arbitrage trading
- Market liquidity assessment
- Volatility monitoring
- Sentiment tracking
- Flash event detection
- Cross-asset correlation

All analytics follow industry-standard patterns and are optimized for time-series queries!
