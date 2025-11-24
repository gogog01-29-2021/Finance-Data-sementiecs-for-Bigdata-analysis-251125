# 🚀 Complete 3-Stage Analytics Pipeline Dashboard

## Overview

This is a **comprehensive real-time dashboard** that visualizes your **complete data pipeline** from raw data collection through Spark processing to enriched analytics stored across multiple databases.

Unlike basic dashboards that only show raw collection data, this dashboard provides **end-to-end visibility** into all three stages of your analytics pipeline.

---

## 🎯 What This Dashboard Shows

### Stage 1: Raw Data Collection
- **LMDB databases**: Reddit sentiment, Twitter sentiment, On-Chain transactions
- **QuestDB**: Real-time orderbook data (crypto + stocks)
- **Metrics**: Collection counts, rates, and freshness

### Stage 2: Processing Pipeline Status
- **Spark Analytics**: Processing job status and health
- **Last run timestamps**: When analytics were last updated
- **Pipeline health**: HEALTHY, STALE, or ERROR status

### Stage 3: Enriched Analytics (THE REAL INSIGHTS!)
Connects to all your processed data across:

#### 📊 **Cassandra Tables** (13+ tables):
1. **arbitrage_opportunities**: Cross-exchange arbitrage spreads
2. **sentiment_correlations**: Sentiment-price correlations with confidence
3. **price_predictions**: Trend predictions (uptrend/downtrend/sideways)
4. **sentiment_divergence**: KEY INSIGHT - When sentiment contradicts price action
5. **word_price_correlation**: Causality analysis (leading/lagging indicators)
6. **flash_events**: Rapid price movements detected
7. **vwap**: Volume-Weighted Average Price signals
8. **bid_ask_imbalance**: Order book pressure analysis
9. **rolling_volatility**: Multi-window volatility metrics
10. **bollinger_bands**: Technical indicator signals
11. **correlation_matrix**: Cross-asset correlations
12. **sentiment_momentum**: Hour-over-hour sentiment changes
13. **word_frequency**: Text analytics and trends

#### 🕸️ **Neo4j Graph Relationships**:
- Semantic relationships between Reddit posts and price movements
- `LEADS_TO`, `CORRELATES_WITH`, `DIVERGES_FROM` relationships
- Asset mention networks
- Influence propagation patterns

#### 📦 **MongoDB Document Analytics**:
- Flexible document storage for complex analytics
- Nested data structures for multi-dimensional analysis
- Quick summary statistics by analytics type

---

## 🎨 Dashboard Features

### 5 Main Sections (Tabs):

#### 1. 🔥 Trading Signals
- **Arbitrage opportunities** with spread visualization
- **Price predictions** with trend signals (buy/sell/hold)
- **VWAP trading signals** for entry/exit points

#### 2. 💭 Sentiment Analytics
- **Sentiment-price correlations** by asset
- **DIVERGENCE ALERTS** (⚠️ KEY INSIGHT):
  - Bearish divergence: Positive sentiment but price dropping (smart money selling)
  - Bullish divergence: Negative sentiment but price rising (accumulation phase)

#### 3. 📝 Text Analytics
- **Word-price causality**: Which words in Reddit/Twitter predict price moves
- **Leading indicators**: Words that appear BEFORE price changes
- **Lagging indicators**: Words that appear AFTER price changes
- **Top 20 correlated words** with strength scores

#### 4. 📈 Technical Indicators
- **Flash events timeline**: Rapid price movements with recovery detection
- **Cross-asset correlation heatmap**: See which assets move together
- **Volatility patterns** and trend analysis

#### 5. 🔗 Graph Insights
- **Neo4j relationship visualization**: Sentiment → Price causality
- **MongoDB analytics summary**: Document-level insights
- **Semantic network analysis**

---

## 🚀 Quick Start

### Prerequisites

Ensure all databases are running:
```bash
# Start all services via Docker
docker-compose up -d

# Verify services
docker ps
```

Expected services:
- QuestDB (ports 9000, 8812)
- Cassandra (port 9042)
- Neo4j (ports 7474, 7687)
- MongoDB (port 27017)

### Installation

1. **Install dependencies**:
```bash
pip install -r dashboard_requirements.txt
```

2. **Configure database connections** (optional):

Edit `live_dashboard.py` if your databases use non-default credentials:
```python
# Lines 30-50
QUESTDB_HOST = "localhost"
QUESTDB_PORT = 8812
CASSANDRA_HOST = "localhost"
NEO4J_URI = "bolt://localhost:7687"
MONGODB_URI = "mongodb://localhost:27017"
```

Or create a `.env` file:
```env
QUESTDB_HOST=localhost
QUESTDB_PORT=8812
CASSANDRA_HOST=localhost
NEO4J_URI=bolt://localhost:7687
MONGODB_URI=mongodb://localhost:27017
```

3. **Run the dashboard**:
```bash
streamlit run live_dashboard.py
```

The dashboard will open at: **http://localhost:8501**

---

## 📊 Dashboard Auto-Refresh

- **Refresh interval**: 5 minutes (configurable)
- **Caching**: All database queries are cached with TTL
- **Real-time updates**: Dashboard continuously polls all databases

To change refresh interval, edit line 43:
```python
REFRESH_INTERVAL = 300  # 5 minutes in seconds
```

---

## 🔍 What Each Stage Shows

### Stage 1: Raw Collection (Bottom of Dashboard)
**Expandable section**: "🔍 Stage 1: Raw Collection Details"

Shows:
- Reddit posts collected (LMDB)
- Twitter tweets collected (LMDB)
- On-chain transactions collected (LMDB)
- QuestDB orderbook ticks

**Purpose**: Verify data collection is working

### Stage 2: Processing Pipeline
**Expandable section**: "⚙️ Stage 2: Processing Pipeline Status"

Shows:
- Spark job status: HEALTHY / STALE / ERROR
- Last processing run timestamp
- Age of data (minutes since last update)
- Status of each Cassandra table

**Purpose**: Ensure Spark analytics are running every 5 minutes

### Stage 3: Enriched Analytics (Main Dashboard)
**Five tabs with detailed visualizations**

Shows:
- All processed insights from Spark analytics
- Cross-database queries and joins
- Advanced correlations and predictions
- Divergence detection (KEY INSIGHT!)

**Purpose**: Actionable trading insights and anomaly detection

---

## 🎯 Key Insights to Watch

### 1. Sentiment-Price Divergences (Tab 2)
**What it means**:
- 🔴 **Bearish Divergence**: Sentiment is positive BUT price is dropping
  - **Interpretation**: Retail euphoria while smart money exits
  - **Action**: Consider taking profits or shorting

- 🟢 **Bullish Divergence**: Sentiment is negative BUT price is rising
  - **Interpretation**: Retail fear while smart money accumulates
  - **Action**: Consider buying the dip

**Example Alert**:
```
🔴 BTC-USD: BEARISH_DIVERGENCE
- Sentiment: +0.75 (very positive)
- Price Movement: -5.2% (dropping)
- Strength: 0.85 (strong signal)
```

### 2. Arbitrage Opportunities (Tab 1)
**What it means**:
- Shows price differences across exchanges
- Profitable if spread > 10 BPS (basis points)

**Example**:
```
BTC-USD: Binance $42,050 → Kraken $42,150
Spread: 23.8 BPS (PROFITABLE!)
```

### 3. Word-Price Causality (Tab 3)
**What it means**:
- Shows which words in Reddit/Twitter predict price moves
- **Leading indicators**: Words appear BEFORE price changes
- **Lagging indicators**: Words appear AFTER price changes

**Example**:
```
"moon" → +0.82 correlation (bullish indicator)
"dump" → -0.76 correlation (bearish indicator)
"regulation" → -0.45 correlation (fear indicator)
```

### 4. Flash Events (Tab 4)
**What it means**:
- Detects rapid price movements (>2% in <5 minutes)
- Shows recovery patterns

**Use case**: Identify liquidation cascades or news-driven spikes

### 5. Cross-Asset Correlations (Tab 4)
**What it means**:
- Heatmap showing which assets move together
- High correlation (>0.7): Assets move in sync
- Negative correlation (<-0.3): Assets move opposite

**Use case**: Portfolio diversification and risk management

---

## 🛠️ Troubleshooting

### "No data" messages:

1. **Check Stage 1**: Are collection scripts running?
```bash
python reddit_sentiment.py &
python twitter_sentiment.py &
python onchain_data.py &
python websocket3.py &
```

2. **Check Stage 2**: Is Spark analytics running?
```bash
python spark_analytics.py
```

3. **Check databases**: Are all services healthy?
```bash
docker-compose ps
```

### Connection errors:

If you see connection errors in the sidebar:
- **QuestDB**: Check port 8812 is accessible
- **Cassandra**: Check port 9042, keyspace `financial_data` exists
- **Neo4j**: Check port 7687, credentials are correct
- **MongoDB**: Check port 27017

### Empty Cassandra tables:

If Stage 3 shows "No data yet":
1. Wait 5-10 minutes for first Spark run
2. Check Spark logs: Look for errors in `spark_analytics.py` output
3. Verify QuestDB has orderbook data (Stage 1 → orderbook_count)

---

## 📈 Performance & Scaling

### Caching Strategy:
- All database queries cached for 5 minutes (TTL=300s)
- Reduces load on databases
- Faster dashboard refresh

### Optimization Tips:
1. **Limit query size**: Adjust `LIMIT` in `load_cassandra_table()` function
2. **Increase refresh interval**: Change `REFRESH_INTERVAL` to 600 (10 min)
3. **Add indexes**: Ensure Cassandra tables have proper indexes on timestamp + symbol

### Resource Usage:
- **Memory**: ~500 MB (with caching)
- **CPU**: Low (mostly network I/O)
- **Network**: Moderate (5 database connections)

---

## 🔄 Data Flow

```
┌────────────────────────────────────────────────────┐
│          STAGE 1: RAW COLLECTION                   │
│  Reddit → LMDB   Twitter → LMDB   OnChain → LMDB │
│  Orderbook → QuestDB (real-time streaming)        │
└────────────────────┬───────────────────────────────┘
                     │
                     ↓ Read every 5 min
┌────────────────────────────────────────────────────┐
│         STAGE 2: SPARK PROCESSING                  │
│  spark_analytics.py runs 13+ analytics modules    │
│  - Arbitrage detection                            │
│  - Sentiment correlation                          │
│  - Word-price causality                           │
│  - Divergence detection                           │
│  - Flash event detection                          │
│  - Cross-asset correlation                        │
└────────────────────┬───────────────────────────────┘
                     │
                     ↓ Write results
┌────────────────────────────────────────────────────┐
│       STAGE 3: ENRICHED ANALYTICS STORAGE          │
│  Cassandra (13 tables) + Neo4j (graphs) + MongoDB│
│                                                    │
│  Dashboard reads from here! ←─────────────────────┤
└────────────────────────────────────────────────────┘
```

---

## 📝 Customization

### Add New Visualizations:

1. Create a new function in the "VISUALIZATIONS" section:
```python
def create_my_custom_chart(df):
    fig = px.line(df, x='timestamp', y='value')
    return fig
```

2. Load the data:
```python
my_data_df = load_cassandra_table('my_custom_table')
```

3. Add to a tab:
```python
with tab1:
    st.subheader("My Custom Chart")
    my_fig = create_my_custom_chart(my_data_df)
    st.plotly_chart(my_fig, use_container_width=True)
```

### Add New Cassandra Tables:

The dashboard automatically supports any Cassandra table in the `financial_data` keyspace:

```python
# Add a new loader function
@st.cache_data(ttl=REFRESH_INTERVAL)
def load_my_new_analytics():
    return load_cassandra_table('my_new_table', limit=500)

# Use it in main()
my_new_df = load_my_new_analytics()
```

---

## 🎓 Understanding the Metrics

### Sentiment Score:
- **Range**: -1.0 (very negative) to +1.0 (very positive)
- **0.0**: Neutral
- **> 0.3**: Bullish sentiment
- **< -0.3**: Bearish sentiment

### Correlation Score:
- **Range**: -1.0 to +1.0
- **> 0.7**: Strong positive correlation
- **< -0.7**: Strong negative correlation
- **-0.3 to 0.3**: No significant correlation

### Spread (BPS):
- **Basis points**: 1 BPS = 0.01%
- **> 10 BPS**: Profitable arbitrage opportunity
- **< 5 BPS**: Too small to trade (fees)

### Divergence Strength:
- **Range**: 0.0 to 1.0
- **> 0.7**: Strong divergence (high confidence signal)
- **0.4-0.7**: Moderate divergence
- **< 0.4**: Weak divergence (ignore)

---

## 📧 Support

If you encounter issues:
1. Check database connections (Cassandra, Neo4j, MongoDB)
2. Verify Spark analytics are running (`spark_analytics.py`)
3. Check logs in the Streamlit sidebar for specific errors
4. Ensure all Docker services are healthy: `docker-compose ps`

---

## 🎉 What Makes This Dashboard Special

Unlike basic dashboards that only show raw data collection, this dashboard provides:

✅ **Complete pipeline visibility**: Stage 1 → 2 → 3
✅ **Multi-database integration**: 5 databases (LMDB, QuestDB, Cassandra, Neo4j, MongoDB)
✅ **Processed insights**: Not just raw data, but ACTIONABLE analytics
✅ **Divergence detection**: KEY INSIGHT for contrarian trading
✅ **Word-price causality**: Leading indicators from text analysis
✅ **Graph relationships**: Semantic networks via Neo4j
✅ **Real-time updates**: Auto-refresh every 5 minutes
✅ **Professional visualizations**: Plotly interactive charts

This is a **production-grade analytics dashboard** that shows the COMPLETE picture of your crypto/stock analytics pipeline!

---

**Happy Trading! 🚀📊💰**
