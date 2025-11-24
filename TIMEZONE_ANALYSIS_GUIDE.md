# Timezone-Aware Correlation Analysis Guide

## Overview

This pipeline performs **two types** of correlation analysis:

1. **Same Timezone (MapReduce)**: Find patterns within same region/time
2. **Cross Timezone (Lead-Lag)**: Predict price movements across regions

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    DATA SOURCES                                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Reddit (LMDB)              QuestDB (Timeseries)                │
│  ┌──────────────┐           ┌──────────────┐                   │
│  │ Sentiment    │           │ Orderbook    │                   │
│  │ - subreddit  │           │ - exchange   │                   │
│  │ - text       │           │ - region     │                   │
│  │ - timestamp  │           │ - mid_price  │                   │
│  │ - polarity   │           │ - timestamp  │                   │
│  └──────────────┘           └──────────────┘                   │
│         │                          │                             │
│         └──────────┬───────────────┘                            │
│                    ▼                                             │
└─────────────────────────────────────────────────────────────────┘
                     │
        ┌────────────┴────────────┐
        │                         │
        ▼                         ▼
┌──────────────────┐    ┌──────────────────┐
│ SAME TIMEZONE    │    │ CROSS TIMEZONE   │
│ (MapReduce)      │    │ (Lead-Lag)       │
└──────────────────┘    └──────────────────┘
```

---

## Part 1: Same Timezone MapReduce

### What is it?

Analyzes **correlations within the same timezone and time period** using MapReduce paradigm.

### MapReduce Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                        MAP PHASE                                 │
└─────────────────────────────────────────────────────────────────┘

Reddit Posts (r/BitcoinKR, 3 AM UTC = Noon in Korea)
    ↓
Sentiment MAP: Extract (key, value)
    Key = (timezone: KR, asset: BTC, hour: 3, sentiment_tier: POSITIVE)
    Value = (polarity: 0.7, count: 1)

QuestDB Orderbook (Upbit, 3 AM UTC = Noon in Korea)
    ↓
Price MAP: Extract (key, value)
    Key = (timezone: KR, asset: BTC, hour: 3)
    Value = (price_change: +2.3%, volatility: 1.5%)

┌─────────────────────────────────────────────────────────────────┐
│                       REDUCE PHASE                               │
└─────────────────────────────────────────────────────────────────┘

GROUP BY (timezone, asset, hour)
    ↓
Aggregate:
    KR, BTC, hour=3:
        - avg_sentiment: 0.65
        - post_volume: 47
        - avg_price_change: +2.1%
        - volatility: 1.8%

PATTERN FOUND:
    "In Korea timezone, during hour 3 UTC (noon local),
     when BTC sentiment is positive (0.65),
     prices tend to rise (+2.1%)"
```

### Key Dimensions (Composite Keys)

The MapReduce creates composite keys with multiple dimensions:

```python
Composite Key = {
    timezone: "KR" | "GLOBAL",
    asset: "BTC" | "ETH" | "SOL" | ...,
    sentiment_tier: "STRONG_POSITIVE" | "POSITIVE" | "NEUTRAL" | "NEGATIVE" | "STRONG_NEGATIVE",
    hour_utc: 0-23,
    day_of_week: 1-7 (Monday-Sunday)
}
```

### Patterns Discovered

**1. Hourly Patterns**
```
┌──────────┬───────┬──────┬────────────────┬─────────────┬──────────────┐
│ timezone │ asset │ hour │ avg_sentiment  │ post_volume │ price_change │
├──────────┼───────┼──────┼────────────────┼─────────────┼──────────────┤
│ KR       │ BTC   │ 0    │ 0.45           │ 120         │ +1.2%        │
│ KR       │ BTC   │ 1    │ 0.52           │ 145         │ +1.8%        │
│ KR       │ BTC   │ 2    │ 0.38           │ 98          │ +0.5%        │
│ GLOBAL   │ BTC   │ 14   │ 0.31           │ 450         │ +0.8%        │
│ GLOBAL   │ BTC   │ 15   │ 0.41           │ 520         │ +1.5%        │
└──────────┴───────┴──────┴────────────────┴─────────────┴──────────────┘

INSIGHT: Korean timezone shows higher sentiment-price correlation during
         hours 0-8 UTC (9 AM - 5 PM KST = peak trading hours)
```

**2. Sentiment Tier Patterns**
```
┌──────────┬────────────────────┬─────────────┬─────────────┐
│ timezone │ sentiment_tier     │ total_posts │ avg_impact  │
├──────────┼────────────────────┼─────────────┼─────────────┤
│ KR       │ STRONG_POSITIVE    │ 234         │ +2.5%       │
│ KR       │ POSITIVE           │ 567         │ +1.1%       │
│ KR       │ NEUTRAL            │ 890         │ +0.2%       │
│ KR       │ NEGATIVE           │ 345         │ -0.8%       │
│ KR       │ STRONG_NEGATIVE    │ 123         │ -2.1%       │
└──────────┴────────────────────┴─────────────┴─────────────┘

INSIGHT: Strong sentiment (±0.5 polarity) correlates with larger
         price movements in same timezone/time
```

**3. Weekly Patterns**
```
┌──────────┬─────────────┬─────────────┬──────────────┐
│ timezone │ day_of_week │ post_volume │ price_change │
├──────────┼─────────────┼─────────────┼──────────────┤
│ KR       │ 1 (Mon)     │ 450         │ +0.5%        │
│ KR       │ 2 (Tue)     │ 520         │ +1.2%        │
│ KR       │ 5 (Fri)     │ 380         │ -0.3%        │
│ KR       │ 7 (Sun)     │ 280         │ +0.1%        │
└──────────┴─────────────┴─────────────┴──────────────┘

INSIGHT: Tuesday shows highest sentiment + price activity in KR
```

---

## Part 2: Cross Timezone Lead-Lag

### What is it?

Tests if sentiment in **one timezone predicts** price movements in **another timezone** with time lag.

### Lead-Lag Flow

```
Timeline (UTC):
════════════════════════════════════════════════════════════════

00:00 UTC (9 AM Korea)
    │
    │  r/BitcoinKR: "BTC breaking resistance!"
    │  Sentiment: +0.75 (STRONG_POSITIVE)
    │
    ↓ [2 hour lag]
    │
02:00 UTC
    │  Upbit (Korea): BTC/KRW +2.1%
    │  ✓ CORRELATION FOUND (2h lag, same timezone)
    │
    ↓ [8 hour lag]
    │
08:00 UTC (9 AM London)
    │  Binance (Global): BTC/USDT +1.5%
    │  ✓ CORRELATION FOUND (8h lag, cross timezone)
    │
    ↓ [12 hour lag]
    │
12:00 UTC (9 AM New York)
    │  Coinbase (Global): BTC/USD +0.8%
    │  ✓ CORRELATION FOUND (12h lag, cross timezone)
    │
════════════════════════════════════════════════════════════════

HYPOTHESIS TESTED:
    Korean Reddit sentiment (T) → Global prices (T + 8-12h)
    Does early Asian sentiment predict later Western price action?
```

### Lead-Lag Scenarios

**Scenario 1: Same Timezone, Short Lag**
```
Korean Sentiment (T) → Korean Prices (T + 2h)
├─ Tests: Does Reddit discussion predict near-term local price?
└─ Example: Morning r/BitcoinKR buzz → afternoon Upbit rally
```

**Scenario 2: Cross Timezone, Medium Lag**
```
Korean Sentiment (T) → Global Prices (T + 8h)
├─ Tests: Does Asian sentiment predict European trading?
└─ Example: Asian optimism → European session pump
```

**Scenario 3: Cross Timezone, Long Lag**
```
Korean Sentiment (T) → Global Prices (T + 12h)
├─ Tests: Does early timezone predict later timezone?
└─ Example: Asian news → US market reaction
```

**Scenario 4: Reverse Causality**
```
Global Sentiment (T) → Korean Prices (T + 6h)
├─ Tests: Does Western sentiment affect Asian markets?
└─ Example: US evening news → Korean morning prices
```

### Results Format

```sql
SELECT
    sentiment_time,      -- When sentiment occurred
    sentiment_tz,        -- KR or GLOBAL
    price_tz,            -- KR or GLOBAL
    asset,               -- BTC, ETH, etc.
    lag_hours,           -- 2, 4, 8, or 12
    avg_sentiment,       -- Average polarity
    post_count,          -- Volume of posts
    price_volatility     -- Price movement %
FROM timezone_lead_lag_correlations
ORDER BY lag_hours, sentiment_time
```

Example output:
```
┌─────────────────────┬──────────────┬──────────┬───────┬───────────┬───────────────┬─────────────┬──────────────┐
│ sentiment_time      │ sentiment_tz │ price_tz │ asset │ lag_hours │ avg_sentiment │ post_count  │ volatility   │
├─────────────────────┼──────────────┼──────────┼───────┼───────────┼───────────────┼─────────────┼──────────────┤
│ 2024-01-15 00:00:00 │ KR           │ KR       │ BTC   │ 2         │ 0.65          │ 45          │ 2.1%         │
│ 2024-01-15 00:00:00 │ KR           │ GLOBAL   │ BTC   │ 8         │ 0.65          │ 45          │ 1.5%         │
│ 2024-01-15 00:00:00 │ KR           │ GLOBAL   │ BTC   │ 12        │ 0.65          │ 45          │ 0.8%         │
│ 2024-01-15 14:00:00 │ GLOBAL       │ KR       │ BTC   │ 6         │ 0.42          │ 230         │ 1.2%         │
└─────────────────────┴──────────────┴──────────┴───────┴───────────┴───────────────┴─────────────┴──────────────┘

INSIGHT: Korean sentiment at 00:00 UTC shows:
    - Strongest correlation with KR prices at +2h lag (2.1% volatility)
    - Moderate correlation with GLOBAL prices at +8h lag (1.5%)
    - Weakening correlation at +12h lag (0.8%)

CONCLUSION: Korean sentiment has predictive power for:
    1. Local market (2h lead time)
    2. European market (8h lead time)
    3. Diminishing effect on US market (12h lead time)
```

---

## How to Use

### Step 1: Start Infrastructure

```bash
# Start Docker Desktop first!

# Then start containers
docker-compose up -d

# Wait for containers to be healthy
docker ps
```

### Step 2: Collect Data (Run for Several Hours)

```bash
# Collect crypto/stock prices + Reddit sentiment
python main.py

# Let it run for at least 4-6 hours to get meaningful data
```

### Step 3: Run Timezone-Aware Analytics

```bash
# Run integrated pipeline
python spark_integrated_pipeline.py

# This will:
# 1. Run original analytics (arbitrage, sentiment, prediction)
# 2. Run same-timezone MapReduce pattern mining
# 3. Run cross-timezone lead-lag correlation
# 4. Write all results to MongoDB/Cassandra
```

### Step 4: View Results

```bash
# Query all databases
python query_all_data.py

# Or query MongoDB directly for timezone analytics
mongosh
> use financial_analytics
> db.timezone_hourly_patterns.find().pretty()
> db.timezone_lead_lag_correlations.find().pretty()
```

---

## Understanding the Output

### Same Timezone Patterns (MapReduce Results)

**Collection: `timezone_hourly_patterns`**

```javascript
{
  "timezone": "KR",
  "asset": "BTC",
  "hour": 3,
  "avg_sentiment": 0.65,
  "sentiment_std": 0.23,
  "post_volume": 47,
  "avg_price_change": 2.1,
  "price_volatility": 1.8,
  "avg_spread": 5.2
}
```

**Interpretation:**
- During hour 3 UTC (noon in Korea)
- Korean BTC sentiment averaged 0.65 (positive)
- 47 posts during that hour
- Prices rose +2.1% on average
- **Pattern**: Positive sentiment during Korean trading hours → price increase

---

### Cross Timezone Correlations (Lead-Lag Results)

**Collection: `timezone_lead_lag_correlations`**

```javascript
{
  "sentiment_time": "2024-01-15T00:00:00Z",
  "sentiment_tz": "KR",
  "price_tz": "GLOBAL",
  "asset": "BTC",
  "lag_hours": 8,
  "avg_sentiment": 0.65,
  "post_count": 45,
  "avg_price": 43250.50,
  "price_volatility": 1.5
}
```

**Interpretation:**
- Korean sentiment at midnight UTC (9 AM Korea)
- Sentiment: +0.65 (positive), 45 posts
- 8 hours later → Global BTC prices showed 1.5% volatility
- **Hypothesis**: Korean morning sentiment predicts European afternoon prices

---

## Key Insights to Look For

### 1. Timezone Leadership
```
Question: Which timezone's sentiment has strongest predictive power?

Look for:
- Highest correlation coefficients
- Consistent patterns across lag periods
- Volume-weighted sentiment impact
```

### 2. Optimal Lag Period
```
Question: What's the best time lag for prediction?

Compare:
- 2h lag (same timezone, near-term)
- 4h lag (same timezone, medium-term)
- 8h lag (cross timezone, next session)
- 12h lag (cross timezone, next major market)
```

### 3. Asset-Specific Behavior
```
Question: Do different assets show different patterns?

Compare:
- BTC (high liquidity, 24/7 global)
- ETH (similar to BTC)
- Altcoins (lower liquidity, higher impact from sentiment)
```

### 4. Day-of-Week Effects
```
Question: Are there weekly patterns?

Look for:
- Monday effect (weekend news accumulation)
- Friday effect (position closing before weekend)
- Weekend vs. weekday differences
```

---

## Advanced Analysis

### Calculate Correlation Strength

```python
# Use Spark to calculate Pearson correlation
from pyspark.sql.functions import corr

correlation_strength = lead_lag_df.groupBy("lag_hours", "sentiment_tz", "price_tz").agg(
    corr("avg_sentiment", "price_volatility").alias("pearson_corr")
)
```

### Find Strongest Predictive Signals

```python
# Which timezone + lag combination has highest correlation?
best_signals = correlation_strength.orderBy(
    col("pearson_corr").desc()
).limit(10)
```

---

## Next Steps

1. **Collect More Data**: Run for 24+ hours to get full daily cycles
2. **Tune Parameters**: Adjust lag periods based on initial results
3. **Add More Assets**: Include more crypto symbols
4. **Statistical Tests**: Add significance testing (p-values)
5. **Machine Learning**: Use patterns as features for price prediction models

---

## Technical Notes

- **MapReduce in Spark**: Uses DataFrame groupBy + agg (optimized for distributed computing)
- **Timezone Handling**: All timestamps in UTC, regions assigned by exchange/subreddit
- **Key Uniqueness**: Composite keys prevent collision, enable multi-dimensional analysis
- **Scalability**: Designed to handle millions of records efficiently

---

## Questions?

This is a research-grade analytics pipeline. Experiment with:
- Different lag periods
- New timezone definitions
- Additional key dimensions
- Custom aggregation functions

**The goal**: Find exploitable patterns in cross-market information flow!
