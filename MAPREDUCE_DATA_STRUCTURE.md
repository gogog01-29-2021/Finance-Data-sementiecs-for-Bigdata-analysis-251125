# 📊 MAPREDUCE DATA STRUCTURE - COMPREHENSIVE GUIDE

## 🔍 RAW DATA SOURCES

### 1. ORDERBOOK DATA (QuestDB) - 396,501 Records

**Sample Record:**
```json
{
  "timestamp": "2025-11-15 10:52:58.753054",
  "exchange": "upbit",
  "symbol": "BTC-KRW",
  "base": "BTC",
  "quote": "KRW",
  "region": "KR",              ← Timezone indicator
  "venue_type": "SPOT",
  "best_bid": 144700000.0,     ← Buy side price
  "best_ask": 144701000.0,     ← Sell side price
  "mid_price": 144700500.0,    ← (bid + ask) / 2
  "spread_bps": 0.069,         ← Spread in basis points
  "recv_ts_s": 1763203978.594
}
```

### 2. REDDIT SENTIMENT DATA (LMDB) - 1,096 Records

**Sample Record:**
```json
{
  "item_id": "1mddode",
  "item_type": "post",
  "subreddit": "SatoshiStreetBets",
  "text": "Introducing Away From Keyboard ($AFK) on Solana 🚀...",
  "sentiment": {
    "polarity": 0.247,          ← -1 (negative) to +1 (positive)
    "subjectivity": 0.628,      ← 0 (objective) to 1 (subjective)
    "label": "positive"
  },
  "timestamp": 1763204117.984
}
```

---

## 🎯 ANALYTICS A-E: MAPREDUCE TRANSFORMATIONS

---

## A) FIX EXISTING ANALYTICS

### A1. ARBITRAGE DETECTION (Cross-Exchange Price Differences)

**MAP PHASE:**
```
Input: Orderbook records
Output: (key, value) pairs

Key = (symbol, timestamp_rounded)
Value = {exchange, region, best_bid, best_ask, mid_price}

Example:
key = ("BTC-KRW", "2025-11-15 10:53:00")
value = [
  {exchange: "upbit", region: "KR", mid_price: 144700500},
  {exchange: "bithumb", region: "KR", mid_price: 144750000},
  {exchange: "binance", region: "GLOBAL", mid_price: 144800000}
]
```

**SHUFFLE PHASE:**
```
Group all values by key
→ All prices for same symbol at same time bucket
```

**REDUCE PHASE:**
```
For each key (symbol, time):
  - Cross-join all exchange pairs
  - Calculate spread_bps = (sell_price - buy_price) / buy_price * 10000
  - Filter: spread_bps > THRESHOLD (reduce from 50 → 10-20 bps)

Output:
key = ("BTC-KRW", "upbit→binance")
value = {
  buy_exchange: "upbit",
  sell_exchange: "binance",
  buy_price: 144700500,
  sell_price: 144800000,
  spread_bps: 68.8,
  profit_pct: 0.688%
}
```

---

### A2. SENTIMENT-PRICE CORRELATION

**MAP PHASE 1 (Sentiment):**
```
Input: Sentiment records
Output: (key, value) pairs

Key = (time_window, subreddit)
Value = sentiment_polarity

Example:
key = ("2025-11-15 10:00:00-11:00:00", "SatoshiStreetBets")
value = [0.247, -0.125, 0.108, 0.164, 0.239]
```

**REDUCE PHASE 1 (Sentiment Aggregation):**
```
For each key (time_window, subreddit):
  avg_sentiment = mean(values)
  sentiment_volatility = stddev(values)
  post_count = count(values)

Output:
key = ("2025-11-15 10:00:00-11:00:00", "SatoshiStreetBets")
value = {avg_sentiment: 0.127, sentiment_volatility: 0.145, post_count: 5}
```

**MAP PHASE 2 (Price):**
```
Input: Orderbook records
Output: (key, value) pairs

Key = (time_window, symbol)
Value = mid_price

Example:
key = ("2025-11-15 10:00:00-11:00:00", "BTC-KRW")
value = [144700500, 144750000, 144800000, ...]
```

**REDUCE PHASE 2 (Price Aggregation):**
```
For each key (time_window, symbol):
  avg_price = mean(values)
  max_price = max(values)
  min_price = min(values)
  price_volatility = (max_price - min_price) / avg_price * 100

Output:
key = ("2025-11-15 10:00:00-11:00:00", "BTC-KRW")
value = {avg_price: 144750000, price_volatility: 0.5%}
```

**JOIN PHASE:**
```
Join sentiment_agg and price_agg on time_window

Output:
key = "2025-11-15 10:00:00-11:00:00"
value = {
  subreddit: "SatoshiStreetBets",
  symbol: "BTC-KRW",
  avg_sentiment: 0.127,
  post_count: 5,
  avg_price: 144750000,
  price_volatility: 0.5%
}
```

---

### A3. PRICE PREDICTION (Moving Averages)

**MAP PHASE:**
```
Input: Orderbook records (last 24 hours instead of 1 hour)
Output: (key, value) pairs

Key = (symbol, exchange)
Value = {timestamp, mid_price, spread_bps}

Sorted by timestamp
```

**REDUCE PHASE (Window Function):**
```
For each key (symbol, exchange):
  - Sort values by timestamp
  - Calculate rolling MA_10 (10-period moving average)
  - Detect trend: UP if price > MA, DOWN if price < MA
  - Calculate confidence = |price - MA| / MA * 100

Output:
key = ("BTC-KRW", "upbit")
value = {
  timestamp: "2025-11-15 10:53:00",
  mid_price: 144700500,
  ma_10: 144500000,
  trend: "UP",
  prediction: "BULLISH",
  confidence: 0.14%
}
```

---

## B) VOLUME & LIQUIDITY ANALYTICS

### B1. ORDER BOOK DEPTH ANALYSIS

**MAP PHASE:**
```
Input: Orderbook records
Output: (key, value) pairs

Key = (symbol, exchange, price_level)
Value = volume_at_level

Example (simulated from bid/ask spread):
key = ("BTC-KRW", "upbit", "within_0.1%")
value = estimated_depth = 1 / spread_bps
```

**REDUCE PHASE:**
```
For each key (symbol, exchange):
  depth_0.1pct = sum(volumes within 0.1% of mid)
  depth_0.5pct = sum(volumes within 0.5% of mid)
  depth_1.0pct = sum(volumes within 1.0% of mid)

  liquidity_score = depth_0.1pct / (spread_bps * 100)

Output:
key = ("BTC-KRW", "upbit")
value = {
  depth_0_1_pct: 500000000,  # 500M KRW
  depth_0_5_pct: 2000000000,
  liquidity_score: 7200,
  market_depth_tier: "DEEP"
}
```

---

### B2. VOLUME-WEIGHTED AVERAGE PRICE (VWAP)

**Note:** Current data doesn't have volume. We'll use **spread-weighted** approximation.

**MAP PHASE:**
```
Input: Orderbook records
Output: (key, value) pairs

Key = (symbol, exchange, time_window)
Value = {mid_price, spread_bps, timestamp}
```

**REDUCE PHASE:**
```
For each key:
  # Lower spread = higher liquidity = higher weight
  inverse_spread = 1 / spread_bps

  total_weighted_price = sum(mid_price * inverse_spread)
  total_weights = sum(inverse_spread)

  VWAP = total_weighted_price / total_weights

Output:
key = ("BTC-KRW", "upbit", "2025-11-15 10:00-11:00")
value = {
  VWAP: 144725000,
  simple_avg: 144750000,
  vwap_vs_avg: -0.017%,  # VWAP < avg means more trades at lower prices
  avg_spread: 0.5 bps
}
```

---

### B3. BID-ASK IMBALANCE (Buy/Sell Pressure)

**MAP PHASE:**
```
Input: Orderbook records
Output: (key, value) pairs

Key = (symbol, exchange, time_window)
Value = {best_bid, best_ask, mid_price}
```

**REDUCE PHASE:**
```
For each key:
  bid_strength = mid_price - best_bid
  ask_strength = best_ask - mid_price

  imbalance_ratio = (bid_strength - ask_strength) / (bid_strength + ask_strength)

  # imbalance_ratio > 0 → More buy pressure
  # imbalance_ratio < 0 → More sell pressure

Output:
key = ("BTC-KRW", "upbit", "2025-11-15 10:00-11:00")
value = {
  imbalance_ratio: 0.15,     # Bullish (buy pressure)
  bid_strength: 250,
  ask_strength: 200,
  prediction: "BUY_PRESSURE",
  confidence: "MEDIUM"
}
```

---

## C) VOLATILITY & PRICE PATTERN DETECTION

### C1. ROLLING VOLATILITY (1h, 4h, 24h)

**MAP PHASE:**
```
Input: Orderbook records
Output: (key, value) pairs

Key = (symbol, exchange)
Value = {timestamp, mid_price}
```

**REDUCE PHASE (Window Functions):**
```
For each key (symbol, exchange):
  Sort by timestamp

  # 1-hour window
  volatility_1h = stddev(mid_price[last_1_hour]) / mean(mid_price) * 100

  # 4-hour window
  volatility_4h = stddev(mid_price[last_4_hours]) / mean(mid_price) * 100

  # 24-hour window
  volatility_24h = stddev(mid_price[last_24_hours]) / mean(mid_price) * 100

  # Detect spike
  volatility_spike = volatility_1h > (volatility_24h * 2)

Output:
key = ("BTC-KRW", "upbit")
value = {
  timestamp: "2025-11-15 10:53:00",
  volatility_1h: 0.8%,
  volatility_4h: 1.2%,
  volatility_24h: 2.5%,
  volatility_trend: "DECREASING",
  spike_detected: False
}
```

---

### C2. BOLLINGER BANDS

**MAP PHASE:**
```
Input: Orderbook records
Output: (key, value) pairs

Key = (symbol, exchange)
Value = {timestamp, mid_price}
```

**REDUCE PHASE:**
```
For each key:
  Sort by timestamp
  Calculate 20-period moving average
  Calculate 20-period standard deviation

  MA_20 = mean(mid_price[last_20])
  STD_20 = stddev(mid_price[last_20])

  upper_band = MA_20 + (2 * STD_20)
  lower_band = MA_20 - (2 * STD_20)

  current_price = latest(mid_price)

  band_position = (current_price - lower_band) / (upper_band - lower_band)

  # band_position > 0.8 → Overbought
  # band_position < 0.2 → Oversold

Output:
key = ("BTC-KRW", "upbit")
value = {
  timestamp: "2025-11-15 10:53:00",
  mid_price: 144700500,
  MA_20: 144500000,
  upper_band: 145500000,
  lower_band: 143500000,
  band_position: 0.6,
  signal: "NEUTRAL",
  band_width_pct: 1.38%
}
```

---

### C3. FLASH EVENT DETECTION (Rapid Price Movements)

**MAP PHASE:**
```
Input: Orderbook records
Output: (key, value) pairs

Key = (symbol, exchange, minute_bucket)
Value = {timestamp, mid_price}
```

**REDUCE PHASE:**
```
For each key (1-minute window):
  first_price = first(mid_price)
  last_price = last(mid_price)
  max_price = max(mid_price)
  min_price = min(mid_price)

  minute_change_pct = (last_price - first_price) / first_price * 100
  minute_range_pct = (max_price - min_price) / first_price * 100

  flash_event = (abs(minute_change_pct) > 5%) OR (minute_range_pct > 7%)

Output:
key = ("BTC-KRW", "upbit", "2025-11-15 10:52:00-10:53:00")
value = {
  flash_event_detected: True,
  minute_change_pct: -6.2%,    # Flash crash
  max_swing_pct: 8.5%,
  event_type: "FLASH_CRASH",
  recovery_within_minute: False
}
```

---

## D) ENHANCED SENTIMENT ANALYTICS

### D1. SENTIMENT MOMENTUM (Rate of Change)

**MAP PHASE:**
```
Input: Sentiment records
Output: (key, value) pairs

Key = (subreddit, hour_bucket)
Value = {timestamp, sentiment_polarity}
```

**REDUCE PHASE:**
```
For each subreddit:
  Sort by hour_bucket

  current_hour_sentiment = avg(sentiment[current_hour])
  previous_hour_sentiment = avg(sentiment[previous_hour])

  sentiment_momentum = current_hour_sentiment - previous_hour_sentiment

  momentum_acceleration = momentum[t] - momentum[t-1]

Output:
key = ("SatoshiStreetBets", "2025-11-15 10:00")
value = {
  current_sentiment: 0.15,
  previous_sentiment: -0.05,
  sentiment_momentum: +0.20,      # Strong positive shift
  momentum_type: "BULLISH_REVERSAL",
  post_count_change: +150%
}
```

---

### D2. SENTIMENT DIVERGENCE (Sentiment ≠ Price)

**MAP PHASE 1 (Sentiment):**
```
Key = (asset, hour_bucket)
Value = avg_sentiment
```

**MAP PHASE 2 (Price):**
```
Key = (asset, hour_bucket)
Value = price_change_pct
```

**JOIN & REDUCE:**
```
For each (asset, hour):
  sentiment_direction = sign(avg_sentiment)
  price_direction = sign(price_change_pct)

  divergence = (sentiment_direction != price_direction)

  divergence_strength = abs(avg_sentiment) * abs(price_change_pct)

Output:
key = ("BTC", "2025-11-15 10:00")
value = {
  avg_sentiment: +0.25,         # Positive
  price_change_pct: -2.5%,      # Negative
  divergence_detected: True,
  divergence_type: "BEARISH_DIVERGENCE",
  strength: 0.625,
  prediction: "PRICE_MAY_FOLLOW_SENTIMENT_UP"
}
```

---

### D3. SENTIMENT LEADING INDICATORS (Which Subreddit Leads?)

**MAP PHASE:**
```
Input: Sentiment + Price (joined with lags)
Output: (key, value) pairs

Key = (subreddit, asset, lag_hours)
Value = {sentiment_at_T, price_change_at_T+lag}
```

**REDUCE PHASE:**
```
For each (subreddit, asset):
  Test multiple lags [1h, 2h, 4h, 8h, 12h]

  For each lag:
    correlation = corr(sentiment_at_T, price_change_at_T+lag)

  best_lag = lag with highest correlation
  lead_strength = correlation_value

Output:
key = ("BitcoinKR", "BTC")
value = {
  best_lag_hours: 4,            # KR sentiment leads price by 4 hours
  correlation: 0.68,
  lead_strength: "STRONG",
  predictive_power: "HIGH",
  timezone_factor: "KR_LEADS_GLOBAL"
}
```

---

## E) CROSS-ASSET ANALYTICS

### E1. CORRELATION MATRIX (BTC-ETH, BTC-SOL)

**MAP PHASE:**
```
Input: Orderbook records
Output: (key, value) pairs

Key = (time_window)
Value = {asset: price_change_pct}
```

**REDUCE PHASE:**
```
For each time_window:
  Group all assets

  assets_dict = {
    "BTC": [price_changes],
    "ETH": [price_changes],
    "SOL": [price_changes],
    ...
  }

  # Calculate correlation matrix
  correlation_matrix = corr(assets_dict)

Output:
key = "2025-11-15 (24h rolling)"
value = {
  correlations: {
    ("BTC", "ETH"): 0.85,
    ("BTC", "SOL"): 0.72,
    ("ETH", "SOL"): 0.80,
    ...
  },
  market_regime: "HIGH_CORRELATION",  # All assets moving together
  diversification_benefit: "LOW"
}
```

---

### E2. PAIRS TRADING SIGNALS (ETH/BTC Ratio)

**MAP PHASE:**
```
Input: Orderbook records
Output: (key, value) pairs

Key = (time_bucket)
Value = {BTC_price, ETH_price}
```

**REDUCE PHASE:**
```
For each time_bucket:
  ratio = ETH_price / BTC_price

  # Calculate z-score
  mean_ratio = mean(ratio[last_30_days])
  std_ratio = stddev(ratio[last_30_days])

  z_score = (current_ratio - mean_ratio) / std_ratio

  # Trading signal
  if z_score > 2:
    signal = "SHORT_ETH_LONG_BTC"  # Ratio too high
  elif z_score < -2:
    signal = "LONG_ETH_SHORT_BTC"  # Ratio too low
  else:
    signal = "NO_TRADE"

Output:
key = "2025-11-15 10:53:00"
value = {
  ETH_BTC_ratio: 0.033,
  mean_ratio: 0.032,
  std_ratio: 0.001,
  z_score: 1.0,
  signal: "NO_TRADE",
  expected_mean_reversion: False
}
```

---

### E3. MARKET REGIME DETECTION

**MAP PHASE:**
```
Input: Orderbook records (all assets)
Output: (key, value) pairs

Key = (date)
Value = {asset, volatility, correlation_to_BTC, price_change}
```

**REDUCE PHASE:**
```
For each date:
  # Aggregate across all assets
  avg_volatility = mean(volatility[all_assets])
  avg_correlation_to_BTC = mean(correlation[all_assets])
  market_direction = sign(mean(price_change[all_assets]))

  # Classify regime
  if avg_volatility > 3% and avg_correlation > 0.8:
    regime = "CRISIS_MODE"
  elif avg_volatility < 1% and market_direction > 0:
    regime = "BULL_MARKET"
  elif avg_volatility < 1% and market_direction < 0:
    regime = "BEAR_MARKET"
  else:
    regime = "SIDEWAYS_CHOPPY"

Output:
key = "2025-11-15"
value = {
  market_regime: "BULL_MARKET",
  avg_volatility: 0.8%,
  avg_correlation_to_BTC: 0.75,
  regime_strength: "STRONG",
  suggested_strategy: "MOMENTUM_FOLLOWING"
}
```

---

## 🎯 SUMMARY: MAPREDUCE PATTERN FOR ALL ANALYTICS

| Analytics | MAP Key | MAP Value | REDUCE Operation | Output |
|-----------|---------|-----------|------------------|--------|
| A1: Arbitrage | (symbol, time) | {exchange, price} | Cross-join, calc spread | Arbitrage opportunities |
| A2: Sentiment Correlation | (time_window) | {sentiment, price} | Aggregate, join | Correlation metrics |
| A3: Price Prediction | (symbol, exchange) | {timestamp, price} | Moving avg, trend | Price predictions |
| B1: Order Depth | (symbol, exchange) | {price_level, volume} | Sum by level | Liquidity metrics |
| B2: VWAP | (symbol, time_window) | {price, spread} | Weighted avg | VWAP vs price |
| B3: Bid-Ask Imbalance | (symbol, exchange) | {bid, ask} | Calc ratio | Buy/sell pressure |
| C1: Volatility | (symbol, exchange) | {timestamp, price} | Stddev windows | Multi-timeframe vol |
| C2: Bollinger Bands | (symbol, exchange) | {timestamp, price} | MA + 2*stddev | Overbought/oversold |
| C3: Flash Events | (symbol, minute) | {timestamp, price} | Max-min in window | Flash crash detection |
| D1: Sentiment Momentum | (subreddit, hour) | {sentiment} | Current - previous | Momentum & acceleration |
| D2: Divergence | (asset, hour) | {sentiment, price} | Compare directions | Divergence signals |
| D3: Leading Indicators | (subreddit, asset, lag) | {sentiment, price} | Correlation by lag | Best predictive lag |
| E1: Correlation Matrix | (time_window) | {asset: price_change} | Pairwise corr | Correlation matrix |
| E2: Pairs Trading | (time_bucket) | {asset1_price, asset2_price} | Ratio z-score | Trading signals |
| E3: Market Regime | (date) | {volatility, correlation} | Classify regime | Regime + strategy |

---

## 🚀 NEXT STEPS

**Which analytics do you want me to implement?**

1. **Quick Wins (2-3 hours):**
   - A: Fix arbitrage threshold + prediction window
   - B1-B3: Volume & Liquidity (all 3)
   - C1: Rolling volatility

2. **Medium Effort (4-6 hours):**
   - C2-C3: Bollinger Bands + Flash Detection
   - D1-D2: Sentiment momentum + divergence
   - E1: Correlation matrix

3. **Full Suite (8-10 hours):**
   - ALL A-E analytics
   - Write to Cassandra/Neo4j/MongoDB
   - Dashboard-ready outputs

**Tell me which group or specific analytics you want, and I'll implement them with proper MapReduce patterns!**
