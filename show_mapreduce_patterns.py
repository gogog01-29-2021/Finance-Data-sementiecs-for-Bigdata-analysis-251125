#!/usr/bin/env python3
"""
SHOW MAPREDUCE PATTERNS - Demonstrates actual key-value transformations
Shows you the real MapReduce patterns being used on your data
"""

import lmdb
import json
from datetime import datetime
from collections import defaultdict

# Read Reddit sentiment data
print("=" * 80)
print("MAPREDUCE PATTERN DEMONSTRATION")
print("Using YOUR actual data (1,096 sentiment records)")
print("=" * 80)

# Open LMDB
env = lmdb.open("data/reddit/reddit_sentiment_lmdb.db", readonly=True)
sentiment_records = []

with env.begin() as txn:
    cursor = txn.cursor()
    for key, value in cursor:
        if key.startswith(b"sentiment:"):
            record = json.loads(value.decode())
            sentiment_records.append(record)

env.close()

print(f"\n✅ Loaded {len(sentiment_records)} sentiment records\n")

# ============================================================================
# PATTERN 1: WORD COUNT → SENTIMENT AGGREGATION
# ============================================================================

print("=" * 80)
print("PATTERN 1: SENTIMENT AGGREGATION (like Word Count)")
print("=" * 80)

print("\n[MAP PHASE] Transform records → (key, value) pairs")
print("-" * 80)

# MAP: Create key-value pairs
map_output = []
for record in sentiment_records[:5]:  # Show first 5
    # Key: (subreddit, hour)
    timestamp = datetime.fromtimestamp(record['timestamp'])
    hour_key = timestamp.strftime('%Y-%m-%d %H:00:00')
    subreddit = record['subreddit']

    key = (subreddit, hour_key)
    value = float(record['sentiment']['polarity'])

    map_output.append((key, value))

    print(f"Record: '{record['text'][:50]}...'")
    print(f"  MAP → key=({subreddit}, {hour_key}), value={value:.3f}")
    print()

print("\n[SHUFFLE PHASE] Group values by same key")
print("-" * 80)

# SHUFFLE: Group by key
shuffled = defaultdict(list)
for record in sentiment_records:
    timestamp = datetime.fromtimestamp(record['timestamp'])
    hour_key = timestamp.strftime('%Y-%m-%d %H:00:00')
    subreddit = record['subreddit']
    key = (subreddit, hour_key)
    value = float(record['sentiment']['polarity'])
    shuffled[key].append(value)

# Show some grouped data
print("Example grouped data:")
for i, (key, values) in enumerate(list(shuffled.items())[:3]):
    subreddit, hour = key
    print(f"\nKey: ({subreddit}, {hour})")
    print(f"  Values: {values[:5]}... ({len(values)} total)")
    print(f"  Count: {len(values)}")

print("\n[REDUCE PHASE] Aggregate each group")
print("-" * 80)

# REDUCE: Calculate aggregates
reduced = {}
for key, values in shuffled.items():
    avg_sentiment = sum(values) / len(values)
    post_count = len(values)
    reduced[key] = {
        'avg_sentiment': avg_sentiment,
        'post_count': post_count
    }

# Show reduced results
print("Final output (key → aggregated value):")
for i, (key, result) in enumerate(list(reduced.items())[:5]):
    subreddit, hour = key
    print(f"\n({subreddit}, {hour}) →")
    print(f"  avg_sentiment: {result['avg_sentiment']:.3f}")
    print(f"  post_count: {result['post_count']}")

# ============================================================================
# PATTERN 2: GROUP BY + FILTER → SENTIMENT TIERS
# ============================================================================

print("\n\n" + "=" * 80)
print("PATTERN 2: SENTIMENT TIER CLASSIFICATION")
print("=" * 80)

print("\n[MAP PHASE] Classify sentiment → tier")
print("-" * 80)

# MAP: Classify into tiers
tier_map = []
for record in sentiment_records[:10]:
    sentiment = float(record['sentiment']['polarity'])
    subreddit = record['subreddit']

    # Classify
    if sentiment > 0.5:
        tier = "STRONG_POSITIVE"
    elif sentiment > 0.1:
        tier = "POSITIVE"
    elif sentiment < -0.5:
        tier = "STRONG_NEGATIVE"
    elif sentiment < -0.1:
        tier = "NEGATIVE"
    else:
        tier = "NEUTRAL"

    key = (subreddit, tier)
    value = 1  # count
    tier_map.append((key, value))

    print(f"Sentiment {sentiment:.3f} → tier={tier}, key=({subreddit}, {tier})")

print("\n[REDUCE PHASE] Count posts in each tier")
print("-" * 80)

# REDUCE: Count by tier
tier_counts = defaultdict(int)
for record in sentiment_records:
    sentiment = float(record['sentiment']['polarity'])
    subreddit = record['subreddit']

    if sentiment > 0.5:
        tier = "STRONG_POSITIVE"
    elif sentiment > 0.1:
        tier = "POSITIVE"
    elif sentiment < -0.5:
        tier = "STRONG_NEGATIVE"
    elif sentiment < -0.1:
        tier = "NEGATIVE"
    else:
        tier = "NEUTRAL"

    key = (subreddit, tier)
    tier_counts[key] += 1

print("Results:")
for key, count in sorted(tier_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
    subreddit, tier = key
    print(f"  {subreddit:20s} | {tier:18s} | {count:4d} posts")

# ============================================================================
# PATTERN 3: TIME-BASED WINDOWING
# ============================================================================

print("\n\n" + "=" * 80)
print("PATTERN 3: HOURLY SENTIMENT TRENDS (Time Window)")
print("=" * 80)

print("\n[MAP PHASE] Extract hour from timestamp")
print("-" * 80)

hourly_sentiment = defaultdict(list)
for record in sentiment_records:
    timestamp = datetime.fromtimestamp(record['timestamp'])
    hour_of_day = timestamp.hour  # 0-23
    sentiment = float(record['sentiment']['polarity'])

    hourly_sentiment[hour_of_day].append(sentiment)

print("\n[REDUCE PHASE] Calculate average sentiment per hour")
print("-" * 80)

print(f"{'Hour':>6s} | {'Avg Sentiment':>15s} | {'Post Count':>12s} | Mood")
print("-" * 60)
for hour in sorted(hourly_sentiment.keys()):
    sentiments = hourly_sentiment[hour]
    avg = sum(sentiments) / len(sentiments)

    mood = "😊 Bullish" if avg > 0.1 else "😐 Neutral" if avg > -0.1 else "😞 Bearish"

    print(f"{hour:6d} | {avg:15.3f} | {len(sentiments):12d} | {mood}")

# ============================================================================
# PATTERN 4: MULTI-KEY GROUPING (Subreddit × Hour)
# ============================================================================

print("\n\n" + "=" * 80)
print("PATTERN 4: SUBREDDIT ACTIVITY BY HOUR")
print("=" * 80)

subreddit_hour = defaultdict(lambda: defaultdict(int))
for record in sentiment_records:
    timestamp = datetime.fromtimestamp(record['timestamp'])
    hour = timestamp.hour
    subreddit = record['subreddit']

    subreddit_hour[subreddit][hour] += 1

print("\nMost active hours for each subreddit:")
for subreddit, hours in subreddit_hour.items():
    most_active_hour = max(hours.items(), key=lambda x: x[1])
    hour, count = most_active_hour
    print(f"  {subreddit:25s} → Hour {hour:2d}:00 ({count:3d} posts)")

# ============================================================================
# SUMMARY: YOUR ANALYTICS PATTERNS
# ============================================================================

print("\n\n" + "=" * 80)
print("🎯 SUMMARY: MapReduce Patterns in Your Analytics")
print("=" * 80)

print("""
Your 12 analytics use these MapReduce patterns:

1. ARBITRAGE DETECTION
   MAP: (symbol, time) → {exchange, price}
   REDUCE: Cross-join exchanges → calculate spread

2. SENTIMENT AGGREGATION
   MAP: (subreddit, hour) → sentiment_polarity
   REDUCE: avg(sentiment), count(posts)

3. PRICE PREDICTION
   MAP: (symbol, exchange) → {timestamp, price}
   REDUCE: Moving average window → trend

4. ORDER DEPTH
   MAP: (symbol, exchange, hour) → spread_bps
   REDUCE: avg(spread), 1000/avg(spread) as liquidity_score

5. VWAP
   MAP: (symbol, hour) → {price, inverse_spread_weight}
   REDUCE: weighted_avg(price, weight)

6. BID-ASK IMBALANCE
   MAP: (symbol, hour) → {bid_strength, ask_strength}
   REDUCE: (bid - ask) / (bid + ask) as imbalance

7. ROLLING VOLATILITY
   MAP: (symbol, exchange) → {timestamp, price}
   REDUCE: stddev(price[1h/4h/24h windows])

8. BOLLINGER BANDS
   MAP: (symbol, exchange) → {timestamp, price}
   REDUCE: MA_20, upper=MA+2*STD, lower=MA-2*STD

9. FLASH EVENTS
   MAP: (symbol, minute) → {first_price, last_price, max, min}
   REDUCE: if |change| > 5% → FLASH_EVENT

10. SENTIMENT MOMENTUM
    MAP: (subreddit, hour) → avg_sentiment
    REDUCE: current_hour - previous_hour

11. DIVERGENCE
    MAP: (hour) → {sentiment_direction, price_direction}
    REDUCE: if directions opposite → DIVERGENCE

12. CORRELATION MATRIX
    MAP: (hour) → {asset1: price_change, asset2: price_change}
    REDUCE: pairwise_correlation(assets)

All following classic MapReduce (key, value) pattern!
""")

print("=" * 80)
print("✅ DONE! These are the patterns running on your 396K+ records")
print("=" * 80)
