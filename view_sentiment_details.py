#!/usr/bin/env python3
"""
VIEW SENTIMENT CORRELATION DETAILS
Shows both raw Reddit messages and aggregated analytics results
"""
import lmdb
import json
from datetime import datetime
from pymongo import MongoClient
from collections import defaultdict

print("=" * 80)
print("SENTIMENT CORRELATION DETAIL VIEWER")
print("=" * 80)

# Connect to MongoDB (Layer 3 - Aggregated results)
mongo_client = MongoClient("mongodb://localhost:27017")
db = mongo_client["financial_analytics"]

# Get one aggregated record
agg_record = db.sentiment_correlations.find_one({"subreddit": "Economics"})

if not agg_record:
    print("No aggregated records found")
    exit()

print(f"\nAGGREGATED RESULT (MongoDB):")
print(f"   Period: {agg_record['period_start']} to {agg_record['period_end']}")
print(f"   Subreddit: {agg_record['subreddit']}")
print(f"   Symbol: {agg_record.get('symbol', 'N/A')}")
print(f"   avg_sentiment: {agg_record['avg_sentiment']:.4f}")
print(f"   post_count: {agg_record['post_count']}")
print(f"   avg_price: ${agg_record.get('avg_price', 0):,.2f}")
print(f"   price_volatility: {agg_record.get('price_volatility', 0):.4f}")

# Open LMDB (Layer 1 - Raw messages)
env = lmdb.open('data/reddit/reddit_sentiment_lmdb.db', readonly=True)

print(f"\nRAW MESSAGES THAT CREATED THIS AGGREGATE:")
print("-" * 80)

# Get the time period
period_start = agg_record['period_start'].timestamp()
period_end = agg_record['period_end'].timestamp()
target_subreddit = agg_record['subreddit']

with env.begin() as txn:
    cursor = txn.cursor()
    messages = []

    for key, value in cursor:
        if key.startswith(b'sentiment:'):
            record = json.loads(value.decode())

            # Filter by subreddit and time period
            if (record['subreddit'] == target_subreddit and
                period_start <= record['timestamp'] <= period_end):
                messages.append(record)

    print(f"\nFound {len(messages)} messages in this time period\n")

    # Show first 10 messages
    for i, msg in enumerate(messages[:10], 1):
        text = msg.get('text', '')[:100]
        safe_text = text.encode('ascii', 'ignore').decode('ascii')

        print(f"{i}. Sentiment: {msg['sentiment']['polarity']:+.3f} ({msg['sentiment']['label']})")
        print(f"   Text: \"{safe_text}...\"")
        print(f"   Time: {datetime.fromtimestamp(msg['timestamp'])}")
        print()

    if len(messages) > 10:
        print(f"... and {len(messages) - 10} more messages")

    # Calculate average to verify
    avg_sentiment = sum(m['sentiment']['polarity'] for m in messages) / len(messages) if messages else 0
    print(f"\nVerification: Calculated avg sentiment = {avg_sentiment:.4f}")
    print(f"  MongoDB shows: {agg_record['avg_sentiment']:.4f}")
    print(f"  Match: {'YES' if abs(avg_sentiment - agg_record['avg_sentiment']) < 0.01 else 'NO'}")

env.close()
mongo_client.close()

print("\n" + "=" * 80)
print("HOW TO USE:")
print("=" * 80)
print("""
1. Raw messages are in LMDB: data/reddit/reddit_sentiment_lmdb.db
   - Contains full text of each Reddit post/comment
   - Each has individual sentiment score

2. Aggregated analytics in MongoDB: financial_analytics.sentiment_correlations
   - Groups messages by (time, subreddit, symbol)
   - Calculates averages across multiple messages
   - Joins with price data

3. The 'post_count' field tells you how many messages were aggregated
""")
print("=" * 80)
