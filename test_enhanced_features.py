#!/usr/bin/env python3
"""
Quick test of enhanced features
"""

import sys
from pathlib import Path

print("=" * 80)
print("TESTING ENHANCED SEMANTIC FEATURES")
print("=" * 80)

# Test 1: Enhanced Sentiment Analyzer
print("\n[Test 1] Enhanced Sentiment Analyzer")
print("-" * 80)

from reddit_sentiment import SentimentAnalyzer

test_texts = [
    "Bitcoin is going to moon! 🚀 $BTC will hit $100k soon!",
    "Sold all my ETH, this crash is terrible",
    "HODL your BTC and ETH, don't panic sell",
    "SEC regulation might affect Ripple and XRP price"
]

analyzer = SentimentAnalyzer()

for text in test_texts:
    result = analyzer.analyze(text)
    print(f"\nText: {text}")
    print(f"  Sentiment: {result['label']} ({result['polarity']:.2f})")
    print(f"  Entities: {result['entities']}")
    print(f"  Topics: {result['topics']}")
    print(f"  Has prediction: {result['has_price_prediction']}")
    print(f"  Urgency: {result['urgency_score']:.2f}")

print("\n✓ Enhanced sentiment analyzer working!")

# Test 2: Data directories
print("\n[Test 2] Checking Data Directories")
print("-" * 80)

data_dirs = [
    Path("data/reddit"),
    Path("data/twitter"),
    Path("data/onchain")
]

for dir_path in data_dirs:
    if dir_path.exists():
        files = list(dir_path.glob("*.db"))
        print(f"✓ {dir_path}: {len(files)} database(s)")
    else:
        print(f"✗ {dir_path}: not found (will be created on first run)")

# Test 3: MongoDB connection
print("\n[Test 3] MongoDB Connection")
print("-" * 80)

try:
    from pymongo import MongoClient
    client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=2000)
    client.server_info()
    print("✓ MongoDB connected successfully")

    db = client['financial_analytics']
    collections = db.list_collection_names()
    print(f"  Collections: {collections}")

    client.close()
except Exception as e:
    print(f"✗ MongoDB connection failed: {e}")
    print("  Make sure MongoDB is running: docker run -d -p 27017:27017 --name mongodb mongo:latest")

# Test 4: Visualization directory
print("\n[Test 4] Visualization Directory")
print("-" * 80)

viz_dir = Path("visualizations")
if viz_dir.exists():
    files = list(viz_dir.glob("*.png"))
    print(f"✓ Visualization directory exists: {len(files)} PNG file(s)")
    for f in files:
        print(f"    - {f.name}")
else:
    viz_dir.mkdir(exist_ok=True)
    print(f"✓ Created visualization directory: {viz_dir.absolute()}")

print("\n" + "=" * 80)
print("ENHANCED FEATURES TEST COMPLETE")
print("=" * 80)
print("\nNext steps:")
print("1. Ensure MongoDB is running")
print("2. Run data collectors to gather data")
print("3. Run analytics pipeline")
print("4. Generate visualizations")
print("\nOr run everything at once:")
print("  python run_enhanced_pipeline.py --quick")
print("=" * 80)
