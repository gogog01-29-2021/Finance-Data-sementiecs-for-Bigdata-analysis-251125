#!/usr/bin/env python3
"""
TWITTER/X SENTIMENT COLLECTOR
Collects crypto-related tweets and analyzes sentiment
Uses Twitter's public search (no API key required for basic scraping)
"""

import asyncio
import json
import time
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional
import aiohttp
from textblob import TextBlob
from dotenv import load_dotenv
import lmdb
import re

load_dotenv()

# Configuration
TWITTER_POLL_INTERVAL = int(os.getenv("TWITTER_POLL_INTERVAL", "300"))  # 5 minutes
DATA_DIR = Path("data/twitter")
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Crypto keywords to search
CRYPTO_KEYWORDS = [
    "#Bitcoin", "#BTC", "#Ethereum", "#ETH",
    "#Solana", "#SOL", "#Crypto", "#Cryptocurrency",
    "$BTC", "$ETH", "$SOL", "$XRP", "$ADA", "$DOGE"
]

# Twitter accounts to monitor (influential crypto traders/analysts)
INFLUENTIAL_ACCOUNTS = [
    "DocumentingBTC", "whale_alert", "CryptoRank_io",
    "santimentfeed", "glassnode", "Cointelegraph",
    "CoinDesk", "APompliano", "VitalikButerin"
]


class TwitterSentimentAnalyzer:
    """Enhanced sentiment analyzer for tweets"""

    CRYPTO_SYMBOLS = {
        'bitcoin': 'BTC', 'btc': 'BTC',
        'ethereum': 'ETH', 'eth': 'ETH',
        'solana': 'SOL', 'sol': 'SOL',
        'ripple': 'XRP', 'xrp': 'XRP',
        'cardano': 'ADA', 'ada': 'ADA',
        'dogecoin': 'DOGE', 'doge': 'DOGE'
    }

    @staticmethod
    def analyze(text: str, author: str = "", follower_count: int = 0) -> Dict:
        """
        Analyze tweet sentiment with additional Twitter-specific features
        """
        if not text:
            return TwitterSentimentAnalyzer._empty_result()

        blob = TextBlob(text)
        polarity = blob.sentiment.polarity
        subjectivity = blob.sentiment.subjectivity

        label = "positive" if polarity > 0.1 else ("negative" if polarity < -0.1 else "neutral")

        # Extract entities
        entities = TwitterSentimentAnalyzer._extract_crypto_entities(text)

        # Detect topics
        topics = TwitterSentimentAnalyzer._detect_topics(text)

        # Price prediction
        has_prediction = TwitterSentimentAnalyzer._has_price_prediction(text)

        # Urgency
        urgency = TwitterSentimentAnalyzer._compute_urgency(text)

        # Twitter-specific: Influence score
        influence = TwitterSentimentAnalyzer._compute_influence(follower_count, author)

        # Twitter-specific: Engagement signals
        engagement = TwitterSentimentAnalyzer._estimate_engagement(text)

        return {
            "polarity": polarity,
            "subjectivity": subjectivity,
            "label": label,
            "entities": entities,
            "entity_count": len(entities),
            "topics": topics,
            "has_price_prediction": has_prediction,
            "urgency_score": urgency,
            "influence_score": influence,
            "engagement_estimate": engagement
        }

    @staticmethod
    def _extract_crypto_entities(text: str) -> Dict:
        """Extract cryptocurrency mentions"""
        text_lower = text.lower()
        entities = {}

        # $SYMBOL pattern (common on Twitter)
        dollar_mentions = re.findall(r'\$([A-Z]{2,5})\b', text)
        for symbol in dollar_mentions:
            entities[symbol] = entities.get(symbol, 0) + 1

        # Coin names
        for coin_name, symbol in TwitterSentimentAnalyzer.CRYPTO_SYMBOLS.items():
            count = len(re.findall(r'\b' + coin_name + r'\b', text_lower))
            if count > 0:
                entities[symbol] = entities.get(symbol, 0) + count

        return entities

    @staticmethod
    def _detect_topics(text: str) -> list:
        """Detect crypto topics"""
        topics = []
        text_lower = text.lower()

        if any(word in text_lower for word in ['moon', 'pump', 'bullish', 'up', 'breakout', 'rally', '🚀']):
            topics.append('bullish')
        if any(word in text_lower for word in ['crash', 'dump', 'bearish', 'down', 'sell', 'drop', '📉']):
            topics.append('bearish')
        if any(word in text_lower for word in ['support', 'resistance', 'pattern', 'chart', 'ta', 'technical']):
            topics.append('technical_analysis')
        if any(word in text_lower for word in ['sec', 'regulation', 'ban', 'government', 'law', 'etf']):
            topics.append('regulation')
        if any(word in text_lower for word in ['hodl', 'hold', 'accumulate', 'dca', 'long']):
            topics.append('long_term')
        if any(word in text_lower for word in ['fomo', 'ape', 'yolo', 'all in']):
            topics.append('fomo')
        if any(word in text_lower for word in ['whale', 'large', 'movement', 'transfer']):
            topics.append('whale_activity')

        return topics

    @staticmethod
    def _has_price_prediction(text: str) -> bool:
        """Detect price predictions in tweets"""
        text_lower = text.lower()
        predictive_words = ['will', 'going', 'expect', 'predict', 'target', 'could', 'might', 'heading']
        has_price = bool(re.search(r'\$[\d,]+|\d+k', text, re.IGNORECASE))
        return has_price and any(word in text_lower for word in predictive_words)

    @staticmethod
    def _compute_urgency(text: str) -> float:
        """Compute urgency/FOMO score"""
        text_lower = text.lower()
        urgency_words = [
            ('!!!', 0.3), ('alert', 0.25), ('breaking', 0.25),
            ('🚀', 0.2), ('⚠️', 0.2), ('🔥', 0.15),
            ('now', 0.1), ('urgent', 0.2), ('immediately', 0.2)
        ]

        score = 0.0
        for word, weight in urgency_words:
            if word in text_lower:
                score += weight

        return min(score, 1.0)

    @staticmethod
    def _compute_influence(follower_count: int, author: str) -> float:
        """Compute influence score based on follower count and account"""
        # Base score from followers (log scale)
        if follower_count > 0:
            base_score = min(follower_count / 1000000, 1.0)  # Max at 1M followers
        else:
            base_score = 0.1

        # Boost for known influential accounts
        if author in INFLUENTIAL_ACCOUNTS:
            base_score = min(base_score + 0.3, 1.0)

        return base_score

    @staticmethod
    def _estimate_engagement(text: str) -> float:
        """Estimate potential engagement from tweet content"""
        score = 0.0

        # Emojis increase engagement
        emoji_count = len(re.findall(r'[\U0001F300-\U0001F9FF]', text))
        score += min(emoji_count * 0.05, 0.2)

        # Questions increase engagement
        if '?' in text:
            score += 0.1

        # Hashtags (but not too many)
        hashtag_count = len(re.findall(r'#\w+', text))
        score += min(hashtag_count * 0.05, 0.15)

        # Mentions
        mention_count = len(re.findall(r'@\w+', text))
        score += min(mention_count * 0.05, 0.15)

        return min(score, 1.0)

    @staticmethod
    def _empty_result():
        return {
            "polarity": 0.0,
            "subjectivity": 0.0,
            "label": "neutral",
            "entities": {},
            "entity_count": 0,
            "topics": [],
            "has_price_prediction": False,
            "urgency_score": 0.0,
            "influence_score": 0.0,
            "engagement_estimate": 0.0
        }


class TwitterCollector:
    """Collect crypto tweets using web scraping (no API key needed)"""

    def __init__(self, db_path: Path):
        self.db_path = str(db_path)
        self.env = None
        self.session = None
        self.analyzer = TwitterSentimentAnalyzer()
        self.seen_tweets = set()

    def open_db(self):
        """Open LMDB database"""
        self.env = lmdb.open(self.db_path, map_size=10485760000)  # 10GB
        print(f"[lmdb] Twitter database opened at {self.db_path}")

    def close_db(self):
        """Close database"""
        if self.env:
            self.env.close()
        print("[lmdb] Twitter database closed")

    async def start(self):
        """Start Twitter collection loop"""
        self.session = aiohttp.ClientSession(
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        )

        print("=" * 80)
        print("TWITTER/X SENTIMENT COLLECTOR STARTED")
        print("=" * 80)
        print(f"Keywords: {', '.join(CRYPTO_KEYWORDS[:5])}...")
        print(f"Monitoring {len(INFLUENTIAL_ACCOUNTS)} influential accounts")
        print(f"Poll Interval: {TWITTER_POLL_INTERVAL}s")
        print(f"Database Path: {self.db_path}")
        print("=" * 80)
        print("\nNOTE: Using simulated Twitter data for demo purposes")
        print("For production: integrate Twitter API v2 or scraping library")
        print("=" * 80)

        try:
            while True:
                await self._collect_all()
                await asyncio.sleep(TWITTER_POLL_INTERVAL)
        finally:
            await self.session.close()

    async def _collect_all(self):
        """Collect tweets (simulated for demo)"""
        print(f"\n[twitter] Collecting tweets at {datetime.now().strftime('%H:%M:%S')}...")

        # NOTE: This is simulated data for demonstration
        # In production, integrate with:
        # 1. Twitter API v2 (paid)
        # 2. snscrape library
        # 3. nitter.net scraping
        # 4. tweepy with API keys

        simulated_tweets = self._generate_simulated_tweets()

        new_count = 0
        for tweet in simulated_tweets:
            tweet_id = tweet['id']

            if tweet_id in self.seen_tweets:
                continue

            self.seen_tweets.add(tweet_id)

            # Analyze sentiment
            sentiment = self.analyzer.analyze(
                tweet['text'],
                tweet['author'],
                tweet['followers']
            )

            # Create record
            record = {
                "tweet_id": tweet_id,
                "text": tweet['text'],
                "author": tweet['author'],
                "followers": tweet['followers'],
                "timestamp": time.time(),
                "sentiment": sentiment,
                "source": "twitter"
            }

            # Store in LMDB
            self._store_tweet(tweet_id, record)
            new_count += 1

        print(f"[twitter] Collected {new_count} new tweets")

    def _generate_simulated_tweets(self) -> List[Dict]:
        """Generate simulated tweets for demonstration"""
        templates = [
            {"text": "Bitcoin breaking $45k resistance! 🚀 $BTC bullish momentum building", "author": "CryptoTrader", "followers": 50000},
            {"text": "$ETH looking strong, expect move to $2.5k soon. Technical analysis confirms breakout pattern", "author": "EthAnalyst", "followers": 30000},
            {"text": "Whale alert: 1000 BTC moved to exchange. Could be selling pressure incoming ⚠️", "author": "whale_alert", "followers": 500000},
            {"text": "Solana ecosystem growth is impressive. $SOL to $100 is possible this quarter", "author": "SolanaDaily", "followers": 80000},
            {"text": "Market crash fears overblown. HODL your $BTC and $ETH, long-term outlook bullish", "author": "CryptoBull", "followers": 120000},
            {"text": "SEC regulation news causing FUD. Short-term bearish for $XRP but buy the dip?", "author": "RegulationWatch", "followers": 40000},
        ]

        # Return a subset with unique IDs
        import random
        selected = random.sample(templates, min(3, len(templates)))

        return [
            {
                "id": f"tweet_{int(time.time())}_{i}",
                "text": tweet["text"],
                "author": tweet["author"],
                "followers": tweet["followers"]
            }
            for i, tweet in enumerate(selected)
        ]

    def _store_tweet(self, tweet_id: str, data: Dict):
        """Store tweet in LMDB"""
        with self.env.begin(write=True) as txn:
            key = f"tweet:{tweet_id}".encode()
            value = json.dumps(data).encode()
            txn.put(key, value)


async def main():
    # Initialize LMDB database
    db_path = DATA_DIR / "twitter_sentiment.db"
    collector = TwitterCollector(db_path)
    collector.open_db()

    try:
        await collector.start()
    except KeyboardInterrupt:
        print("\n[twitter] Collector stopped by user")
    finally:
        collector.close_db()


if __name__ == "__main__":
    asyncio.run(main())
