#!/usr/bin/env python3
"""
REDDIT SENTIMENT COLLECTOR
Polls Reddit API every 60 seconds for posts and comments from finance-related subreddits
Stores data in RocksDB with column families: posts, comments, sentiment
"""

import asyncio
import json
import time
import os
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional
import aiohttp
from textblob import TextBlob
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuration
REDDIT_POLL_INTERVAL = int(os.getenv("REDDIT_POLL_INTERVAL", "60"))
DATA_DIR = Path("data/reddit")
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Try to import key-value database backends in order of preference
DB_BACKEND = None
DB_TYPE = None

try:
    import rocksdb
    DB_BACKEND = "rocksdb"
    DB_TYPE = "RocksDB"
    print("[db] Using RocksDB backend")
except ImportError:
    try:
        import plyvel
        DB_BACKEND = "leveldb"
        DB_TYPE = "LevelDB"
        print("[db] RocksDB not available, using LevelDB")
    except ImportError:
        try:
            import lmdb
            DB_BACKEND = "lmdb"
            DB_TYPE = "LMDB"
            print("[db] Using LMDB backend (Windows-compatible)")
        except ImportError:
            raise ImportError("No key-value database available. Install one of: python-rocksdb, plyvel, lmdb")

# Subreddits to monitor
SUBREDDITS = [
    "Economics",
    "stockMarket",
    "investing",
    "stocks",
    "BitcoinKR",
    "KoreanFinance",
    "CryptoCurrency",
    "Bitcoin",
    "SatoshiStreetBets",
]

# Reddit API Configuration (using public JSON endpoints - no auth required)
REDDIT_BASE = "https://www.reddit.com"


class SentimentAnalyzer:
    """Enhanced sentiment analyzer with entity extraction and topic detection"""

    # Common crypto symbols to look for
    CRYPTO_SYMBOLS = {
        'bitcoin': 'BTC', 'btc': 'BTC',
        'ethereum': 'ETH', 'eth': 'ETH',
        'solana': 'SOL', 'sol': 'SOL',
        'ripple': 'XRP', 'xrp': 'XRP',
        'cardano': 'ADA', 'ada': 'ADA',
        'dogecoin': 'DOGE', 'doge': 'DOGE'
    }

    @staticmethod
    def analyze(text: str) -> Dict:
        """
        Enhanced sentiment analysis with semantic features
        Returns: {
            'polarity': float (-1 to 1, negative to positive),
            'subjectivity': float (0 to 1, objective to subjective),
            'label': str ('positive', 'negative', 'neutral'),
            'entities': dict - crypto mentions {'BTC': 3, 'ETH': 1},
            'entity_count': int,
            'topics': list - detected topics,
            'has_price_prediction': bool,
            'urgency_score': float
        }
        """
        if not text:
            return {
                "polarity": 0.0,
                "subjectivity": 0.0,
                "label": "neutral",
                "entities": {},
                "entity_count": 0,
                "topics": [],
                "has_price_prediction": False,
                "urgency_score": 0.0
            }

        blob = TextBlob(text)
        polarity = blob.sentiment.polarity
        subjectivity = blob.sentiment.subjectivity

        # Classify polarity
        if polarity > 0.1:
            label = "positive"
        elif polarity < -0.1:
            label = "negative"
        else:
            label = "neutral"

        # NEW: Extract crypto entities
        entities = SentimentAnalyzer._extract_crypto_entities(text)

        # NEW: Detect topics
        topics = SentimentAnalyzer._detect_topics(text)

        # NEW: Detect price predictions
        has_prediction = SentimentAnalyzer._has_price_prediction(text)

        # NEW: Compute urgency
        urgency = SentimentAnalyzer._compute_urgency(text)

        return {
            "polarity": polarity,
            "subjectivity": subjectivity,
            "label": label,
            "entities": entities,
            "entity_count": len(entities),
            "topics": topics,
            "has_price_prediction": has_prediction,
            "urgency_score": urgency
        }

    @staticmethod
    def _extract_crypto_entities(text: str) -> Dict:
        """Extract cryptocurrency mentions and count them"""
        import re
        text_lower = text.lower()
        entities = {}

        # Look for $SYMBOL pattern
        dollar_mentions = re.findall(r'\$([A-Z]{2,5})\b', text)
        for symbol in dollar_mentions:
            entities[symbol] = entities.get(symbol, 0) + 1

        # Look for coin names
        for coin_name, symbol in SentimentAnalyzer.CRYPTO_SYMBOLS.items():
            count = len(re.findall(r'\b' + coin_name + r'\b', text_lower))
            if count > 0:
                entities[symbol] = entities.get(symbol, 0) + count

        return entities

    @staticmethod
    def _detect_topics(text: str) -> list:
        """Detect crypto-related topics/themes"""
        topics = []
        text_lower = text.lower()

        if any(word in text_lower for word in ['moon', 'pump', 'bullish', 'up', 'breakout', 'rally']):
            topics.append('bullish')
        if any(word in text_lower for word in ['crash', 'dump', 'bearish', 'down', 'sell', 'drop']):
            topics.append('bearish')
        if any(word in text_lower for word in ['support', 'resistance', 'pattern', 'chart', 'analysis']):
            topics.append('technical_analysis')
        if any(word in text_lower for word in ['sec', 'regulation', 'ban', 'government', 'law']):
            topics.append('regulation')
        if any(word in text_lower for word in ['hodl', 'hold', 'accumulate', 'dca', 'long-term']):
            topics.append('long_term')
        if any(word in text_lower for word in ['fomo', 'last chance', 'dont miss', 'hurry', 'now']):
            topics.append('fomo')

        return topics

    @staticmethod
    def _has_price_prediction(text: str) -> bool:
        """Detect if text contains price predictions"""
        import re
        text_lower = text.lower()

        predictive_words = [
            'will', 'going to', 'expect', 'predict', 'target',
            'could reach', 'might hit', 'heading to', 'next stop'
        ]

        has_price = bool(re.search(r'\$\d+|\d+k', text, re.IGNORECASE))
        return has_price and any(word in text_lower for word in predictive_words)

    @staticmethod
    def _compute_urgency(text: str) -> float:
        """Compute urgency/FOMO score (0-1)"""
        text_lower = text.lower()

        urgency_words = [
            ('!!!', 0.3), ('🚀', 0.2), ('💎', 0.15), ('🔥', 0.15),
            ('now', 0.1), ('urgent', 0.2), ('immediately', 0.2),
            ('fomo', 0.2), ('last chance', 0.3), ('dont miss', 0.25),
            ('breaking', 0.15), ('alert', 0.15)
        ]

        score = 0.0
        for word, weight in urgency_words:
            if word in text_lower:
                score += weight

        return min(score, 1.0)


class KeyValueStore:
    """Multi-backend key-value store: RocksDB → LevelDB → LMDB"""

    def __init__(self, db_path: Path):
        self.db_path = str(db_path)
        self.db = None
        self.env = None  # For LMDB
        self.backend = DB_BACKEND

    def open(self):
        """Open database based on available backend"""
        if self.backend == "rocksdb":
            try:
                import rocksdb as rdb
                opts = rdb.Options()
                opts.create_if_missing = True
                self.db = rdb.DB(self.db_path, opts)
                print(f"[rocksdb] Database opened at {self.db_path}")
            except ImportError:
                raise ImportError("RocksDB backend selected but python-rocksdb not installed")
        elif self.backend == "leveldb":
            try:
                import plyvel
                self.db = plyvel.DB(self.db_path, create_if_missing=True)
                print(f"[leveldb] Database opened at {self.db_path}")
            except ImportError:
                raise ImportError("LevelDB backend selected but plyvel not installed")
        elif self.backend == "lmdb":
            try:
                import lmdb as lmdb_lib
                self.env = lmdb_lib.open(self.db_path, map_size=10485760000)  # 10GB
                print(f"[lmdb] Database opened at {self.db_path}")
            except ImportError:
                raise ImportError("LMDB backend selected but lmdb not installed")

    def put(self, key: str, value: Dict):
        """Store key-value pair"""
        key_bytes = key.encode()
        value_bytes = json.dumps(value).encode()

        if self.backend == "rocksdb" or self.backend == "leveldb":
            self.db.put(key_bytes, value_bytes)
        elif self.backend == "lmdb":
            with self.env.begin(write=True) as txn:
                txn.put(key_bytes, value_bytes)

    def get(self, key: str) -> Optional[Dict]:
        """Get value by key"""
        key_bytes = key.encode()

        if self.backend == "rocksdb" or self.backend == "leveldb":
            value = self.db.get(key_bytes)
            if value:
                return json.loads(value.decode())
        elif self.backend == "lmdb":
            with self.env.begin() as txn:
                value = txn.get(key_bytes)
                if value:
                    return json.loads(value.decode())
        return None

    def put_post(self, post_id: str, data: Dict):
        """Store Reddit post"""
        self.put(f"post:{post_id}", data)

    def put_comment(self, comment_id: str, data: Dict):
        """Store Reddit comment"""
        self.put(f"comment:{comment_id}", data)

    def put_sentiment(self, item_id: str, sentiment_data: Dict):
        """Store sentiment analysis result"""
        self.put(f"sentiment:{item_id}", sentiment_data)

    def get_post(self, post_id: str) -> Optional[Dict]:
        """Get post by ID"""
        return self.get(f"post:{post_id}")

    def close(self):
        """Close database"""
        if self.backend == "lmdb" and self.env:
            self.env.close()
        elif self.db:
            del self.db
        print(f"[{self.backend}] Database closed")


class RedditCollector:
    """Collect posts and comments from Reddit subreddits"""

    def __init__(self, subreddits: List[str], db_store: KeyValueStore):
        self.subreddits = subreddits
        self.db = db_store
        self.session = None
        self.analyzer = SentimentAnalyzer()
        self.seen_posts = set()
        self.seen_comments = set()

    async def start(self):
        """Start Reddit collection loop"""
        self.session = aiohttp.ClientSession(
            headers={"User-Agent": "FinancialDataPipeline/1.0"}
        )

        print("=" * 80)
        print(f"REDDIT SENTIMENT COLLECTOR STARTED ({DB_TYPE})")
        print("=" * 80)
        print(f"Database Backend: {DB_TYPE}")
        print(f"Subreddits: {', '.join(self.subreddits)}")
        print(f"Poll Interval: {REDDIT_POLL_INTERVAL}s")
        print(f"Database Path: {self.db.db_path}")
        print("=" * 80)

        try:
            while True:
                await self._collect_all()
                await asyncio.sleep(REDDIT_POLL_INTERVAL)
        finally:
            await self.session.close()

    async def _collect_all(self):
        """Collect from all subreddits"""
        tasks = [self._collect_subreddit(sub) for sub in self.subreddits]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Count successful collections
        success_count = sum(1 for r in results if not isinstance(r, Exception))
        print(f"\n[reddit] Collected from {success_count}/{len(self.subreddits)} subreddits")

    async def _collect_subreddit(self, subreddit: str):
        """Collect posts and comments from a single subreddit"""
        try:
            # Fetch new posts
            url = f"{REDDIT_BASE}/r/{subreddit}/new.json?limit=25"

            async with self.session.get(url) as resp:
                if resp.status != 200:
                    print(f"[reddit:{subreddit}] Error {resp.status}")
                    return

                data = await resp.json()
                posts = data.get("data", {}).get("children", [])

                new_posts = 0
                new_comments = 0

                for post_wrapper in posts:
                    post_data = post_wrapper.get("data", {})
                    post_id = post_data.get("id")

                    # Skip if already seen
                    if post_id in self.seen_posts:
                        continue

                    self.seen_posts.add(post_id)

                    # Extract post information
                    post_info = {
                        "id": post_id,
                        "subreddit": subreddit,
                        "title": post_data.get("title", ""),
                        "selftext": post_data.get("selftext", ""),
                        "author": post_data.get("author", ""),
                        "score": post_data.get("score", 0),
                        "upvote_ratio": post_data.get("upvote_ratio", 0),
                        "num_comments": post_data.get("num_comments", 0),
                        "created_utc": post_data.get("created_utc", 0),
                        "url": post_data.get("url", ""),
                        "permalink": post_data.get("permalink", ""),
                        "collected_at": time.time(),
                    }

                    # Analyze sentiment
                    combined_text = f"{post_info['title']} {post_info['selftext']}"
                    sentiment = self.analyzer.analyze(combined_text)

                    sentiment_info = {
                        "item_id": post_id,
                        "item_type": "post",
                        "subreddit": subreddit,
                        "text": combined_text[:500],  # First 500 chars
                        "sentiment": sentiment,
                        "timestamp": time.time(),
                    }

                    # Store in RocksDB
                    self.db.put_post(post_id, post_info)
                    self.db.put_sentiment(post_id, sentiment_info)
                    new_posts += 1

                    # Fetch comments for this post
                    comments_collected = await self._collect_comments(subreddit, post_id)
                    new_comments += comments_collected

                if new_posts > 0 or new_comments > 0:
                    print(f"[reddit:{subreddit}] New posts: {new_posts}, New comments: {new_comments}")

        except Exception as e:
            print(f"[reddit:{subreddit}] Collection error: {e}")

    async def _collect_comments(self, subreddit: str, post_id: str, limit: int = 10) -> int:
        """Collect comments from a post"""
        try:
            url = f"{REDDIT_BASE}/r/{subreddit}/comments/{post_id}.json?limit={limit}"

            async with self.session.get(url) as resp:
                if resp.status != 200:
                    return 0

                data = await resp.json()

                # Reddit returns [post_data, comments_data]
                if len(data) < 2:
                    return 0

                comments_listing = data[1].get("data", {}).get("children", [])
                new_count = 0

                for comment_wrapper in comments_listing:
                    comment_data = comment_wrapper.get("data", {})
                    comment_id = comment_data.get("id")

                    # Skip if not a comment or already seen
                    if not comment_id or comment_id in self.seen_comments:
                        continue

                    self.seen_comments.add(comment_id)

                    # Extract comment information
                    comment_text = comment_data.get("body", "")
                    if not comment_text or comment_text == "[deleted]" or comment_text == "[removed]":
                        continue

                    comment_info = {
                        "id": comment_id,
                        "post_id": post_id,
                        "subreddit": subreddit,
                        "body": comment_text,
                        "author": comment_data.get("author", ""),
                        "score": comment_data.get("score", 0),
                        "created_utc": comment_data.get("created_utc", 0),
                        "collected_at": time.time(),
                    }

                    # Analyze sentiment
                    sentiment = self.analyzer.analyze(comment_text)

                    sentiment_info = {
                        "item_id": comment_id,
                        "item_type": "comment",
                        "post_id": post_id,
                        "subreddit": subreddit,
                        "text": comment_text[:500],  # First 500 chars
                        "sentiment": sentiment,
                        "timestamp": time.time(),
                    }

                    # Store in RocksDB
                    self.db.put_comment(comment_id, comment_info)
                    self.db.put_sentiment(comment_id, sentiment_info)
                    new_count += 1

                return new_count

        except Exception as e:
            print(f"[reddit:{subreddit}:{post_id}] Comments error: {e}")
            return 0


async def main():
    # Initialize key-value database (auto-detects backend)
    db_path = DATA_DIR / f"reddit_sentiment_{DB_BACKEND}.db"
    db_store = KeyValueStore(db_path)
    db_store.open()

    try:
        # Start collector
        collector = RedditCollector(SUBREDDITS, db_store)
        await collector.start()

    except KeyboardInterrupt:
        print("\n[reddit] Collector stopped by user")
    finally:
        db_store.close()


if __name__ == "__main__":
    asyncio.run(main())
