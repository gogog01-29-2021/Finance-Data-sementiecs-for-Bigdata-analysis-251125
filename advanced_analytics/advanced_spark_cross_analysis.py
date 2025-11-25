#!/usr/bin/env python3
"""
ADVANCED CROSS-SOURCE SPARK ANALYTICS
Deep analysis of relationships between text (news) and price data using PySpark:

1. Word-Price Temporal Analysis:
   - Word frequency before/after price movements
   - Price movements before/after specific words appear
   - Lagged correlations (word -> price, price -> word)

2. Pattern Velocity Analysis:
   - How fast words spread after price moves
   - How fast prices move after certain words appear
   - Momentum and acceleration of patterns

3. Bidirectional Causality (Granger-style):
   - Does word X predict price movement?
   - Does price movement predict word X appearance?
   - Lead-lag relationships

4. Key-Value MapReduce Style Analysis:
   - (word, date) -> count
   - (word, price_direction) -> correlation
   - (word, lagged_return) -> predictive_power

5. Sentiment-Price Dynamics:
   - Sentiment momentum vs price momentum
   - Sentiment reversal patterns
   - Cross-asset sentiment spillover

6. PageRank Algorithm for Word Importance:
   - Build word co-occurrence graph
   - Apply PageRank to find influential words
   - Word influence network analysis

7. Formal vs Informal Language Analysis:
   - Headlines (formal) vs Social Media (informal)
   - Compare top words across sources
   - Sentiment difference analysis
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv
import json
import re

# Fix Windows encoding
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

load_dotenv()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.ml.feature import Tokenizer, StopWordsRemover
import pandas as pd
import numpy as np
from collections import Counter, defaultdict

# Directories
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data" / "batch"
OUTPUT_DIR = BASE_DIR / "advanced_analytics" / "spark_output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Extended stop words for financial text
STOP_WORDS = set([
    'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
    'of', 'with', 'by', 'from', 'as', 'is', 'was', 'are', 'were', 'been',
    'be', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'could',
    'should', 'may', 'might', 'must', 'shall', 'can', 'need', 'dare', 'ought',
    'used', 'it', 'its', 'this', 'that', 'these', 'those', 'i', 'you', 'he',
    'she', 'we', 'they', 'what', 'which', 'who', 'whom', 'when', 'where',
    'why', 'how', 'all', 'each', 'every', 'both', 'few', 'more', 'most',
    'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same',
    'so', 'than', 'too', 'very', 'just', 'after', 'before', 'over', 'under',
    'says', 'said', 'new', 'year', 'years', 'also', 'like', 'get', 'got',
    'going', 'one', 'two', 'first', 'last', 'now', 'still', 'even', 'back',
    'well', 'way', 'much', 'many', 'think', 'know', 'see', 'come', 'take'
])


def create_spark_session():
    """Create Spark session"""
    print("\n" + "="*60)
    print("INITIALIZING SPARK SESSION")
    print("="*60)

    spark = SparkSession.builder \
        .appName("AdvancedCrossSourceAnalytics") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print(f"Spark version: {spark.version}")

    return spark


# ============================================================
# PAGERANK ALGORITHM FOR WORD IMPORTANCE
# ============================================================

class WordPageRank:
    """
    Apply Google's PageRank algorithm to find influential words.
    Build a word co-occurrence graph where:
    - Nodes = words
    - Edges = co-occurrence in same headline (weighted by frequency)
    """

    def __init__(self, spark, news_df):
        self.spark = spark
        self.news_df = news_df

    def build_word_graph(self):
        """Build word co-occurrence graph"""
        print("\n[PageRank] Building word co-occurrence graph...")

        # Tokenize headlines
        tokenizer = Tokenizer(inputCol="headline", outputCol="words")
        news_tokenized = tokenizer.transform(self.news_df)

        remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
        news_tokenized = remover.transform(news_tokenized)

        # Get words per headline
        headlines_words = news_tokenized.select("filtered_words").collect()

        # Build co-occurrence matrix
        cooccurrence = defaultdict(lambda: defaultdict(int))
        word_freq = Counter()

        for row in headlines_words:
            words = [w.lower() for w in row['filtered_words']
                     if len(w) > 2 and w.lower() not in STOP_WORDS]
            words = list(set(words))  # Unique words per headline

            for word in words:
                word_freq[word] += 1

            # Co-occurrence (undirected edges)
            for i, w1 in enumerate(words):
                for w2 in words[i+1:]:
                    cooccurrence[w1][w2] += 1
                    cooccurrence[w2][w1] += 1

        print(f"  Unique words: {len(word_freq)}")
        print(f"  Co-occurrence pairs: {sum(len(v) for v in cooccurrence.values()) // 2}")

        self.cooccurrence = cooccurrence
        self.word_freq = word_freq

        return cooccurrence

    def calculate_pagerank(self, damping=0.85, iterations=20, top_n=100):
        """Calculate PageRank scores for words"""
        print(f"\n[PageRank] Calculating PageRank (damping={damping}, iterations={iterations})...")

        # Filter to top words by frequency
        top_words = [w for w, _ in self.word_freq.most_common(500)]

        # Initialize PageRank
        n = len(top_words)
        pr = {word: 1.0 / n for word in top_words}

        # Calculate outgoing links count
        out_degree = {}
        for word in top_words:
            out_degree[word] = sum(1 for w in self.cooccurrence[word] if w in top_words)

        # Iterate
        for iteration in range(iterations):
            new_pr = {}
            for word in top_words:
                # Sum of incoming PageRank
                incoming_pr = 0
                for other_word in top_words:
                    if word in self.cooccurrence[other_word] and out_degree[other_word] > 0:
                        # Weight by co-occurrence strength
                        weight = self.cooccurrence[other_word][word]
                        total_out = sum(self.cooccurrence[other_word][w]
                                        for w in self.cooccurrence[other_word] if w in top_words)
                        if total_out > 0:
                            incoming_pr += pr[other_word] * (weight / total_out)

                new_pr[word] = (1 - damping) / n + damping * incoming_pr

            pr = new_pr

            if iteration % 5 == 0:
                print(f"    Iteration {iteration}: max_pr = {max(pr.values()):.6f}")

        # Sort and save results
        pagerank_results = sorted(pr.items(), key=lambda x: x[1], reverse=True)

        results_df = pd.DataFrame(pagerank_results[:top_n], columns=['word', 'pagerank'])
        results_df['frequency'] = results_df['word'].map(self.word_freq)
        results_df['rank'] = range(1, len(results_df) + 1)

        results_df.to_csv(OUTPUT_DIR / "word_pagerank.csv", index=False)

        print(f"\n  Top 20 words by PageRank (influential words):")
        for i, row in results_df.head(20).iterrows():
            print(f"    {row['rank']}. {row['word']}: PR={row['pagerank']:.6f}, freq={row['frequency']}")

        return results_df

    def find_word_communities(self, top_n=100):
        """Find word communities using simple clustering"""
        print("\n[PageRank] Finding word communities...")

        top_words = [w for w, _ in self.word_freq.most_common(top_n)]

        # Build adjacency for top words
        edges = []
        for w1 in top_words:
            for w2, weight in self.cooccurrence[w1].items():
                if w2 in top_words and w1 < w2:  # Undirected
                    edges.append({
                        'source': w1,
                        'target': w2,
                        'weight': weight
                    })

        edges_df = pd.DataFrame(edges)
        edges_df = edges_df.sort_values('weight', ascending=False)
        edges_df.to_csv(OUTPUT_DIR / "word_graph_edges.csv", index=False)

        print(f"  Saved {len(edges_df)} word connections")
        print(f"\n  Strongest word connections:")
        for _, row in edges_df.head(15).iterrows():
            print(f"    {row['source']} <-> {row['target']}: {row['weight']}")

        return edges_df


# ============================================================
# SOCIAL MEDIA DATA GENERATOR (YouTube/Reddit style)
# ============================================================

class SocialMediaDataGenerator:
    """
    Generate synthetic social media comments data for comparison.
    In production, this would fetch from YouTube API, Reddit API, or Twitter API.
    """

    def __init__(self, news_df):
        self.news_df = news_df

    def generate_youtube_comments(self, n_comments=10000):
        """Generate synthetic YouTube-style comments"""
        print("\n[SocialMedia] Generating YouTube-style comments...")

        # Informal templates (typical YouTube finance video comments)
        positive_templates = [
            "🚀🚀🚀 {stock} to the moon!!!",
            "just bought more {stock} let's gooo",
            "this is gonna be huge, {stock} will 10x easy",
            "diamond hands 💎🙌 holding {stock} forever",
            "lol bears are so wrong about {stock}",
            "best stock ever, {stock} is the future",
            "who else is buying the dip? {stock} gang",
            "my portfolio up 500% thanks to {stock} 😂",
            "told yall {stock} was gonna pump",
            "{stock} making me rich fr fr",
            "apes together strong 🦍 {stock}",
            "never selling my {stock} shares, generational wealth",
        ]

        negative_templates = [
            "rip to everyone who bought {stock} at the top",
            "{stock} is trash, worst investment ever",
            "why did i buy {stock}?? down so bad rn",
            "lmao {stock} bagholders in shambles",
            "sold all my {stock}, this thing is done",
            "{stock} scam, dont buy this garbage",
            "paper hands saved me from {stock} disaster",
            "who else got rekt by {stock}? 😭",
            "{stock} going to zero, get out now",
            "biggest regret buying {stock} smh",
        ]

        neutral_templates = [
            "what do yall think about {stock}?",
            "should i buy {stock} or wait?",
            "anyone still holding {stock}?",
            "{stock} looking interesting ngl",
            "idk about {stock} tbh, mixed signals",
            "waiting for {stock} to dip more",
            "whats the PT for {stock}?",
            "new to investing, is {stock} good?",
        ]

        stocks = ['AAPL', 'TSLA', 'NVDA', 'GOOGL', 'AMZN', 'MSFT', 'META', 'GME', 'AMC', 'SPY']
        stock_names = {
            'AAPL': 'Apple', 'TSLA': 'Tesla', 'NVDA': 'Nvidia',
            'GOOGL': 'Google', 'AMZN': 'Amazon', 'MSFT': 'Microsoft',
            'META': 'Meta', 'GME': 'GameStop', 'AMC': 'AMC', 'SPY': 'SPY'
        }

        np.random.seed(42)
        comments = []

        # Generate date range
        dates = pd.date_range(start='2023-01-01', end='2024-11-25', freq='D')

        for _ in range(n_comments):
            sentiment_roll = np.random.random()

            if sentiment_roll < 0.4:
                template = np.random.choice(positive_templates)
                sentiment = 'positive'
                sentiment_score = np.random.uniform(0.3, 0.9)
            elif sentiment_roll < 0.7:
                template = np.random.choice(negative_templates)
                sentiment = 'negative'
                sentiment_score = np.random.uniform(-0.9, -0.3)
            else:
                template = np.random.choice(neutral_templates)
                sentiment = 'neutral'
                sentiment_score = np.random.uniform(-0.2, 0.2)

            stock = np.random.choice(stocks)
            comment = template.format(stock=np.random.choice([stock, stock_names[stock]]))

            comments.append({
                'date': np.random.choice(dates),
                'text': comment,
                'sentiment': sentiment,
                'sentiment_score': sentiment_score,
                'stock_mentioned': stock,
                'source': 'youtube',
                'likes': int(np.random.exponential(50)),
                'replies': int(np.random.exponential(5))
            })

        df = pd.DataFrame(comments)
        df.to_csv(OUTPUT_DIR / "youtube_comments.csv", index=False)
        print(f"  Generated {len(df)} YouTube-style comments")

        return df

    def generate_reddit_comments(self, n_comments=10000):
        """Generate synthetic Reddit-style comments (WSB style)"""
        print("\n[SocialMedia] Generating Reddit-style comments...")

        # WSB-style templates
        positive_templates = [
            "DD: Why {stock} is going to $1000 🚀",
            "YOLO'd my life savings into {stock} calls, cant go tits up",
            "{stock} squeeze incoming, shorts are fukd",
            "positions: 100x {stock} 500c 1/20, wish me luck retards",
            "if you're not buying {stock} rn you hate money",
            "wife's boyfriend approved my {stock} purchase",
            "{stock} tendies printing 🍗🍗🍗",
            "apes dont sell 💎🙌 {stock} forever",
            "this is financial advice, buy {stock}",
            "bull thesis on {stock}: trust me bro",
        ]

        negative_templates = [
            "loss porn: down 90% on {stock} calls",
            "{stock} puts printing, bears eating good tonight",
            "inverse WSB worked, sold {stock} at the top",
            "GUH moment on {stock}, RIP my portfolio",
            "{stock} rug pull, everyone got rekt",
            "sold my {stock} bags, finally free",
            "imagine buying {stock} at ATH lmaooo",
            "{stock} going to $0, get out while you can",
        ]

        neutral_templates = [
            "what's the play on {stock}?",
            "{stock} IV too high for options rn",
            "theta gang on {stock}, selling covered calls",
            "anyone got DD on {stock}?",
            "{stock} consolidating, waiting for breakout",
            "paper trading {stock} before going in",
        ]

        stocks = ['AAPL', 'TSLA', 'NVDA', 'GOOGL', 'AMZN', 'MSFT', 'META', 'GME', 'AMC', 'SPY', 'PLTR', 'BB']

        np.random.seed(43)
        comments = []
        dates = pd.date_range(start='2023-01-01', end='2024-11-25', freq='D')

        for _ in range(n_comments):
            sentiment_roll = np.random.random()

            if sentiment_roll < 0.45:
                template = np.random.choice(positive_templates)
                sentiment = 'positive'
                sentiment_score = np.random.uniform(0.3, 0.9)
            elif sentiment_roll < 0.75:
                template = np.random.choice(negative_templates)
                sentiment = 'negative'
                sentiment_score = np.random.uniform(-0.9, -0.3)
            else:
                template = np.random.choice(neutral_templates)
                sentiment = 'neutral'
                sentiment_score = np.random.uniform(-0.2, 0.2)

            stock = np.random.choice(stocks)
            comment = template.format(stock=stock)

            comments.append({
                'date': np.random.choice(dates),
                'text': comment,
                'sentiment': sentiment,
                'sentiment_score': sentiment_score,
                'stock_mentioned': stock,
                'source': 'reddit',
                'upvotes': int(np.random.exponential(100)),
                'comments': int(np.random.exponential(20))
            })

        df = pd.DataFrame(comments)
        df.to_csv(OUTPUT_DIR / "reddit_comments.csv", index=False)
        print(f"  Generated {len(df)} Reddit-style comments")

        return df


# ============================================================
# FORMAL VS INFORMAL LANGUAGE ANALYSIS
# ============================================================

class FormalInformalAnalysis:
    """Compare formal (news headlines) vs informal (social media) language"""

    def __init__(self, spark, news_df, youtube_df, reddit_df):
        self.spark = spark
        self.news_df = news_df
        self.youtube_df = youtube_df
        self.reddit_df = reddit_df

    def tokenize_text(self, text):
        """Simple tokenization"""
        words = re.findall(r'\b[a-zA-Z]{3,}\b', text.lower())
        return [w for w in words if w not in STOP_WORDS]

    def compare_top_words(self, top_n=50):
        """Compare top words across sources"""
        print("\n[FormalInformal] Comparing top words across sources...")

        # News headlines (formal)
        news_words = Counter()
        for headline in self.news_df['headline']:
            news_words.update(self.tokenize_text(str(headline)))

        # YouTube comments (informal)
        youtube_words = Counter()
        for comment in self.youtube_df['text']:
            youtube_words.update(self.tokenize_text(str(comment)))

        # Reddit comments (very informal)
        reddit_words = Counter()
        for comment in self.reddit_df['text']:
            reddit_words.update(self.tokenize_text(str(comment)))

        # Get top words from each source
        news_top = dict(news_words.most_common(top_n))
        youtube_top = dict(youtube_words.most_common(top_n))
        reddit_top = dict(reddit_words.most_common(top_n))

        # Compare
        all_words = set(news_top.keys()) | set(youtube_top.keys()) | set(reddit_top.keys())

        comparison = []
        for word in all_words:
            comparison.append({
                'word': word,
                'news_count': news_top.get(word, 0),
                'news_rank': list(news_top.keys()).index(word) + 1 if word in news_top else -1,
                'youtube_count': youtube_top.get(word, 0),
                'youtube_rank': list(youtube_top.keys()).index(word) + 1 if word in youtube_top else -1,
                'reddit_count': reddit_top.get(word, 0),
                'reddit_rank': list(reddit_top.keys()).index(word) + 1 if word in reddit_top else -1,
            })

        comparison_df = pd.DataFrame(comparison)
        comparison_df['total_count'] = comparison_df['news_count'] + comparison_df['youtube_count'] + comparison_df['reddit_count']
        comparison_df = comparison_df.sort_values('total_count', ascending=False)

        comparison_df.to_csv(OUTPUT_DIR / "word_comparison_sources.csv", index=False)

        # Find source-specific words
        news_only = [w for w in news_top if w not in youtube_top and w not in reddit_top]
        social_only = [w for w in (set(youtube_top) | set(reddit_top)) if w not in news_top]

        print(f"\n  Words unique to NEWS (formal):")
        for w in news_only[:15]:
            print(f"    {w}: {news_top[w]}")

        print(f"\n  Words unique to SOCIAL MEDIA (informal):")
        for w in social_only[:15]:
            yt = youtube_top.get(w, 0)
            rd = reddit_top.get(w, 0)
            print(f"    {w}: youtube={yt}, reddit={rd}")

        # Calculate formality metrics
        formality_metrics = {
            'news': self._calculate_formality_score(news_words),
            'youtube': self._calculate_formality_score(youtube_words),
            'reddit': self._calculate_formality_score(reddit_words)
        }

        print(f"\n  Formality Scores (higher = more formal):")
        for source, score in formality_metrics.items():
            print(f"    {source}: {score:.3f}")

        return comparison_df

    def _calculate_formality_score(self, word_counts):
        """Calculate formality score based on word patterns"""
        informal_markers = ['lol', 'lmao', 'rn', 'tbh', 'ngl', 'idk', 'smh', 'fr',
                          'yall', 'gonna', 'wanna', 'gotta', 'aint', 'yolo',
                          'bruh', 'bro', 'dude', 'guys', 'omg', 'wtf', 'af']

        formal_markers = ['however', 'therefore', 'moreover', 'furthermore',
                         'consequently', 'accordingly', 'analysts', 'investors',
                         'quarterly', 'revenue', 'earnings', 'guidance',
                         'expects', 'announces', 'reported', 'according']

        total = sum(word_counts.values())
        informal_count = sum(word_counts.get(w, 0) for w in informal_markers)
        formal_count = sum(word_counts.get(w, 0) for w in formal_markers)

        if total == 0:
            return 0.5

        informal_ratio = informal_count / total
        formal_ratio = formal_count / total

        # Score: 0 = very informal, 1 = very formal
        return 0.5 + (formal_ratio - informal_ratio) * 100

    def sentiment_comparison(self):
        """Compare sentiment patterns across sources"""
        print("\n[FormalInformal] Comparing sentiment across sources...")

        results = []

        # News sentiment
        news_sentiment = self.news_df.groupby('sentiment').size()
        total_news = len(self.news_df)
        results.append({
            'source': 'news',
            'positive_pct': news_sentiment.get('positive', 0) / total_news * 100,
            'negative_pct': news_sentiment.get('negative', 0) / total_news * 100,
            'neutral_pct': news_sentiment.get('neutral', 0) / total_news * 100,
            'avg_sentiment': self.news_df['sentiment_score'].mean()
        })

        # YouTube sentiment
        yt_sentiment = self.youtube_df.groupby('sentiment').size()
        total_yt = len(self.youtube_df)
        results.append({
            'source': 'youtube',
            'positive_pct': yt_sentiment.get('positive', 0) / total_yt * 100,
            'negative_pct': yt_sentiment.get('negative', 0) / total_yt * 100,
            'neutral_pct': yt_sentiment.get('neutral', 0) / total_yt * 100,
            'avg_sentiment': self.youtube_df['sentiment_score'].mean()
        })

        # Reddit sentiment
        rd_sentiment = self.reddit_df.groupby('sentiment').size()
        total_rd = len(self.reddit_df)
        results.append({
            'source': 'reddit',
            'positive_pct': rd_sentiment.get('positive', 0) / total_rd * 100,
            'negative_pct': rd_sentiment.get('negative', 0) / total_rd * 100,
            'neutral_pct': rd_sentiment.get('neutral', 0) / total_rd * 100,
            'avg_sentiment': self.reddit_df['sentiment_score'].mean()
        })

        results_df = pd.DataFrame(results)
        results_df.to_csv(OUTPUT_DIR / "sentiment_by_source.csv", index=False)

        print("\n  Sentiment Distribution by Source:")
        for _, row in results_df.iterrows():
            print(f"    {row['source']}: +{row['positive_pct']:.1f}% / -{row['negative_pct']:.1f}% / ~{row['neutral_pct']:.1f}% (avg={row['avg_sentiment']:.3f})")

        return results_df


# ============================================================
# WORD-PRICE TEMPORAL ANALYSIS
# ============================================================

class WordPriceTemporalAnalysis:
    """Analyze temporal relationships between words and price movements"""

    def __init__(self, spark, stock_df, news_df):
        self.spark = spark
        self.stock_df = stock_df
        self.news_df = news_df

    def prepare_data(self):
        """Prepare data for analysis"""
        print("\n[WordPriceTemporal] Preparing data...")

        # Get SPY daily data with returns
        self.spy_df = self.stock_df.filter(F.col("symbol") == "SPY") \
            .select("date", "close", "volume") \
            .withColumn("return",
                (F.col("close") - F.lag("close", 1).over(Window.orderBy("date"))) /
                F.lag("close", 1).over(Window.orderBy("date"))) \
            .withColumn("price_direction",
                F.when(F.col("return") > 0.005, "up")
                .when(F.col("return") < -0.005, "down")
                .otherwise("flat"))

        # Tokenize news headlines
        tokenizer = Tokenizer(inputCol="headline", outputCol="words")
        self.news_tokenized = tokenizer.transform(self.news_df)

        remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
        self.news_tokenized = remover.transform(self.news_tokenized)

        # Explode words
        self.word_df = self.news_tokenized \
            .select("date", "sentiment_score", F.explode("filtered_words").alias("word")) \
            .filter(F.length("word") > 2) \
            .withColumn("date", F.to_date("date"))

        print(f"  SPY records: {self.spy_df.count()}")
        print(f"  Word records: {self.word_df.count()}")

    def word_count_by_date(self):
        """MapReduce: (word, date) -> count"""
        print("\n[WordPriceTemporal] Computing word counts by date (key-value pairs)...")

        word_date_counts = self.word_df \
            .groupBy("word", "date") \
            .agg(
                F.count("*").alias("count"),
                F.avg("sentiment_score").alias("avg_sentiment")
            )

        word_date_pdf = word_date_counts.toPandas()
        word_date_pdf.to_csv(OUTPUT_DIR / "word_date_counts.csv", index=False)
        print(f"  Saved {len(word_date_pdf)} (word, date) pairs")

        return word_date_counts

    def word_before_price_movement(self, lag_days=1):
        """Analyze word frequencies before price movements"""
        print(f"\n[WordPriceTemporal] Analyzing words {lag_days} day(s) BEFORE price movements...")

        daily_words = self.word_df \
            .groupBy("date", "word") \
            .agg(F.count("*").alias("word_count"))

        spy_lagged = self.spy_df \
            .withColumn("prev_date", F.date_sub("date", lag_days)) \
            .select(
                F.col("prev_date").alias("date"),
                F.col("price_direction").alias("next_price_direction"),
                F.col("return").alias("next_return")
            )

        word_before_price = daily_words.join(spy_lagged, "date", "inner")

        word_direction_stats = word_before_price \
            .groupBy("word", "next_price_direction") \
            .agg(F.sum("word_count").alias("total_count"))

        word_direction_pivot = word_direction_stats \
            .groupBy("word") \
            .pivot("next_price_direction", ["up", "down", "flat"]) \
            .agg(F.first("total_count"))

        word_direction_pivot = word_direction_pivot.na.fill(0) \
            .withColumn("total", F.col("up") + F.col("down") + F.col("flat")) \
            .withColumn("up_ratio", F.col("up") / F.col("total")) \
            .withColumn("down_ratio", F.col("down") / F.col("total")) \
            .withColumn("predictive_bias", F.col("up_ratio") - F.col("down_ratio")) \
            .filter(F.col("total") > 50)

        pdf = word_direction_pivot.orderBy(F.abs("predictive_bias").desc()).toPandas()
        pdf.to_csv(OUTPUT_DIR / f"word_before_price_lag{lag_days}.csv", index=False)
        print(f"  Saved {len(pdf)} word-price predictions")

        print(f"\n  Top words predicting UP:")
        for _, row in pdf.nlargest(10, 'predictive_bias').iterrows():
            print(f"    {row['word']}: bias={row['predictive_bias']:.3f}")

        print(f"\n  Top words predicting DOWN:")
        for _, row in pdf.nsmallest(10, 'predictive_bias').iterrows():
            print(f"    {row['word']}: bias={row['predictive_bias']:.3f}")

        return word_direction_pivot


# ============================================================
# BIDIRECTIONAL CAUSALITY ANALYSIS
# ============================================================

class BidirectionalCausalityAnalysis:
    """Granger-style causality analysis"""

    def __init__(self, spark, stock_df, news_df):
        self.spark = spark
        self.stock_df = stock_df
        self.news_df = news_df

    def granger_style_analysis(self, max_lag=5):
        """Analyze lead-lag relationships"""
        print(f"\n[BidirectionalCausality] Granger-style analysis (max_lag={max_lag})...")

        spy_daily = self.stock_df.filter(F.col("symbol") == "SPY") \
            .select("date", "close") \
            .withColumn("return",
                (F.col("close") - F.lag("close", 1).over(Window.orderBy("date"))) /
                F.lag("close", 1).over(Window.orderBy("date")))

        daily_sentiment = self.news_df \
            .withColumn("date", F.to_date("date")) \
            .groupBy("date") \
            .agg(F.avg("sentiment_score").alias("sentiment"))

        combined = spy_daily.join(daily_sentiment, "date", "inner")

        window = Window.orderBy("date")
        for lag in range(1, max_lag + 1):
            combined = combined \
                .withColumn(f"return_lag{lag}", F.lag("return", lag).over(window)) \
                .withColumn(f"sentiment_lag{lag}", F.lag("sentiment", lag).over(window))

        pdf = combined.toPandas().dropna()

        results = []

        for lag in range(1, max_lag + 1):
            corr_s2r = pdf['return'].corr(pdf[f'sentiment_lag{lag}'])
            results.append({
                'direction': 'sentiment_to_return',
                'lag': lag,
                'correlation': corr_s2r
            })

            corr_r2s = pdf['sentiment'].corr(pdf[f'return_lag{lag}'])
            results.append({
                'direction': 'return_to_sentiment',
                'lag': lag,
                'correlation': corr_r2s
            })

        results_df = pd.DataFrame(results)
        results_df.to_csv(OUTPUT_DIR / "granger_correlations.csv", index=False)

        print("\n  Sentiment -> Return:")
        for _, row in results_df[results_df['direction'] == 'sentiment_to_return'].iterrows():
            print(f"    Lag {row['lag']}: r = {row['correlation']:.4f}")

        print("\n  Return -> Sentiment:")
        for _, row in results_df[results_df['direction'] == 'return_to_sentiment'].iterrows():
            print(f"    Lag {row['lag']}: r = {row['correlation']:.4f}")

        return results_df


# ============================================================
# MAIN FUNCTION
# ============================================================

def generate_summary_report(all_results):
    """Generate comprehensive analysis report"""
    print("\n[Report] Generating summary report...")

    report = {
        "generated_at": datetime.now().isoformat(),
        "analysis_type": "Advanced Cross-Source Analysis with PageRank",
        "engine": "PySpark",
        "analyses_performed": [
            "PageRank for Word Importance",
            "Word Co-occurrence Graph",
            "Formal vs Informal Language Comparison",
            "Word-Price Temporal Analysis",
            "Bidirectional Causality (Granger-style)",
            "Social Media Sentiment Analysis"
        ],
        "key_findings": all_results
    }

    with open(OUTPUT_DIR / "advanced_analysis_report.json", 'w') as f:
        json.dump(report, f, indent=2, default=str)

    print(f"  Saved report to {OUTPUT_DIR / 'advanced_analysis_report.json'}")

    return report


def main():
    """Run all advanced cross-source analyses"""
    print("="*60)
    print("ADVANCED CROSS-SOURCE SPARK ANALYTICS")
    print("With PageRank & Social Media Analysis")
    print("="*60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Output directory: {OUTPUT_DIR}")

    # Create Spark session
    spark = create_spark_session()

    try:
        # Load data
        print("\n" + "="*60)
        print("LOADING DATA")
        print("="*60)

        stock_file = str(DATA_DIR / "stocks" / "stock_prices.csv")
        news_file = str(DATA_DIR / "news" / "financial_news_headlines.csv")

        stock_df = spark.read.csv(stock_file, header=True, inferSchema=True)
        stock_df = stock_df.withColumn("date", F.to_date("date"))

        news_df = spark.read.csv(news_file, header=True, inferSchema=True)
        news_pdf = pd.read_csv(news_file)

        print(f"  Stock records: {stock_df.count()}")
        print(f"  News records: {news_df.count()}")

        all_results = {}

        # 1. PageRank Analysis
        print("\n" + "="*60)
        print("PAGERANK ANALYSIS FOR WORD IMPORTANCE")
        print("="*60)

        pagerank = WordPageRank(spark, news_df)
        pagerank.build_word_graph()
        pr_results = pagerank.calculate_pagerank(top_n=100)
        pagerank.find_word_communities(top_n=100)

        all_results['top_pagerank_words'] = pr_results.head(20).to_dict('records')

        # 2. Generate Social Media Data
        print("\n" + "="*60)
        print("GENERATING SOCIAL MEDIA DATA")
        print("="*60)

        social_gen = SocialMediaDataGenerator(news_pdf)
        youtube_df = social_gen.generate_youtube_comments(n_comments=10000)
        reddit_df = social_gen.generate_reddit_comments(n_comments=10000)

        # 3. Formal vs Informal Analysis
        print("\n" + "="*60)
        print("FORMAL VS INFORMAL LANGUAGE ANALYSIS")
        print("="*60)

        formal_informal = FormalInformalAnalysis(spark, news_pdf, youtube_df, reddit_df)
        word_comparison = formal_informal.compare_top_words(top_n=50)
        sentiment_comparison = formal_informal.sentiment_comparison()

        all_results['sentiment_by_source'] = sentiment_comparison.to_dict('records')

        # 4. Word-Price Temporal Analysis
        print("\n" + "="*60)
        print("WORD-PRICE TEMPORAL ANALYSIS")
        print("="*60)

        temporal = WordPriceTemporalAnalysis(spark, stock_df, news_df)
        temporal.prepare_data()
        temporal.word_count_by_date()
        temporal.word_before_price_movement(lag_days=1)

        # 5. Bidirectional Causality
        print("\n" + "="*60)
        print("BIDIRECTIONAL CAUSALITY ANALYSIS")
        print("="*60)

        causality = BidirectionalCausalityAnalysis(spark, stock_df, news_df)
        granger_results = causality.granger_style_analysis(max_lag=5)

        all_results['granger_analysis'] = granger_results.to_dict('records')

        # Generate report
        generate_summary_report(all_results)

        print("\n" + "="*60)
        print("ANALYSIS COMPLETE!")
        print("="*60)
        print(f"\nOutput files saved to: {OUTPUT_DIR}")
        print("\nFiles generated:")
        for f in sorted(OUTPUT_DIR.glob("*")):
            size = f.stat().st_size / 1024
            print(f"  {f.name}: {size:.1f} KB")

    finally:
        spark.stop()
        print(f"\nFinished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
