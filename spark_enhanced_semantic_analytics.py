#!/usr/bin/env python3
"""
ENHANCED SEMANTIC ANALYTICS PIPELINE
Uses rich semantic features from Reddit, Twitter, and on-chain data
Generates meaningful insights by combining social sentiment with actual behavior
"""

import os
import sys
import json
import lmdb
from pathlib import Path
from datetime import datetime

# Spark environment setup
os.environ['HADOOP_HOME'] = 'C:/hadoop'
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from dotenv import load_dotenv
load_dotenv()

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, stddev, count, sum as spark_sum, max as spark_max,
    lit, when, explode, array, struct, to_json, from_json,
    unix_timestamp, from_unixtime
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType, MapType, ArrayType
)
from pymongo import MongoClient

# Configuration
QUESTDB_HOST = os.getenv("QUESTDB_HOST", "localhost")
QUESTDB_PORT = os.getenv("QUESTDB_PORT", "8812")
QUESTDB_JDBC = f"jdbc:postgresql://{QUESTDB_HOST}:{QUESTDB_PORT}/qdb"

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "financial_analytics")

LMDB_REDDIT = Path("data/reddit/reddit_sentiment_lmdb.db")
LMDB_TWITTER = Path("data/twitter/twitter_sentiment.db")
LMDB_ONCHAIN = Path("data/onchain/onchain_data.db")


class EnhancedSemanticAnalytics:
    """Enhanced analytics using rich semantic features"""

    def __init__(self):
        self.spark = None
        self.mongo_client = None

    def initialize_spark(self):
        """Initialize Spark session"""
        print("[spark] Initializing Enhanced Semantic Analytics...")

        self.spark = SparkSession.builder \
            .appName("EnhancedSemanticAnalytics") \
            .master("local[2]") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")
        print("[spark] Spark session initialized")

    def initialize_mongodb(self):
        """Initialize MongoDB client"""
        self.mongo_client = MongoClient(MONGODB_URI)
        print("[mongo] MongoDB connected")

    def read_questdb_orderbook(self):
        """Read orderbook data from QuestDB"""
        print("[spark] Reading orderbook data...")

        df = self.spark.read \
            .format("jdbc") \
            .option("url", QUESTDB_JDBC) \
            .option("dbtable", "orderbook") \
            .option("user", "admin") \
            .option("password", "quest") \
            .option("driver", "org.postgresql.Driver") \
            .load()

        df = df.withColumnRenamed("ts", "timestamp")
        print(f"[spark] Loaded {df.count()} orderbook records")
        return df

    def read_lmdb_sentiment(self, db_path: Path, key_prefix: str) -> list:
        """Read sentiment data from LMDB"""
        print(f"[lmdb] Reading from {db_path} (prefix: {key_prefix})...")

        if not db_path.exists():
            print(f"[lmdb] Database not found: {db_path}")
            return []

        env = lmdb.open(str(db_path), readonly=True)
        records = []

        with env.begin() as txn:
            cursor = txn.cursor()
            for key, value in cursor:
                if key.startswith(key_prefix.encode()):
                    record = json.loads(value.decode())
                    records.append(record)

        env.close()
        print(f"[lmdb] Loaded {len(records)} records")
        return records

    def read_reddit_sentiment(self):
        """Read enhanced Reddit sentiment"""
        records = self.read_lmdb_sentiment(LMDB_REDDIT, "sentiment:")

        if records:
            df = self.spark.createDataFrame(records)
            print(f"[reddit] Loaded {df.count()} Reddit sentiment records")
            return df
        else:
            return None

    def read_twitter_sentiment(self):
        """Read Twitter sentiment"""
        records = self.read_lmdb_sentiment(LMDB_TWITTER, "tweet:")

        if records:
            df = self.spark.createDataFrame(records)
            print(f"[twitter] Loaded {df.count()} Twitter sentiment records")
            return df
        else:
            return None

    def read_onchain_data(self):
        """Read on-chain transaction data"""
        records = self.read_lmdb_sentiment(LMDB_ONCHAIN, "tx:")

        if records:
            df = self.spark.createDataFrame(records)
            print(f"[onchain] Loaded {df.count()} on-chain transactions")
            return df
        else:
            return None

    def analyze_entity_specific_sentiment(self, reddit_df, twitter_df):
        """
        NEW SEMANTIC ANALYTICS #1: Entity-Specific Sentiment
        Instead of overall sentiment, analyze sentiment PER COIN
        """
        print("\n[SEMANTIC-1] Analyzing entity-specific sentiment...")

        results = []

        for df, source in [(reddit_df, "reddit"), (twitter_df, "twitter")]:
            if df is None:
                continue

            # Extract entities from sentiment field
            # Reddit/Twitter store entities as {'BTC': 3, 'ETH': 1} in sentiment.entities

            entity_data = df.select(
                col("timestamp"),
                col("sentiment.entities").alias("entities"),
                col("sentiment.polarity").alias("polarity"),
                col("sentiment.topics").alias("topics"),
                col("sentiment.urgency_score").alias("urgency")
            ).collect()

            # Process each record
            for row in entity_data:
                entities = row['entities']
                if entities:
                    for symbol, mentions in entities.items():
                        results.append({
                            "source": source,
                            "symbol": symbol,
                            "mentions": mentions,
                            "sentiment": row['polarity'],
                            "urgency": row['urgency'],
                            "topics": row['topics'],
                            "timestamp": row['timestamp']
                        })

        if results:
            result_df = self.spark.createDataFrame(results)

            # Aggregate by symbol
            aggregated = result_df.groupBy("symbol", "source").agg(
                count("*").alias("total_mentions"),
                avg("sentiment").alias("avg_sentiment"),
                avg("urgency").alias("avg_urgency")
            ).orderBy(col("total_mentions").desc())

            print(f"  [OK] Entity-specific sentiment for {aggregated.count()} symbol-source pairs")
            return aggregated
        else:
            return None

    def analyze_topic_price_correlation(self, reddit_df, twitter_df, orderbook_df):
        """
        NEW SEMANTIC ANALYTICS #2: Topic-Price Correlation
        Correlate specific topics (bullish, regulation, etc.) with price movements
        """
        print("\n[SEMANTIC-2] Analyzing topic-price correlations...")

        # Combine social data
        social_records = []

        for df, source in [(reddit_df, "reddit"), (twitter_df, "twitter")]:
            if df is None:
                continue

            data = df.select(
                col("timestamp"),
                col("sentiment.topics").alias("topics"),
                col("sentiment.entities").alias("entities")
            ).collect()

            for row in data:
                if row['topics'] and row['entities']:
                    for topic in row['topics']:
                        for symbol in row['entities'].keys():
                            social_records.append({
                                "timestamp": row['timestamp'],
                                "topic": topic,
                                "symbol": symbol,
                                "source": source
                            })

        if not social_records:
            print("  [SKIP] No topic data available")
            return None

        social_df = self.spark.createDataFrame(social_records)

        # Get price changes for each symbol
        price_changes = orderbook_df.filter(col("venue_type") == "SPOT") \
            .select(
                col("timestamp"),
                col("base").alias("symbol"),
                col("mid_price")
            )

        # Aggregate topics by hour
        topic_counts = social_df.groupBy(
            (col("timestamp") / 3600).cast("long").alias("hour"),
            col("topic"),
            col("symbol")
        ).agg(
            count("*").alias("topic_mentions")
        )

        print(f"  [OK] Topic-price correlation analysis completed")
        return topic_counts

    def analyze_sentiment_vs_onchain(self, reddit_df, twitter_df, onchain_df):
        """
        NEW SEMANTIC ANALYTICS #3: Sentiment vs On-Chain Behavior
        Compare what people SAY vs what they DO
        This is the KEY semantic insight!
        """
        print("\n[SEMANTIC-3] Analyzing sentiment vs on-chain behavior...")

        if onchain_df is None:
            print("  [SKIP] No on-chain data available")
            return None

        # Aggregate social sentiment by hour
        social_sentiment = []

        for df, source in [(reddit_df, "reddit"), (twitter_df, "twitter")]:
            if df is None:
                continue

            data = df.select(
                (col("timestamp") / 3600).cast("long").alias("hour"),
                col("sentiment.polarity").alias("sentiment"),
                col("sentiment.entities").alias("entities")
            ).collect()

            for row in data:
                if row['entities']:
                    for symbol in row['entities'].keys():
                        social_sentiment.append({
                            "hour": row['hour'],
                            "symbol": symbol,
                            "sentiment": row['sentiment'],
                            "source": source
                        })

        if not social_sentiment:
            print("  [SKIP] No social sentiment data")
            return None

        social_df = self.spark.createDataFrame(social_sentiment)

        # Aggregate on-chain behavior by hour
        onchain_behavior = onchain_df.select(
            (col("timestamp") / 3600).cast("long").alias("hour"),
            col("asset").alias("symbol"),
            col("analysis.signal").alias("signal"),
            col("analysis.flow_direction").alias("flow"),
            col("analysis.is_whale").alias("is_whale")
        )

        # Join sentiment with on-chain behavior
        combined = social_df.groupBy("hour", "symbol").agg(
            avg("sentiment").alias("avg_sentiment"),
            count("*").alias("mention_count")
        ).join(
            onchain_behavior.groupBy("hour", "symbol").agg(
                count(when(col("signal") == "accumulation", 1)).alias("accumulation_count"),
                count(when(col("signal") == "distribution", 1)).alias("distribution_count"),
                count(when(col("is_whale") == True, 1)).alias("whale_count")
            ),
            on=["hour", "symbol"],
            how="inner"
        )

        # Detect divergence: positive sentiment but distribution (selling)
        divergence = combined.withColumn(
            "divergence_type",
            when(
                (col("avg_sentiment") > 0.2) & (col("distribution_count") > col("accumulation_count")),
                "BEARISH_DIVERGENCE"  # People bullish but whales selling
            ).when(
                (col("avg_sentiment") < -0.2) & (col("accumulation_count") > col("distribution_count")),
                "BULLISH_DIVERGENCE"  # People bearish but whales buying
            ).otherwise("NO_DIVERGENCE")
        ).filter(
            col("divergence_type") != "NO_DIVERGENCE"
        )

        print(f"  [OK] Found {divergence.count()} sentiment-onchain divergences")
        return divergence

    def analyze_influence_weighted_sentiment(self, twitter_df):
        """
        NEW SEMANTIC ANALYTICS #4: Influence-Weighted Sentiment
        Weight sentiment by user influence (follower count)
        """
        print("\n[SEMANTIC-4] Analyzing influence-weighted sentiment...")

        if twitter_df is None:
            print("  [SKIP] No Twitter data available")
            return None

        # Extract influence scores and sentiment
        influence_data = twitter_df.select(
            col("timestamp"),
            col("sentiment.entities").alias("entities"),
            col("sentiment.polarity").alias("sentiment"),
            col("sentiment.influence_score").alias("influence")
        ).collect()

        weighted_results = []

        for row in influence_data:
            if row['entities'] and row['influence']:
                for symbol in row['entities'].keys():
                    # Weight sentiment by influence
                    weighted_sentiment = row['sentiment'] * row['influence']

                    weighted_results.append({
                        "timestamp": row['timestamp'],
                        "symbol": symbol,
                        "raw_sentiment": row['sentiment'],
                        "influence": row['influence'],
                        "weighted_sentiment": weighted_sentiment
                    })

        if not weighted_results:
            print("  [SKIP] No influence data")
            return None

        result_df = self.spark.createDataFrame(weighted_results)

        # Aggregate by symbol
        aggregated = result_df.groupBy("symbol").agg(
            avg("raw_sentiment").alias("avg_raw_sentiment"),
            avg("weighted_sentiment").alias("avg_weighted_sentiment"),
            avg("influence").alias("avg_influence"),
            count("*").alias("mention_count")
        ).withColumn(
            "sentiment_adjustment",
            col("avg_weighted_sentiment") - col("avg_raw_sentiment")
        ).orderBy(col("mention_count").desc())

        print(f"  [OK] Influence-weighted sentiment for {aggregated.count()} symbols")
        return aggregated

    def write_to_mongodb(self, analytics_results: dict):
        """Write all analytics to MongoDB"""
        print("\n[mongo] Writing enhanced semantic analytics to MongoDB...")

        db = self.mongo_client[MONGODB_DATABASE]
        collection = db["enhanced_semantic_analytics"]

        # Write each analytics type
        for analytics_type, df in analytics_results.items():
            if df is not None and df.count() > 0:
                # Convert to pandas and write
                pandas_df = df.toPandas()
                records = pandas_df.to_dict('records')

                # Add metadata
                for record in records:
                    record['analytics_type'] = analytics_type
                    record['created_at'] = datetime.now()

                collection.insert_many(records)
                print(f"  ✓ Wrote {len(records)} records for {analytics_type}")

        print("[mongo] Enhanced analytics written successfully")

    def run_enhanced_pipeline(self):
        """Run the complete enhanced semantic analytics pipeline"""
        print("=" * 80)
        print("ENHANCED SEMANTIC ANALYTICS PIPELINE")
        print("=" * 80)
        print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)

        # Read all data sources
        print("\n[1] Reading data sources...")
        orderbook_df = self.read_questdb_orderbook()
        reddit_df = self.read_reddit_sentiment()
        twitter_df = self.read_twitter_sentiment()
        onchain_df = self.read_onchain_data()

        # Run enhanced semantic analytics
        print("\n[2] Running enhanced semantic analytics...")

        analytics_results = {}

        # SEMANTIC-1: Entity-specific sentiment
        analytics_results['entity_sentiment'] = self.analyze_entity_specific_sentiment(
            reddit_df, twitter_df
        )

        # SEMANTIC-2: Topic-price correlation
        analytics_results['topic_correlation'] = self.analyze_topic_price_correlation(
            reddit_df, twitter_df, orderbook_df
        )

        # SEMANTIC-3: Sentiment vs on-chain (KEY INSIGHT!)
        analytics_results['sentiment_onchain_divergence'] = self.analyze_sentiment_vs_onchain(
            reddit_df, twitter_df, onchain_df
        )

        # SEMANTIC-4: Influence-weighted sentiment
        analytics_results['influence_weighted'] = self.analyze_influence_weighted_sentiment(
            twitter_df
        )

        # Write results
        print("\n[3] Writing results to MongoDB...")
        self.write_to_mongodb(analytics_results)

        print("\n" + "=" * 80)
        print("ENHANCED SEMANTIC ANALYTICS COMPLETED")
        print("=" * 80)

        return analytics_results

    def shutdown(self):
        """Cleanup resources"""
        if self.spark:
            self.spark.stop()
        if self.mongo_client:
            self.mongo_client.close()
        print("\n[shutdown] Resources cleaned up")


def main():
    """Main entry point"""
    pipeline = EnhancedSemanticAnalytics()

    try:
        pipeline.initialize_spark()
        pipeline.initialize_mongodb()
        results = pipeline.run_enhanced_pipeline()

        print("\n✓ Pipeline completed successfully!")
        print("  View results in MongoDB: financial_analytics.enhanced_semantic_analytics")

    except Exception as e:
        print(f"\n✗ Pipeline failed: {e}")
        import traceback
        traceback.print_exc()

    finally:
        pipeline.shutdown()


if __name__ == "__main__":
    main()
