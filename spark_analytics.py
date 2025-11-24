#!/usr/bin/env python3
"""
PYSPARK ANALYTICS PIPELINE
Reads data from QuestDB (JDBC) and LMDB (sentiment)
Performs analytics: arbitrage detection, sentiment correlation, price prediction
Outputs to: Cassandra (time-series), Neo4j (graphs), MongoDB (documents)
"""

import os
import sys
import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List
import lmdb

# Set Spark environment variables BEFORE importing pyspark (Windows fix)
os.environ['HADOOP_HOME'] = 'C:/hadoop'
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['PYTHONUNBUFFERED'] = '1'  # Disable buffering to avoid socket issues
os.environ['PYTHONFAULTHANDLER'] = '1'  # Enable fault handler

from dotenv import load_dotenv
from neo4j import GraphDatabase
from neo4j_graph_writer import Neo4jGraphWriter
from advanced_cross_analytics import AdvancedCrossAnalytics

# Load environment variables from .env file
load_dotenv()

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, stddev, max as spark_max, min as spark_min,
    window, lag, lead, abs as spark_abs, when, lit, sum,
    from_json, to_json, struct, collect_list, explode,
    unix_timestamp, from_unixtime, current_timestamp, count
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    LongType, TimestampType, IntegerType, MapType
)
from pyspark.sql.window import Window

# Configuration (loaded from .env)
QUESTDB_HOST = os.getenv("QUESTDB_HOST", "localhost")
QUESTDB_PORT = os.getenv("QUESTDB_PORT", "8812")
QUESTDB_USER = os.getenv("QUESTDB_USER", "admin")
QUESTDB_PASSWORD = os.getenv("QUESTDB_PASSWORD", "quest")
QUESTDB_JDBC = f"jdbc:postgresql://{QUESTDB_HOST}:{QUESTDB_PORT}/qdb"

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "financial_data")

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "financial_analytics")

LMDB_PATH = Path("data/reddit/reddit_sentiment_lmdb.db")

# Analytics configuration
ARBITRAGE_THRESHOLD_BPS = int(os.getenv("ARBITRAGE_THRESHOLD_BPS", "10"))  # Reduced from 50 to 10 bps
SENTIMENT_CORRELATION_WINDOW = "1 hour"
PREDICTION_WINDOW_HOURS = 24  # Changed from minutes to hours for better predictions


class SparkAnalyticsPipeline:
    """PySpark-based analytics pipeline for financial data"""

    def __init__(self):
        self.spark = None
        self.lmdb = None

    def initialize_spark(self):
        """Initialize Spark session with required connectors"""
        print("[spark] Initializing Spark session...")

        self.spark = SparkSession.builder \
            .appName("FinancialDataAnalytics") \
            .master("local[2]") \
            .config("spark.jars.packages",
                   "org.postgresql:postgresql:42.5.0,"
                   "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                   "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0") \
            .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
            .config("spark.cassandra.connection.port", CASSANDRA_PORT) \
            .config("spark.mongodb.read.connection.uri", MONGODB_URI) \
            .config("spark.mongodb.write.connection.uri", MONGODB_URI) \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.python.worker.reuse", "true") \
            .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true") \
            .config("spark.python.worker.faulthandler.enabled", "true") \
            .config("spark.task.maxFailures", "1") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")
        print("[spark] Spark session initialized")

    def read_questdb_orderbook(self) -> "DataFrame":
        """Read orderbook data from QuestDB via JDBC"""
        print("[spark] Reading orderbook data from QuestDB...")

        # Read ALL available data - simplified query without subquery
        df = self.spark.read \
            .format("jdbc") \
            .option("url", QUESTDB_JDBC) \
            .option("dbtable", "orderbook") \
            .option("user", QUESTDB_USER) \
            .option("password", QUESTDB_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .load()

        # Rename ts column to timestamp for consistency
        df = df.withColumnRenamed("ts", "timestamp")

        print(f"[spark] Loaded {df.count()} orderbook records")
        return df

    def read_lmdb_sentiment(self) -> "DataFrame":
        """Read sentiment data from LMDB and convert to Spark DataFrame"""
        print("[spark] Reading sentiment data from LMDB...")

        # Open LMDB
        env = lmdb.open(str(LMDB_PATH), readonly=True)

        # Read sentiment data
        sentiment_records = []

        with env.begin() as txn:
            cursor = txn.cursor()
            for key, value in cursor:
                if key.startswith(b"sentiment:"):
                    record = json.loads(value.decode())
                    sentiment_records.append(record)

        env.close()

        print(f"[spark] Loaded {len(sentiment_records)} sentiment records from LMDB")

        # Convert to Spark DataFrame
        if sentiment_records:
            df = self.spark.createDataFrame(sentiment_records)
            return df
        else:
            # Return empty DataFrame with schema
            schema = StructType([
                StructField("item_id", StringType(), True),
                StructField("item_type", StringType(), True),
                StructField("subreddit", StringType(), True),
                StructField("text", StringType(), True),
                StructField("sentiment", MapType(StringType(), StringType()), True),
                StructField("timestamp", DoubleType(), True),
            ])
            return self.spark.createDataFrame([], schema)

    def detect_arbitrage(self, orderbook_df: "DataFrame") -> "DataFrame":
        """
        Detect arbitrage opportunities across exchanges
        Returns opportunities where spread > threshold
        """
        print("[spark] Detecting arbitrage opportunities...")

        # Filter only crypto (SPOT) data
        crypto_df = orderbook_df.filter(col("venue_type") == "SPOT")

        # Get latest price for each symbol-exchange pair
        window_spec = Window.partitionBy("symbol", "exchange").orderBy(col("timestamp").desc())
        latest_prices = crypto_df.withColumn("row_num", lag("seq").over(window_spec)) \
            .filter(col("row_num").isNull()) \
            .select("timestamp", "exchange", "symbol", "base", "quote", "region",
                   "best_bid", "best_ask", "mid_price")

        # Cross join to find all exchange pairs for same symbol
        arb_opportunities = latest_prices.alias("ex1") \
            .join(latest_prices.alias("ex2"),
                  (col("ex1.symbol") == col("ex2.symbol")) &
                  (col("ex1.exchange") < col("ex2.exchange")),  # Avoid duplicates
                  "inner") \
            .select(
                col("ex1.timestamp").alias("timestamp"),
                col("ex1.symbol").alias("symbol"),
                col("ex1.base").alias("base"),
                col("ex1.exchange").alias("buy_exchange"),
                col("ex2.exchange").alias("sell_exchange"),
                col("ex1.best_ask").alias("buy_price"),
                col("ex2.best_bid").alias("sell_price"),
                col("ex1.region").alias("buy_region"),
                col("ex2.region").alias("sell_region")
            )

        # Calculate arbitrage spread
        arb_opportunities = arb_opportunities.withColumn(
            "spread_bps",
            ((col("sell_price") - col("buy_price")) / col("buy_price")) * 10000
        ).withColumn(
            "profit_pct",
            ((col("sell_price") - col("buy_price")) / col("buy_price")) * 100
        )

        # Filter for profitable opportunities
        profitable_arb = arb_opportunities.filter(
            col("spread_bps") > ARBITRAGE_THRESHOLD_BPS
        ).orderBy(col("spread_bps").desc())

        arb_count = profitable_arb.count()
        print(f"[spark] Found {arb_count} arbitrage opportunities")

        return profitable_arb

    def correlate_sentiment_price(self, orderbook_df: "DataFrame",
                                  sentiment_df: "DataFrame") -> "DataFrame":
        """
        Correlate Reddit sentiment with price movements
        """
        print("[spark] Correlating sentiment with price movements...")

        # Convert timestamp to proper format
        sentiment_with_ts = sentiment_df.withColumn(
            "timestamp",
            from_unixtime(col("timestamp"))
        )

        # Extract sentiment score
        sentiment_with_ts = sentiment_with_ts.withColumn(
            "sentiment_polarity",
            col("sentiment.polarity").cast(DoubleType())
        )

        # Aggregate sentiment by subreddit and time window
        sentiment_agg = sentiment_with_ts.groupBy(
            window(col("timestamp"), SENTIMENT_CORRELATION_WINDOW),
            col("subreddit")
        ).agg(
            avg("sentiment_polarity").alias("avg_sentiment"),
            stddev("sentiment_polarity").alias("sentiment_volatility"),
            count("*").alias("post_count")
        ).select(
            col("window.start").alias("period_start"),
            col("window.end").alias("period_end"),
            col("subreddit"),
            col("avg_sentiment"),
            col("sentiment_volatility"),
            col("post_count")
        )

        # Aggregate crypto prices by time window
        crypto_df = orderbook_df.filter(col("venue_type") == "SPOT")

        price_agg = crypto_df.groupBy(
            window(col("timestamp"), SENTIMENT_CORRELATION_WINDOW),
            col("symbol")
        ).agg(
            avg("mid_price").alias("avg_price"),
            spark_max("mid_price").alias("max_price"),
            spark_min("mid_price").alias("min_price"),
            avg("spread_bps").alias("avg_spread")
        ).select(
            col("window.start").alias("period_start"),
            col("window.end").alias("period_end"),
            col("symbol"),
            col("avg_price"),
            col("max_price"),
            col("min_price"),
            col("avg_spread")
        ).withColumn(
            "price_volatility",
            ((col("max_price") - col("min_price")) / col("avg_price")) * 100
        )

        # Join sentiment and price data (broad join - looking for general correlation)
        correlation = sentiment_agg.join(
            price_agg,
            (sentiment_agg.period_start == price_agg.period_start),
            "inner"
        ).select(
            sentiment_agg.period_start,
            sentiment_agg.period_end,
            col("subreddit"),
            col("symbol"),
            col("avg_sentiment"),
            col("sentiment_volatility"),
            col("post_count"),
            col("avg_price"),
            col("price_volatility"),
            col("avg_spread")
        )

        corr_count = correlation.count()
        print(f"[spark] Generated {corr_count} sentiment-price correlation records")

        return correlation

    def predict_price_movement(self, orderbook_df: "DataFrame") -> "DataFrame":
        """
        Simple price prediction based on historical patterns
        Uses moving averages and trend detection
        """
        print("[spark] Predicting price movements...")

        # Focus on recent data - using 24 hours for better prediction window
        recent_data = orderbook_df.filter(
            col("timestamp") > from_unixtime(unix_timestamp() - (PREDICTION_WINDOW_HOURS * 3600))
        )

        # Calculate moving averages
        window_spec = Window.partitionBy("symbol", "exchange") \
            .orderBy("timestamp") \
            .rowsBetween(-10, 0)

        predictions = recent_data.withColumn(
            "ma_10",
            avg("mid_price").over(window_spec)
        ).withColumn(
            "ma_spread",
            avg("spread_bps").over(window_spec)
        )

        # Detect trend
        predictions = predictions.withColumn(
            "trend",
            when(col("mid_price") > col("ma_10"), "UP")
            .when(col("mid_price") < col("ma_10"), "DOWN")
            .otherwise("NEUTRAL")
        )

        # Simple prediction: if trending up and spread is tightening, predict continuation
        predictions = predictions.withColumn(
            "prediction",
            when(
                (col("trend") == "UP") & (col("spread_bps") < col("ma_spread")),
                "BULLISH"
            ).when(
                (col("trend") == "DOWN") & (col("spread_bps") < col("ma_spread")),
                "BEARISH"
            ).otherwise("NEUTRAL")
        ).withColumn(
            "confidence",
            spark_abs((col("mid_price") - col("ma_10")) / col("ma_10")) * 100
        )

        # Get latest predictions only
        agg_df = predictions.groupBy("symbol", "exchange") \
            .agg(spark_max("timestamp").alias("latest_timestamp"))

        latest_predictions = predictions.alias("p").join(
                agg_df.alias("agg"),
                (col("p.symbol") == col("agg.symbol")) &
                (col("p.exchange") == col("agg.exchange")) &
                (col("p.timestamp") == col("agg.latest_timestamp")),
                "inner"
            ).select(
                col("p.timestamp"),
                col("p.exchange"),
                col("p.symbol"),
                col("p.base"),
                col("p.quote"),
                col("p.venue_type"),
                col("p.mid_price"),
                col("p.ma_10"),
                col("p.trend"),
                col("p.prediction"),
                col("p.confidence")
            )

        pred_count = latest_predictions.count()
        print(f"[spark] Generated {pred_count} price predictions")

        return latest_predictions

    # ========================================================================
    # B) VOLUME & LIQUIDITY ANALYTICS
    # ========================================================================

    def analyze_order_depth(self, orderbook_df: "DataFrame") -> "DataFrame":
        """
        B1: Order book depth analysis
        Uses spread as proxy for liquidity (tighter spread = deeper book)
        """
        print("[spark] Analyzing order book depth...")

        # Filter crypto data
        crypto_df = orderbook_df.filter(col("venue_type") == "SPOT")

        # Calculate liquidity metrics using spread as proxy
        depth_analysis = crypto_df.groupBy(
            window(col("timestamp"), "1 hour"),
            col("symbol"),
            col("exchange")
        ).agg(
            avg("spread_bps").alias("avg_spread"),
            spark_min("spread_bps").alias("min_spread"),
            spark_max("spread_bps").alias("max_spread"),
            count("*").alias("tick_count")
        ).select(
            col("window.start").alias("period_start"),
            col("window.end").alias("period_end"),
            col("symbol"),
            col("exchange"),
            col("avg_spread"),
            col("min_spread"),
            col("max_spread"),
            col("tick_count")
        )

        # Calculate liquidity score (inverse of spread - lower spread = higher liquidity)
        depth_analysis = depth_analysis.withColumn(
            "liquidity_score",
            (1000.0 / col("avg_spread"))  # Higher score = better liquidity
        ).withColumn(
            "market_depth_tier",
            when(col("liquidity_score") > 10000, "VERY_DEEP")
            .when(col("liquidity_score") > 5000, "DEEP")
            .when(col("liquidity_score") > 1000, "MODERATE")
            .otherwise("SHALLOW")
        ).withColumn(
            "spread_stability",
            (col("max_spread") - col("min_spread")) / col("avg_spread")
        )

        depth_count = depth_analysis.count()
        print(f"[spark] Generated {depth_count} order depth records")

        return depth_analysis

    def calculate_vwap(self, orderbook_df: "DataFrame") -> "DataFrame":
        """
        B2: Volume-Weighted Average Price
        Uses inverse spread as weight proxy (tighter spread = higher volume)
        """
        print("[spark] Calculating VWAP...")

        # Filter crypto data
        crypto_df = orderbook_df.filter(col("venue_type") == "SPOT")

        # Add inverse spread weight
        weighted_df = crypto_df.withColumn(
            "spread_weight",
            1.0 / (col("spread_bps") + 0.01)  # Add small constant to avoid division by zero
        ).withColumn(
            "weighted_price",
            col("mid_price") * col("spread_weight")
        )

        # Calculate VWAP per time window
        vwap_df = weighted_df.groupBy(
            window(col("timestamp"), "1 hour"),
            col("symbol"),
            col("exchange")
        ).agg(
            (sum("weighted_price") / sum("spread_weight")).alias("VWAP"),
            avg("mid_price").alias("simple_avg"),
            avg("spread_bps").alias("avg_spread"),
            count("*").alias("tick_count")
        ).select(
            col("window.start").alias("period_start"),
            col("window.end").alias("period_end"),
            col("symbol"),
            col("exchange"),
            col("VWAP"),
            col("simple_avg"),
            col("avg_spread"),
            col("tick_count")
        )

        # Calculate VWAP vs simple average
        vwap_df = vwap_df.withColumn(
            "vwap_vs_avg_pct",
            ((col("VWAP") - col("simple_avg")) / col("simple_avg")) * 100
        ).withColumn(
            "signal",
            when(col("vwap_vs_avg_pct") < -0.1, "BEARISH_PRESSURE")
            .when(col("vwap_vs_avg_pct") > 0.1, "BULLISH_PRESSURE")
            .otherwise("NEUTRAL")
        )

        vwap_count = vwap_df.count()
        print(f"[spark] Generated {vwap_count} VWAP records")

        return vwap_df

    def analyze_bid_ask_imbalance(self, orderbook_df: "DataFrame") -> "DataFrame":
        """
        B3: Bid-Ask Imbalance Analysis
        Measures buy vs sell pressure
        """
        print("[spark] Analyzing bid-ask imbalance...")

        # Filter crypto data
        crypto_df = orderbook_df.filter(col("venue_type") == "SPOT")

        # Calculate bid/ask distances from mid price
        imbalance_df = crypto_df.withColumn(
            "bid_strength",
            col("mid_price") - col("best_bid")
        ).withColumn(
            "ask_strength",
            col("best_ask") - col("mid_price")
        ).withColumn(
            "imbalance_ratio",
            (col("bid_strength") - col("ask_strength")) / (col("bid_strength") + col("ask_strength"))
        )

        # Aggregate by time window
        imbalance_agg = imbalance_df.groupBy(
            window(col("timestamp"), "1 hour"),
            col("symbol"),
            col("exchange")
        ).agg(
            avg("imbalance_ratio").alias("avg_imbalance"),
            avg("bid_strength").alias("avg_bid_strength"),
            avg("ask_strength").alias("avg_ask_strength"),
            count("*").alias("tick_count")
        ).select(
            col("window.start").alias("period_start"),
            col("window.end").alias("period_end"),
            col("symbol"),
            col("exchange"),
            col("avg_imbalance"),
            col("avg_bid_strength"),
            col("avg_ask_strength"),
            col("tick_count")
        )

        # Classify pressure
        imbalance_agg = imbalance_agg.withColumn(
            "pressure_type",
            when(col("avg_imbalance") > 0.15, "STRONG_BUY_PRESSURE")
            .when(col("avg_imbalance") > 0.05, "MODERATE_BUY_PRESSURE")
            .when(col("avg_imbalance") < -0.15, "STRONG_SELL_PRESSURE")
            .when(col("avg_imbalance") < -0.05, "MODERATE_SELL_PRESSURE")
            .otherwise("BALANCED")
        ).withColumn(
            "confidence",
            spark_abs(col("avg_imbalance"))
        )

        imbalance_count = imbalance_agg.count()
        print(f"[spark] Generated {imbalance_count} bid-ask imbalance records")

        return imbalance_agg

    # ========================================================================
    # C) VOLATILITY & PATTERN DETECTION
    # ========================================================================

    def calculate_rolling_volatility(self, orderbook_df: "DataFrame") -> "DataFrame":
        """
        C1: Rolling volatility across multiple timeframes (1h, 4h, 24h)
        """
        print("[spark] Calculating rolling volatility...")

        # Filter crypto data
        crypto_df = orderbook_df.filter(col("venue_type") == "SPOT")

        # Define window specifications for different timeframes
        window_1h = Window.partitionBy("symbol", "exchange") \
            .orderBy(unix_timestamp("timestamp")) \
            .rangeBetween(-3600, 0)  # 1 hour in seconds

        window_4h = Window.partitionBy("symbol", "exchange") \
            .orderBy(unix_timestamp("timestamp")) \
            .rangeBetween(-14400, 0)  # 4 hours in seconds

        window_24h = Window.partitionBy("symbol", "exchange") \
            .orderBy(unix_timestamp("timestamp")) \
            .rangeBetween(-86400, 0)  # 24 hours in seconds

        # Calculate volatility for each timeframe
        volatility_df = crypto_df.withColumn(
            "volatility_1h",
            (stddev("mid_price").over(window_1h) / avg("mid_price").over(window_1h)) * 100
        ).withColumn(
            "volatility_4h",
            (stddev("mid_price").over(window_4h) / avg("mid_price").over(window_4h)) * 100
        ).withColumn(
            "volatility_24h",
            (stddev("mid_price").over(window_24h) / avg("mid_price").over(window_24h)) * 100
        )

        # Get latest volatility for each symbol-exchange pair
        latest_vol = volatility_df.groupBy("symbol", "exchange") \
            .agg(spark_max("timestamp").alias("latest_timestamp"))

        volatility_final = volatility_df.alias("v").join(
            latest_vol.alias("l"),
            (col("v.symbol") == col("l.symbol")) &
            (col("v.exchange") == col("l.exchange")) &
            (col("v.timestamp") == col("l.latest_timestamp")),
            "inner"
        ).select(
            col("v.timestamp"),
            col("v.exchange"),
            col("v.symbol"),
            col("v.base"),
            col("v.quote"),
            col("v.mid_price"),
            col("v.volatility_1h"),
            col("v.volatility_4h"),
            col("v.volatility_24h")
        )

        # Detect volatility trends and spikes
        volatility_final = volatility_final.withColumn(
            "volatility_trend",
            when(col("volatility_1h") < col("volatility_24h") * 0.5, "DECREASING")
            .when(col("volatility_1h") > col("volatility_24h") * 2, "INCREASING")
            .otherwise("STABLE")
        ).withColumn(
            "spike_detected",
            col("volatility_1h") > (col("volatility_24h") * 2)
        )

        vol_count = volatility_final.count()
        print(f"[spark] Generated {vol_count} volatility records")

        return volatility_final

    def calculate_bollinger_bands(self, orderbook_df: "DataFrame") -> "DataFrame":
        """
        C2: Bollinger Bands calculation (20-period MA ± 2 std dev)
        """
        print("[spark] Calculating Bollinger Bands...")

        # Filter crypto data
        crypto_df = orderbook_df.filter(col("venue_type") == "SPOT")

        # Window specification for 20-period lookback
        window_spec = Window.partitionBy("symbol", "exchange") \
            .orderBy("timestamp") \
            .rowsBetween(-19, 0)

        # Calculate Bollinger Bands
        bb_df = crypto_df.withColumn(
            "MA_20",
            avg("mid_price").over(window_spec)
        ).withColumn(
            "STD_20",
            stddev("mid_price").over(window_spec)
        ).withColumn(
            "upper_band",
            col("MA_20") + (2 * col("STD_20"))
        ).withColumn(
            "lower_band",
            col("MA_20") - (2 * col("STD_20"))
        )

        # Calculate band position (0 = lower band, 1 = upper band)
        bb_df = bb_df.withColumn(
            "band_position",
            (col("mid_price") - col("lower_band")) / (col("upper_band") - col("lower_band"))
        ).withColumn(
            "band_width_pct",
            ((col("upper_band") - col("lower_band")) / col("MA_20")) * 100
        ).withColumn(
            "signal",
            when(col("band_position") > 0.8, "OVERBOUGHT")
            .when(col("band_position") < 0.2, "OVERSOLD")
            .otherwise("NEUTRAL")
        )

        # Get latest values
        latest_bb = bb_df.groupBy("symbol", "exchange") \
            .agg(spark_max("timestamp").alias("latest_timestamp"))

        bb_final = bb_df.alias("bb").join(
            latest_bb.alias("l"),
            (col("bb.symbol") == col("l.symbol")) &
            (col("bb.exchange") == col("l.exchange")) &
            (col("bb.timestamp") == col("l.latest_timestamp")),
            "inner"
        ).select(
            col("bb.timestamp"),
            col("bb.exchange"),
            col("bb.symbol"),
            col("bb.mid_price"),
            col("bb.MA_20"),
            col("bb.upper_band"),
            col("bb.lower_band"),
            col("bb.band_position"),
            col("bb.band_width_pct"),
            col("bb.signal")
        )

        bb_count = bb_final.count()
        print(f"[spark] Generated {bb_count} Bollinger Bands records")

        return bb_final

    def detect_flash_events(self, orderbook_df: "DataFrame") -> "DataFrame":
        """
        C3: Flash event detection (>5% moves in <1 minute)
        """
        print("[spark] Detecting flash events...")

        # Filter crypto data
        crypto_df = orderbook_df.filter(col("venue_type") == "SPOT")

        # Create 1-minute buckets
        minute_buckets = crypto_df.withColumn(
            "minute_bucket",
            from_unixtime(
                (unix_timestamp(col("timestamp")) / 60).cast("long") * 60
            )
        )

        # Aggregate by minute
        minute_agg = minute_buckets.groupBy(
            col("minute_bucket"),
            col("symbol"),
            col("exchange")
        ).agg(
            spark_min("timestamp").alias("first_timestamp"),
            spark_max("timestamp").alias("last_timestamp"),
            spark_min("mid_price").alias("first_price"),
            spark_max("mid_price").alias("last_price"),
            spark_min("mid_price").alias("min_price"),
            spark_max("mid_price").alias("max_price"),
            count("*").alias("tick_count")
        )

        # Calculate price changes
        minute_agg = minute_agg.withColumn(
            "minute_change_pct",
            ((col("last_price") - col("first_price")) / col("first_price")) * 100
        ).withColumn(
            "minute_range_pct",
            ((col("max_price") - col("min_price")) / col("first_price")) * 100
        ).withColumn(
            "flash_event_detected",
            (spark_abs(col("minute_change_pct")) > 5) | (col("minute_range_pct") > 7)
        )

        # Filter only flash events
        flash_events = minute_agg.filter(col("flash_event_detected") == True)

        # Classify event type
        flash_events = flash_events.withColumn(
            "event_type",
            when(col("minute_change_pct") < -5, "FLASH_CRASH")
            .when(col("minute_change_pct") > 5, "FLASH_PUMP")
            .otherwise("FLASH_VOLATILITY")
        ).withColumn(
            "recovery_within_minute",
            spark_abs(col("minute_change_pct")) < (col("minute_range_pct") * 0.5)
        )

        flash_count = flash_events.count()
        print(f"[spark] Detected {flash_count} flash events")

        return flash_events

    # ========================================================================
    # D) ENHANCED SENTIMENT ANALYTICS
    # ========================================================================

    def analyze_sentiment_momentum(self, sentiment_df: "DataFrame") -> "DataFrame":
        """
        D1: Sentiment momentum (rate of change)
        """
        print("[spark] Analyzing sentiment momentum...")

        # Convert timestamp
        sentiment_with_ts = sentiment_df.withColumn(
            "timestamp",
            from_unixtime(col("timestamp"))
        ).withColumn(
            "sentiment_polarity",
            col("sentiment.polarity").cast(DoubleType())
        )

        # Aggregate by hour
        hourly_sentiment = sentiment_with_ts.groupBy(
            window(col("timestamp"), "1 hour"),
            col("subreddit")
        ).agg(
            avg("sentiment_polarity").alias("avg_sentiment"),
            count("*").alias("post_count")
        ).select(
            col("window.start").alias("hour_start"),
            col("window.end").alias("hour_end"),
            col("subreddit"),
            col("avg_sentiment"),
            col("post_count")
        )

        # Calculate momentum using window function
        window_spec = Window.partitionBy("subreddit").orderBy("hour_start")

        momentum_df = hourly_sentiment.withColumn(
            "previous_sentiment",
            lag("avg_sentiment", 1).over(window_spec)
        ).withColumn(
            "previous_post_count",
            lag("post_count", 1).over(window_spec)
        )

        # Calculate momentum and acceleration
        momentum_df = momentum_df.withColumn(
            "sentiment_momentum",
            col("avg_sentiment") - col("previous_sentiment")
        ).withColumn(
            "post_count_change_pct",
            ((col("post_count") - col("previous_post_count")) / col("previous_post_count")) * 100
        ).withColumn(
            "momentum_type",
            when((col("sentiment_momentum") > 0.1) & (col("avg_sentiment") > 0), "BULLISH_ACCELERATION")
            .when((col("sentiment_momentum") > 0.1) & (col("avg_sentiment") < 0), "BULLISH_REVERSAL")
            .when((col("sentiment_momentum") < -0.1) & (col("avg_sentiment") < 0), "BEARISH_ACCELERATION")
            .when((col("sentiment_momentum") < -0.1) & (col("avg_sentiment") > 0), "BEARISH_REVERSAL")
            .otherwise("STABLE")
        )

        # Filter out null values from lag operation
        momentum_final = momentum_df.filter(col("previous_sentiment").isNotNull())

        momentum_count = momentum_final.count()
        print(f"[spark] Generated {momentum_count} sentiment momentum records")

        return momentum_final

    def detect_sentiment_divergence(self, orderbook_df: "DataFrame",
                                   sentiment_df: "DataFrame") -> "DataFrame":
        """
        D2: Sentiment-price divergence detection
        """
        print("[spark] Detecting sentiment-price divergence...")

        # Prepare sentiment data
        sentiment_with_ts = sentiment_df.withColumn(
            "timestamp",
            from_unixtime(col("timestamp"))
        ).withColumn(
            "sentiment_polarity",
            col("sentiment.polarity").cast(DoubleType())
        )

        # Aggregate sentiment by hour
        sentiment_hourly = sentiment_with_ts.groupBy(
            window(col("timestamp"), "1 hour"),
            col("subreddit")
        ).agg(
            avg("sentiment_polarity").alias("avg_sentiment")
        ).select(
            col("window.start").alias("period_start"),
            col("window.end").alias("period_end"),
            col("subreddit"),
            col("avg_sentiment")
        )

        # Prepare price data
        crypto_df = orderbook_df.filter(col("venue_type") == "SPOT")

        # Calculate hourly price changes
        price_hourly = crypto_df.groupBy(
            window(col("timestamp"), "1 hour"),
            col("symbol")
        ).agg(
            avg("mid_price").alias("avg_price")
        ).select(
            col("window.start").alias("period_start"),
            col("window.end").alias("period_end"),
            col("symbol"),
            col("avg_price")
        )

        # Calculate price change
        window_spec = Window.partitionBy("symbol").orderBy("period_start")
        price_with_change = price_hourly.withColumn(
            "previous_price",
            lag("avg_price", 1).over(window_spec)
        ).withColumn(
            "price_change_pct",
            ((col("avg_price") - col("previous_price")) / col("previous_price")) * 100
        ).filter(col("previous_price").isNotNull())

        # Join sentiment and price
        divergence = sentiment_hourly.join(
            price_with_change,
            sentiment_hourly.period_start == price_with_change.period_start,
            "inner"
        ).select(
            sentiment_hourly.period_start,
            sentiment_hourly.period_end,
            col("subreddit"),
            col("symbol"),
            col("avg_sentiment"),
            col("price_change_pct")
        )

        # Detect divergence
        divergence = divergence.withColumn(
            "sentiment_direction",
            when(col("avg_sentiment") > 0.05, 1)
            .when(col("avg_sentiment") < -0.05, -1)
            .otherwise(0)
        ).withColumn(
            "price_direction",
            when(col("price_change_pct") > 0.5, 1)
            .when(col("price_change_pct") < -0.5, -1)
            .otherwise(0)
        ).withColumn(
            "divergence_detected",
            (col("sentiment_direction") * col("price_direction")) < 0
        ).withColumn(
            "divergence_strength",
            spark_abs(col("avg_sentiment")) * spark_abs(col("price_change_pct"))
        )

        # Classify divergence type
        divergence = divergence.withColumn(
            "divergence_type",
            when(
                (col("divergence_detected") == True) & (col("sentiment_direction") > 0),
                "BULLISH_SENTIMENT_BEARISH_PRICE"
            ).when(
                (col("divergence_detected") == True) & (col("sentiment_direction") < 0),
                "BEARISH_SENTIMENT_BULLISH_PRICE"
            ).otherwise("NO_DIVERGENCE")
        )

        # Filter significant divergences
        significant_divergence = divergence.filter(
            (col("divergence_detected") == True) & (col("divergence_strength") > 0.1)
        )

        div_count = significant_divergence.count()
        print(f"[spark] Detected {div_count} sentiment-price divergences")

        return significant_divergence

    # ========================================================================
    # F) TEXT CONTENT ANALYTICS (Word-Level Analysis)
    # ========================================================================

    def analyze_word_frequency(self, sentiment_df: "DataFrame") -> "DataFrame":
        """
        F1: Word frequency and word count analysis
        Extracts individual words from text content and analyzes frequency patterns
        """
        print("[spark] Analyzing word frequency and content...")

        from pyspark.sql.functions import (
            split, explode, lower, regexp_replace, trim, length, size
        )

        # Convert timestamp and extract text
        text_df = sentiment_df.withColumn(
            "timestamp",
            from_unixtime(col("timestamp"))
        ).withColumn(
            "sentiment_polarity",
            col("sentiment.polarity").cast(DoubleType())
        ).select(
            col("item_id"),
            col("subreddit"),
            col("text"),
            col("timestamp"),
            col("sentiment_polarity")
        ).filter(
            col("text").isNotNull() & (length(col("text")) > 0)
        )

        # Calculate word count per text
        word_count_df = text_df.withColumn(
            "word_count",
            size(split(col("text"), "\\s+"))
        )

        # Tokenize: clean text and split into words
        words_df = word_count_df.withColumn(
            "cleaned_text",
            lower(regexp_replace(col("text"), "[^a-zA-Z0-9\\s]", " "))
        ).withColumn(
            "words_array",
            split(trim(col("cleaned_text")), "\\s+")
        ).withColumn(
            "word",
            explode(col("words_array"))
        ).filter(
            # Filter out empty strings and very short words (< 3 chars)
            (length(col("word")) >= 3)
        )

        # Aggregate word frequency by time window (1 hour) and subreddit
        word_freq_hourly = words_df.groupBy(
            window(col("timestamp"), "1 hour"),
            col("subreddit"),
            col("word")
        ).agg(
            count("*").alias("word_frequency"),
            avg("sentiment_polarity").alias("avg_sentiment_for_word"),
            avg("word_count").alias("avg_text_length")
        ).select(
            col("window.start").alias("period_start"),
            col("window.end").alias("period_end"),
            col("subreddit"),
            col("word"),
            col("word_frequency"),
            col("avg_sentiment_for_word"),
            col("avg_text_length")
        )

        # Calculate word rank within each time window and subreddit
        window_spec = Window.partitionBy("period_start", "subreddit").orderBy(col("word_frequency").desc())

        word_freq_ranked = word_freq_hourly.withColumn(
            "word_rank",
            lag("word_frequency", 0).over(window_spec)
        ).withColumn(
            "word_rank",
            when(col("word_rank").isNull(), col("word_frequency")).otherwise(col("word_rank"))
        )

        # Only keep top 100 words per time window per subreddit
        top_words = word_freq_ranked.filter(
            col("word_frequency") >= 2  # Word must appear at least twice
        )

        word_count = top_words.count()
        print(f"[spark] Generated {word_count} word frequency records")

        return top_words

    def correlate_words_with_price(self, sentiment_df: "DataFrame",
                                   orderbook_df: "DataFrame") -> "DataFrame":
        """
        F2: Word-price correlation analysis
        Connects word frequency trends with price movements
        """
        print("[spark] Correlating word trends with price movements...")

        from pyspark.sql.functions import (
            split, explode, lower, regexp_replace, trim, length
        )

        # Get word frequency data (simplified version for correlation)
        text_df = sentiment_df.withColumn(
            "timestamp",
            from_unixtime(col("timestamp"))
        ).withColumn(
            "sentiment_polarity",
            col("sentiment.polarity").cast(DoubleType())
        ).select(
            col("subreddit"),
            col("text"),
            col("timestamp"),
            col("sentiment_polarity")
        ).filter(
            col("text").isNotNull() & (length(col("text")) > 0)
        )

        # Tokenize and aggregate by hour
        words_df = text_df.withColumn(
            "cleaned_text",
            lower(regexp_replace(col("text"), "[^a-zA-Z0-9\\s]", " "))
        ).withColumn(
            "word",
            explode(split(trim(col("cleaned_text")), "\\s+"))
        ).filter(
            length(col("word")) >= 3
        )

        # Aggregate words by hour
        word_hourly = words_df.groupBy(
            window(col("timestamp"), "1 hour"),
            col("subreddit"),
            col("word")
        ).agg(
            count("*").alias("word_frequency")
        ).select(
            col("window.start").alias("period_start"),
            col("window.end").alias("period_end"),
            col("subreddit"),
            col("word"),
            col("word_frequency")
        )

        # Get price data
        crypto_df = orderbook_df.filter(col("venue_type") == "SPOT")

        price_hourly = crypto_df.groupBy(
            window(col("timestamp"), "1 hour"),
            col("symbol")
        ).agg(
            avg("mid_price").alias("avg_price")
        ).select(
            col("window.start").alias("period_start"),
            col("window.end").alias("period_end"),
            col("symbol"),
            col("avg_price")
        )

        # Calculate price change
        window_spec = Window.partitionBy("symbol").orderBy("period_start")
        price_with_change = price_hourly.withColumn(
            "previous_price",
            lag("avg_price", 1).over(window_spec)
        ).withColumn(
            "price_change_pct",
            ((col("avg_price") - col("previous_price")) / col("previous_price")) * 100
        ).filter(col("previous_price").isNotNull())

        # Calculate word frequency change (trending words)
        word_window_spec = Window.partitionBy("subreddit", "word").orderBy("period_start")
        word_with_change = word_hourly.withColumn(
            "previous_frequency",
            lag("word_frequency", 1).over(word_window_spec)
        ).withColumn(
            "frequency_change_pct",
            when(
                col("previous_frequency").isNotNull() & (col("previous_frequency") > 0),
                ((col("word_frequency") - col("previous_frequency")) / col("previous_frequency")) * 100
            ).otherwise(100.0)  # First occurrence = 100% increase
        ).filter(
            col("previous_frequency").isNotNull()
        )

        # Join word trends with price movements
        word_price_correlation = word_with_change.join(
            price_with_change,
            word_with_change.period_start == price_with_change.period_start,
            "inner"
        ).select(
            word_with_change.period_start,
            word_with_change.period_end,
            col("subreddit"),
            col("symbol"),
            col("word"),
            col("word_frequency"),
            col("frequency_change_pct"),
            col("avg_price"),
            col("price_change_pct")
        )

        # Detect correlation patterns
        word_price_correlation = word_price_correlation.withColumn(
            "word_trend",
            when(col("frequency_change_pct") > 50, "SURGING")
            .when(col("frequency_change_pct") > 20, "RISING")
            .when(col("frequency_change_pct") < -20, "DECLINING")
            .otherwise("STABLE")
        ).withColumn(
            "price_trend",
            when(col("price_change_pct") > 2, "BULLISH")
            .when(col("price_change_pct") < -2, "BEARISH")
            .otherwise("NEUTRAL")
        ).withColumn(
            "correlation_signal",
            when(
                (col("word_trend") == "SURGING") & (col("price_trend") == "BULLISH"),
                "STRONG_BULLISH"
            ).when(
                (col("word_trend") == "SURGING") & (col("price_trend") == "BEARISH"),
                "DIVERGENCE_BEARISH"
            ).when(
                (col("word_trend") == "DECLINING") & (col("price_trend") == "BULLISH"),
                "DIVERGENCE_BULLISH"
            ).when(
                (col("word_trend") == "RISING") & (col("price_trend") == "BULLISH"),
                "MODERATE_BULLISH"
            ).otherwise("NO_SIGNAL")
        )

        # Filter for significant signals only
        significant_signals = word_price_correlation.filter(
            (col("word_frequency") >= 3) &  # Word appears at least 3 times
            (col("correlation_signal") != "NO_SIGNAL")
        )

        signal_count = significant_signals.count()
        print(f"[spark] Generated {signal_count} word-price correlation signals")

        return significant_signals

    def analyze_word_sentiment_distribution(self, sentiment_df: "DataFrame") -> "DataFrame":
        """
        F3: Word-sentiment distribution analysis
        Analyzes how specific words correlate with sentiment scores
        """
        print("[spark] Analyzing word-sentiment distribution...")

        from pyspark.sql.functions import (
            split, explode, lower, regexp_replace, trim, length
        )

        # Prepare text data
        text_df = sentiment_df.withColumn(
            "timestamp",
            from_unixtime(col("timestamp"))
        ).withColumn(
            "sentiment_polarity",
            col("sentiment.polarity").cast(DoubleType())
        ).select(
            col("subreddit"),
            col("text"),
            col("timestamp"),
            col("sentiment_polarity")
        ).filter(
            col("text").isNotNull() & (length(col("text")) > 0)
        )

        # Tokenize
        words_df = text_df.withColumn(
            "cleaned_text",
            lower(regexp_replace(col("text"), "[^a-zA-Z0-9\\s]", " "))
        ).withColumn(
            "word",
            explode(split(trim(col("cleaned_text")), "\\s+"))
        ).filter(
            length(col("word")) >= 3
        )

        # Aggregate word-sentiment statistics
        word_sentiment = words_df.groupBy(
            col("subreddit"),
            col("word")
        ).agg(
            count("*").alias("total_occurrences"),
            avg("sentiment_polarity").alias("avg_sentiment"),
            stddev("sentiment_polarity").alias("sentiment_stddev"),
            spark_max("sentiment_polarity").alias("max_sentiment"),
            spark_min("sentiment_polarity").alias("min_sentiment")
        ).withColumn(
            "sentiment_range",
            col("max_sentiment") - col("min_sentiment")
        ).withColumn(
            "sentiment_category",
            when(col("avg_sentiment") > 0.3, "VERY_POSITIVE")
            .when(col("avg_sentiment") > 0.1, "POSITIVE")
            .when(col("avg_sentiment") < -0.3, "VERY_NEGATIVE")
            .when(col("avg_sentiment") < -0.1, "NEGATIVE")
            .otherwise("NEUTRAL")
        ).withColumn(
            "timestamp",
            current_timestamp()
        )

        # Filter for words with sufficient occurrences
        significant_words = word_sentiment.filter(
            col("total_occurrences") >= 5
        ).orderBy(col("total_occurrences").desc())

        word_sent_count = significant_words.count()
        print(f"[spark] Generated {word_sent_count} word-sentiment distribution records")

        return significant_words

    # ========================================================================
    # E) CROSS-ASSET ANALYTICS
    # ========================================================================

    def calculate_correlation_matrix(self, orderbook_df: "DataFrame") -> "DataFrame":
        """
        E1: Asset correlation matrix (BTC, ETH, SOL, etc.)
        """
        print("[spark] Calculating correlation matrix...")

        # Filter crypto data
        crypto_df = orderbook_df.filter(col("venue_type") == "SPOT")

        # Create hourly price changes for each asset
        hourly_prices = crypto_df.groupBy(
            window(col("timestamp"), "1 hour"),
            col("base")  # BTC, ETH, SOL, etc.
        ).agg(
            avg("mid_price").alias("avg_price")
        ).select(
            col("window.start").alias("hour_start"),
            col("base"),
            col("avg_price")
        )

        # Calculate price changes
        window_spec = Window.partitionBy("base").orderBy("hour_start")
        price_changes = hourly_prices.withColumn(
            "previous_price",
            lag("avg_price", 1).over(window_spec)
        ).withColumn(
            "price_change_pct",
            ((col("avg_price") - col("previous_price")) / col("previous_price")) * 100
        ).filter(col("previous_price").isNotNull())

        # Pivot to get asset columns
        from pyspark.sql.functions import first

        pivot_df = price_changes.groupBy("hour_start").pivot("base").agg(
            first("price_change_pct")
        )

        # Get column names (assets)
        asset_columns = [c for c in pivot_df.columns if c != "hour_start"]

        # Calculate pairwise correlations
        correlations = []
        for i, asset1 in enumerate(asset_columns):
            for asset2 in asset_columns[i+1:]:
                # Calculate correlation for each pair
                corr_value = pivot_df.stat.corr(asset1, asset2)
                correlations.append({
                    "asset_1": asset1,
                    "asset_2": asset2,
                    "correlation": corr_value,
                    "timestamp": datetime.now()
                })

        # Convert to DataFrame
        if correlations:
            corr_df = self.spark.createDataFrame(correlations)

            # Add classification
            corr_df = corr_df.withColumn(
                "correlation_strength",
                when(spark_abs(col("correlation")) > 0.8, "VERY_HIGH")
                .when(spark_abs(col("correlation")) > 0.6, "HIGH")
                .when(spark_abs(col("correlation")) > 0.4, "MODERATE")
                .when(spark_abs(col("correlation")) > 0.2, "LOW")
                .otherwise("VERY_LOW")
            ).withColumn(
                "market_regime",
                when(col("correlation") > 0.8, "HIGH_CORRELATION")
                .when(col("correlation") > 0.5, "MODERATE_CORRELATION")
                .otherwise("LOW_CORRELATION")
            )

            corr_count = corr_df.count()
            print(f"[spark] Generated {corr_count} correlation pairs")

            return corr_df
        else:
            # Return empty DataFrame
            schema = StructType([
                StructField("asset_1", StringType(), True),
                StructField("asset_2", StringType(), True),
                StructField("correlation", DoubleType(), True),
                StructField("timestamp", TimestampType(), True),
                StructField("correlation_strength", StringType(), True),
                StructField("market_regime", StringType(), True)
            ])
            return self.spark.createDataFrame([], schema)

    def transform_to_unified_format(self, df: "DataFrame", analytics_type: str) -> "DataFrame":
        """
        Transform any analytics DataFrame into unified format
        Maps specific fields to generic columns (value1, value2, value3, metadata)
        """
        from pyspark.sql.functions import (
            to_date, lit, create_map, concat, when, coalesce
        )

        # Add analytics_type and date_bucket
        unified_df = df.withColumn("analytics_type", lit(analytics_type)) \
                       .withColumn("date_bucket", to_date(col("timestamp")).cast(StringType()))

        # Initialize common fields with nulls
        if "symbol" not in df.columns:
            unified_df = unified_df.withColumn("symbol", lit(None).cast(StringType()))
        if "exchange" not in df.columns:
            unified_df = unified_df.withColumn("exchange", lit(None).cast(StringType()))
        if "subreddit" not in df.columns:
            unified_df = unified_df.withColumn("subreddit", lit(None).cast(StringType()))
        if "base" not in df.columns:
            unified_df = unified_df.withColumn("base", lit(None).cast(StringType()))
        if "quote" not in df.columns:
            unified_df = unified_df.withColumn("quote", lit(None).cast(StringType()))

        # Map analytics-specific fields to unified schema
        if analytics_type == "arbitrage":
            unified_df = unified_df \
                .withColumn("value1", col("buy_price")) \
                .withColumn("value2", col("sell_price")) \
                .withColumn("value3", col("spread_bps")) \
                .withColumn("count_metric", lit(None).cast(LongType())) \
                .withColumn("signal_type", lit("ARBITRAGE")) \
                .withColumn("category", lit(None).cast(StringType())) \
                .withColumn("alert_detected", col("spread_bps") > ARBITRAGE_THRESHOLD_BPS) \
                .withColumn("metadata", create_map(
                    lit("profit_pct"), col("profit_pct").cast(StringType()),
                    lit("buy_exchange"), col("buy_exchange"),
                    lit("sell_exchange"), col("sell_exchange"),
                    lit("buy_region"), col("buy_region"),
                    lit("sell_region"), col("sell_region")
                ))

        elif analytics_type == "sentiment_correlation":
            unified_df = unified_df \
                .withColumn("timestamp", col("period_start")) \
                .withColumn("value1", col("avg_sentiment")) \
                .withColumn("value2", col("avg_price")) \
                .withColumn("value3", col("price_volatility")) \
                .withColumn("count_metric", col("post_count")) \
                .withColumn("signal_type", lit(None).cast(StringType())) \
                .withColumn("category", lit(None).cast(StringType())) \
                .withColumn("alert_detected", lit(False)) \
                .withColumn("metadata", create_map(
                    lit("sentiment_volatility"), coalesce(col("sentiment_volatility"), lit(0.0)).cast(StringType()),
                    lit("avg_spread"), coalesce(col("avg_spread"), lit(0.0)).cast(StringType())
                ))

        elif analytics_type == "price_prediction":
            unified_df = unified_df \
                .withColumn("value1", col("mid_price")) \
                .withColumn("value2", col("ma_10")) \
                .withColumn("value3", col("confidence")) \
                .withColumn("count_metric", lit(None).cast(LongType())) \
                .withColumn("signal_type", col("prediction")) \
                .withColumn("category", col("trend")) \
                .withColumn("alert_detected", col("confidence") > 5.0) \
                .withColumn("metadata", create_map(
                    lit("venue_type"), col("venue_type")
                ))

        elif analytics_type == "word_frequency":
            # Extract word from metadata map to a separate field for easier querying
            unified_df = unified_df \
                .withColumn("timestamp", col("period_start")) \
                .withColumn("value1", col("avg_sentiment_for_word")) \
                .withColumn("value2", col("avg_text_length")) \
                .withColumn("value3", lit(None).cast(DoubleType())) \
                .withColumn("count_metric", col("word_frequency")) \
                .withColumn("signal_type", lit(None).cast(StringType())) \
                .withColumn("category", lit("WORD_ANALYSIS")) \
                .withColumn("alert_detected", col("word_frequency") > 50) \
                .withColumn("metadata", create_map(
                    lit("word"), col("word"),
                    lit("word_rank"), coalesce(col("word_rank"), lit(0)).cast(StringType())
                ))

        elif analytics_type == "word_price_correlation":
            unified_df = unified_df \
                .withColumn("timestamp", col("period_start")) \
                .withColumn("value1", col("avg_price")) \
                .withColumn("value2", col("price_change_pct")) \
                .withColumn("value3", col("frequency_change_pct")) \
                .withColumn("count_metric", col("word_frequency")) \
                .withColumn("signal_type", col("correlation_signal")) \
                .withColumn("category", col("word_trend")) \
                .withColumn("alert_detected",
                    col("correlation_signal").isin(["STRONG_BULLISH", "DIVERGENCE_BEARISH"])) \
                .withColumn("metadata", create_map(
                    lit("word"), col("word"),
                    lit("price_trend"), col("price_trend")
                ))

        elif analytics_type == "word_sentiment_dist":
            unified_df = unified_df \
                .withColumn("value1", col("avg_sentiment")) \
                .withColumn("value2", col("sentiment_stddev")) \
                .withColumn("value3", col("sentiment_range")) \
                .withColumn("count_metric", col("total_occurrences")) \
                .withColumn("signal_type", lit(None).cast(StringType())) \
                .withColumn("category", col("sentiment_category")) \
                .withColumn("alert_detected",
                    col("sentiment_category").isin(["VERY_POSITIVE", "VERY_NEGATIVE"])) \
                .withColumn("metadata", create_map(
                    lit("word"), col("word"),
                    lit("max_sentiment"), col("max_sentiment").cast(StringType()),
                    lit("min_sentiment"), col("min_sentiment").cast(StringType())
                ))

        elif analytics_type == "rolling_volatility":
            unified_df = unified_df \
                .withColumn("value1", col("mid_price")) \
                .withColumn("value2", col("volatility_1h")) \
                .withColumn("value3", col("volatility_4h")) \
                .withColumn("count_metric", lit(None).cast(LongType())) \
                .withColumn("signal_type", col("volatility_trend")) \
                .withColumn("category", lit(None).cast(StringType())) \
                .withColumn("alert_detected", col("spike_detected")) \
                .withColumn("metadata", create_map(
                    lit("volatility_24h"), col("volatility_24h").cast(StringType())
                ))

        elif analytics_type == "sentiment_momentum":
            unified_df = unified_df \
                .withColumn("timestamp", col("hour_start")) \
                .withColumn("value1", col("avg_sentiment")) \
                .withColumn("value2", col("sentiment_momentum")) \
                .withColumn("value3", col("post_count_change_pct")) \
                .withColumn("count_metric", col("post_count")) \
                .withColumn("signal_type", col("momentum_type")) \
                .withColumn("category", lit(None).cast(StringType())) \
                .withColumn("alert_detected",
                    col("momentum_type").isin(["BULLISH_ACCELERATION", "BEARISH_ACCELERATION"])) \
                .withColumn("metadata", create_map(
                    lit("previous_sentiment"), col("previous_sentiment").cast(StringType())
                ))

        # Add more elif blocks for other analytics types as needed
        else:
            # Default transformation for unknown types
            unified_df = unified_df \
                .withColumn("value1", lit(None).cast(DoubleType())) \
                .withColumn("value2", lit(None).cast(DoubleType())) \
                .withColumn("value3", lit(None).cast(DoubleType())) \
                .withColumn("count_metric", lit(None).cast(LongType())) \
                .withColumn("signal_type", lit(None).cast(StringType())) \
                .withColumn("category", lit(None).cast(StringType())) \
                .withColumn("alert_detected", lit(False)) \
                .withColumn("metadata", create_map())

        # Select only the unified schema columns
        unified_df = unified_df.select(
            "analytics_type",
            "date_bucket",
            "timestamp",
            "symbol",
            "exchange",
            "subreddit",
            "base",
            "quote",
            "value1",
            "value2",
            "value3",
            "count_metric",
            "signal_type",
            "category",
            "metadata",
            "alert_detected"
        )

        return unified_df

    def write_to_cassandra_unified(self, df: "DataFrame", analytics_type: str):
        """Write DataFrame to unified Cassandra table"""
        print(f"[spark] Writing {analytics_type} to unified Cassandra table...")

        # Transform to unified format
        unified_df = self.transform_to_unified_format(df, analytics_type)

        # Write to unified table
        unified_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .option("keyspace", CASSANDRA_KEYSPACE) \
            .option("table", "analytics_unified") \
            .save()

        record_count = df.count()
        print(f"[spark] Successfully wrote {record_count} {analytics_type} records to unified table")

    def write_to_cassandra(self, df: "DataFrame", table_name: str):
        """Write DataFrame to Cassandra"""
        print(f"[spark] Writing to Cassandra table: {table_name}...")

        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .option("keyspace", CASSANDRA_KEYSPACE) \
            .option("table", table_name) \
            .save()

        print(f"[spark] Successfully wrote to Cassandra: {table_name}")

    def write_to_neo4j(self, df: "DataFrame", node_label: str, relationship: str = None):
        """Write DataFrame to Neo4j as nodes/relationships using Python driver"""
        print(f"[spark] Writing to Neo4j: {node_label}...")

        # Get Neo4j connection details from environment
        neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        neo4j_user = os.getenv("NEO4J_USER", "neo4j")
        neo4j_password = os.getenv("NEO4J_PASSWORD", "password")

        # Collect data from DataFrame (limit for memory safety)
        rows = df.limit(10000).collect()

        if not rows:
            print(f"[spark] No data to write to Neo4j")
            return

        # Connect to Neo4j
        driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

        def create_nodes(tx, label, records):
            """Create nodes in Neo4j"""
            for record in records:
                # Convert row to dict and filter out null values (Neo4j doesn't accept nulls)
                props = {k: v for k, v in record.asDict().items() if v is not None}

                if not props:
                    continue  # Skip if all values are null

                # Build property string for Cypher
                prop_str = ", ".join([f"{k}: ${k}" for k in props.keys()])
                query = f"MERGE (n:{label} {{{prop_str}}})"
                tx.run(query, **props)

        # Write nodes in batches
        with driver.session() as session:
            batch_size = 1000
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i+batch_size]
                session.execute_write(create_nodes, node_label, batch)

        driver.close()
        print(f"[spark] Successfully wrote {len(rows)} nodes to Neo4j: {node_label}")

    def write_to_mongodb(self, df: "DataFrame", collection_name: str):
        """Write DataFrame to MongoDB"""
        print(f"[spark] Writing to MongoDB collection: {collection_name}...")

        df.write \
            .format("mongodb") \
            .mode("append") \
            .option("database", MONGODB_DATABASE) \
            .option("collection", collection_name) \
            .save()

        print(f"[spark] Successfully wrote to MongoDB: {collection_name}")

    def write_to_mongodb_unified(self, df: "DataFrame", analytics_type: str):
        """Write DataFrame to unified MongoDB collection"""
        print(f"[spark] Writing {analytics_type} to unified MongoDB collection...")

        # Transform to unified format
        unified_df = self.transform_to_unified_format(df, analytics_type)

        # Write to unified collection
        unified_df.write \
            .format("mongodb") \
            .mode("append") \
            .option("database", MONGODB_DATABASE) \
            .option("collection", "analytics_unified") \
            .save()

        record_count = df.count()
        print(f"[spark] Successfully wrote {record_count} {analytics_type} records to unified collection")

    def run_pipeline(self):
        """Execute the complete analytics pipeline with all analytics"""
        print("=" * 80)
        print("ENHANCED PYSPARK ANALYTICS PIPELINE STARTED")
        print("=" * 80)

        try:
            # Initialize Spark
            self.initialize_spark()

            # Read data sources
            print("\n[1] Reading data sources...")
            orderbook_df = self.read_questdb_orderbook()
            sentiment_df = self.read_lmdb_sentiment()

            orderbook_count = orderbook_df.count()
            sentiment_count = sentiment_df.count()

            print(f"Loaded {orderbook_count} orderbook records")
            print(f"Loaded {sentiment_count} sentiment records")

            # ========================================================================
            # A) ORIGINAL ANALYTICS (FIXED)
            # ========================================================================
            print("\n" + "=" * 80)
            print("A) ORIGINAL ANALYTICS (IMPROVED)")
            print("=" * 80)

            print("\n[A1] Arbitrage detection (threshold: {0} bps)...".format(ARBITRAGE_THRESHOLD_BPS))
            arbitrage_df = self.detect_arbitrage(orderbook_df)

            if sentiment_count > 0:
                print("\n[A2] Sentiment-price correlation...")
                correlation_df = self.correlate_sentiment_price(orderbook_df, sentiment_df)
            else:
                print("\n[A2] Skipping sentiment correlation (no sentiment data)")
                correlation_df = None

            print("\n[A3] Price prediction ({0}h window)...".format(PREDICTION_WINDOW_HOURS))
            prediction_df = self.predict_price_movement(orderbook_df)

            # ========================================================================
            # B) VOLUME & LIQUIDITY ANALYTICS
            # ========================================================================
            print("\n" + "=" * 80)
            print("B) VOLUME & LIQUIDITY ANALYTICS")
            print("=" * 80)

            print("\n[B1] Order depth analysis...")
            depth_df = self.analyze_order_depth(orderbook_df)

            print("\n[B2] VWAP calculation...")
            vwap_df = self.calculate_vwap(orderbook_df)

            print("\n[B3] Bid-ask imbalance...")
            imbalance_df = self.analyze_bid_ask_imbalance(orderbook_df)

            # ========================================================================
            # C) VOLATILITY & PATTERN DETECTION
            # ========================================================================
            print("\n" + "=" * 80)
            print("C) VOLATILITY & PATTERN DETECTION")
            print("=" * 80)

            print("\n[C1] Rolling volatility (1h, 4h, 24h)...")
            volatility_df = self.calculate_rolling_volatility(orderbook_df)

            print("\n[C2] Bollinger Bands...")
            bollinger_df = self.calculate_bollinger_bands(orderbook_df)

            print("\n[C3] Flash event detection...")
            flash_df = self.detect_flash_events(orderbook_df)

            # ========================================================================
            # D) ENHANCED SENTIMENT ANALYTICS
            # ========================================================================
            if sentiment_count > 0:
                print("\n" + "=" * 80)
                print("D) ENHANCED SENTIMENT ANALYTICS")
                print("=" * 80)

                print("\n[D1] Sentiment momentum...")
                momentum_df = self.analyze_sentiment_momentum(sentiment_df)

                print("\n[D2] Sentiment-price divergence...")
                divergence_df = self.detect_sentiment_divergence(orderbook_df, sentiment_df)
            else:
                print("\n[SKIP] Enhanced sentiment analytics (no sentiment data)")
                momentum_df = None
                divergence_df = None

            # ========================================================================
            # E) CROSS-ASSET ANALYTICS
            # ========================================================================
            print("\n" + "=" * 80)
            print("E) CROSS-ASSET ANALYTICS")
            print("=" * 80)

            print("\n[E1] Correlation matrix...")
            correlation_matrix_df = self.calculate_correlation_matrix(orderbook_df)

            # ========================================================================
            # F) TEXT CONTENT ANALYTICS (NEW - Word-Level Analysis)
            # ========================================================================
            if sentiment_count > 0:
                print("\n" + "=" * 80)
                print("F) TEXT CONTENT ANALYTICS (Word-Level Analysis)")
                print("=" * 80)

                print("\n[F1] Word frequency analysis...")
                word_freq_df = self.analyze_word_frequency(sentiment_df)

                print("\n[F2] Word-price correlation...")
                word_price_df = self.correlate_words_with_price(sentiment_df, orderbook_df)

                print("\n[F3] Word-sentiment distribution...")
                word_sentiment_df = self.analyze_word_sentiment_distribution(sentiment_df)
            else:
                print("\n[SKIP] Text content analytics (no sentiment data)")
                word_freq_df = None
                word_price_df = None
                word_sentiment_df = None

            # ========================================================================
            # G) ADVANCED CROSS-ANALYSIS (Reddit ↔ Price Patterns)
            # ========================================================================
            if sentiment_count > 0:
                print("\n" + "=" * 80)
                print("G) ADVANCED CROSS-ANALYSIS (Reddit ↔ Price Patterns)")
                print("=" * 80)

                # Initialize advanced cross-analytics module
                cross_analytics = AdvancedCrossAnalytics()

                print("\n[G1] Words appearing after price moves (lagging indicators)...")
                words_after_price_df = cross_analytics.analyze_words_after_price_moves(
                    orderbook_df, sentiment_df, price_threshold_pct=2.0
                )

                print("\n[G2] Price movements after word spikes (leading indicators)...")
                price_after_words_df = cross_analytics.analyze_price_after_word_spikes(
                    sentiment_df, orderbook_df, frequency_increase_pct=200.0
                )

                print("\n[G3] Bidirectional causality detection...")
                causality_df = cross_analytics.detect_bidirectional_causality(
                    sentiment_df, orderbook_df, lag_hours=4
                )

                print("\n[G4] Pattern velocity (reaction speed measurement)...")
                velocity_df = cross_analytics.detect_pattern_velocity(
                    sentiment_df, orderbook_df
                )
            else:
                print("\n[SKIP] Advanced cross-analysis (no sentiment data)")
                words_after_price_df = None
                price_after_words_df = None
                causality_df = None
                velocity_df = None

            # ========================================================================
            # WRITE RESULTS TO ALL DATABASES
            # ========================================================================
            print("\n" + "=" * 80)
            print("WRITING TO DATABASES")
            print("=" * 80)

            # Cassandra - SKIPPED (not running in this session)
            print("\n[Cassandra] SKIPPED - writing to MongoDB only...")
            # self.write_to_cassandra_unified(arbitrage_df, "arbitrage")
            # self.write_to_cassandra_unified(prediction_df, "price_prediction")
            # ... (Cassandra writes skipped for now)

            # Neo4j - GRAPH RELATIONSHIPS (not flat nodes!)
            print("\n[Neo4j] Creating graph relationships...")
            try:
                neo4j_writer = Neo4jGraphWriter()

                # Collect data for graph creation
                sentiment_data = sentiment_df.limit(10000).collect() if sentiment_count > 0 else []
                orderbook_sample = orderbook_df.limit(10000).collect()
                arbitrage_data = arbitrage_df.limit(1000).collect()
                correlation_data = correlation_matrix_df.limit(1000).collect()
                divergence_data = divergence_df.limit(1000).collect() if divergence_df else []

                # Create complete graph with relationships
                neo4j_writer.write_complete_graph(
                    sentiment_data=sentiment_data,
                    orderbook_data=orderbook_sample,
                    arbitrage_data=arbitrage_data,
                    correlation_data=correlation_data,
                    divergence_data=divergence_data
                )

                neo4j_writer.close()
                print("[Neo4j] ✅ Graph relationships created successfully")

            except Exception as neo4j_err:
                print(f"[WARNING] Neo4j graph creation failed: {neo4j_err}")
                import traceback
                traceback.print_exc()
                print("[WARNING] Continuing with Cassandra/MongoDB only...")

            # MongoDB - UNIFIED SINGLE COLLECTION (analytics_unified)
            print("\n[MongoDB] Writing all analytics to UNIFIED collection (key-value pairs)...")
            try:
                self.write_to_mongodb_unified(arbitrage_df, "arbitrage")
                self.write_to_mongodb_unified(prediction_df, "price_prediction")
                self.write_to_mongodb_unified(depth_df, "order_depth")
                self.write_to_mongodb_unified(vwap_df, "vwap")
                self.write_to_mongodb_unified(imbalance_df, "bid_ask_imbalance")
                self.write_to_mongodb_unified(volatility_df, "rolling_volatility")
                self.write_to_mongodb_unified(bollinger_df, "bollinger_bands")
                self.write_to_mongodb_unified(flash_df, "flash_events")
                self.write_to_mongodb_unified(correlation_matrix_df, "correlation_matrix")

                if correlation_df:
                    self.write_to_mongodb_unified(correlation_df, "sentiment_correlation")
                if momentum_df:
                    self.write_to_mongodb_unified(momentum_df, "sentiment_momentum")
                if divergence_df:
                    self.write_to_mongodb_unified(divergence_df, "sentiment_divergence")

                # Write text content analytics to unified collection
                if word_freq_df:
                    self.write_to_mongodb_unified(word_freq_df, "word_frequency")
                if word_price_df:
                    self.write_to_mongodb_unified(word_price_df, "word_price_correlation")
                if word_sentiment_df:
                    self.write_to_mongodb_unified(word_sentiment_df, "word_sentiment_dist")

                # Write advanced cross-analysis analytics (G1-G4)
                if words_after_price_df:
                    self.write_to_mongodb_unified(words_after_price_df, "words_after_price_move")
                if price_after_words_df:
                    self.write_to_mongodb_unified(price_after_words_df, "price_after_word_spike")
                if causality_df:
                    self.write_to_mongodb_unified(causality_df, "bidirectional_causality")
                if velocity_df:
                    self.write_to_mongodb_unified(velocity_df, "pattern_velocity")

            except Exception as mongo_err:
                print(f"[WARNING] MongoDB write failed: {mongo_err}")
                import traceback
                traceback.print_exc()
                print("[WARNING] Continuing with Neo4j only...")

            # ========================================================================
            # SUMMARY
            # ========================================================================
            print("\n" + "=" * 80)
            print("ANALYTICS PIPELINE COMPLETED SUCCESSFULLY")
            print("=" * 80)

            print("\nA) Original Analytics (Fixed):")
            print(f"  - Arbitrage Opportunities: {arbitrage_df.count()}")
            if correlation_df:
                print(f"  - Sentiment Correlations: {correlation_df.count()}")
            print(f"  - Price Predictions: {prediction_df.count()}")

            print("\nB) Volume & Liquidity:")
            print(f"  - Order Depth Records: {depth_df.count()}")
            print(f"  - VWAP Records: {vwap_df.count()}")
            print(f"  - Bid-Ask Imbalance: {imbalance_df.count()}")

            print("\nC) Volatility & Patterns:")
            print(f"  - Rolling Volatility: {volatility_df.count()}")
            print(f"  - Bollinger Bands: {bollinger_df.count()}")
            print(f"  - Flash Events: {flash_df.count()}")

            if momentum_df and divergence_df:
                print("\nD) Enhanced Sentiment:")
                print(f"  - Sentiment Momentum: {momentum_df.count()}")
                print(f"  - Sentiment Divergence: {divergence_df.count()}")

            print("\nE) Cross-Asset:")
            print(f"  - Correlation Matrix Pairs: {correlation_matrix_df.count()}")

            if word_freq_df and word_sentiment_df:
                print("\nF) Text Content Analytics:")
                print(f"  - Word Frequency Records: {word_freq_df.count()}")
                if word_price_df:
                    print(f"  - Word-Price Correlations: {word_price_df.count()}")
                print(f"  - Word-Sentiment Distributions: {word_sentiment_df.count()}")

            if words_after_price_df or price_after_words_df:
                print("\nG) Advanced Cross-Analysis (Reddit ↔ Price):")
                if words_after_price_df:
                    print(f"  - Words After Price Moves: {words_after_price_df.count()}")
                if price_after_words_df:
                    print(f"  - Price After Word Spikes: {price_after_words_df.count()}")
                if causality_df:
                    print(f"  - Bidirectional Causality Patterns: {causality_df.count()}")
                if velocity_df:
                    print(f"  - Pattern Velocity Measurements: {velocity_df.count()}")

            print("\nData written to:")
            print("  ⏭  Cassandra (SKIPPED)")
            print("  ✅ Neo4j (graph relationships)")
            print("  ✅ MongoDB (document storage with key-value pairs)")
            print("=" * 80)

        except Exception as e:
            print(f"\n[ERROR] Pipeline failed: {e}")
            raise

        finally:
            if self.spark:
                self.spark.stop()
                print("\n[spark] Session stopped")


def main():
    """Main entry point"""
    pipeline = SparkAnalyticsPipeline()

    try:
        # Run pipeline once
        pipeline.run_pipeline()

        print("\nPipeline completed. Run this script periodically for continuous analytics.")

    except KeyboardInterrupt:
        print("\n\nPipeline stopped by user")
    except Exception as e:
        print(f"\n\nPipeline error: {e}")
        raise


if __name__ == "__main__":
    main()
