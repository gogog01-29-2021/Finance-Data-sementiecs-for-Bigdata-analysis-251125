#!/usr/bin/env python3
"""
TIMEZONE-AWARE CORRELATION ANALYSIS
1. Same Timezone: MapReduce pattern mining (key-value analysis)
2. Cross Timezone: Lead-lag correlation (predictive)
"""

import os
import json
import re
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, stddev, max as spark_max, min as spark_min, sum as spark_sum,
    window, when, lit, count, explode, hour, dayofweek, date_format,
    udf, split, lag, lead, expr, unix_timestamp, from_unixtime,
    collect_list, struct, map_from_entries, create_map, array
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    ArrayType, MapType, IntegerType, TimestampType
)
from pyspark.sql.window import Window


# ============================================================================
# TIMEZONE CONFIGURATION (ASSUMPTIONS)
# ============================================================================

TIMEZONE_CONFIG = {
    # Subreddit → Timezone mapping
    'subreddit_timezone': {
        'BitcoinKR': 'KR',
        'KoreanFinance': 'KR',
        'Bitcoin': 'GLOBAL',
        'CryptoCurrency': 'GLOBAL',
        'Economics': 'GLOBAL',
        'stockMarket': 'GLOBAL',
        'investing': 'GLOBAL',
        'stocks': 'GLOBAL',
        'SatoshiStreetBets': 'GLOBAL',
    },

    # Exchange → Timezone mapping (already in your data as 'region' field)
    'exchange_timezone': {
        'upbit': 'KR',
        'bithumb': 'KR',
        'coinone': 'KR',
        'korbit': 'KR',
        'binance': 'GLOBAL',
        'okx': 'GLOBAL',
        'bybit': 'GLOBAL',
    },

    # Trading session definitions (UTC hours)
    'trading_sessions': {
        'KR': {
            'peak_start': 0,    # UTC 00:00 = 9 AM KST
            'peak_end': 8,      # UTC 08:00 = 5 PM KST
            'timezone_offset': 9,
            'label': 'Korea (UTC+9)'
        },
        'GLOBAL': {
            'peak_start': 13,   # UTC 13:00 = 9 AM EST
            'peak_end': 21,     # UTC 21:00 = 5 PM EST
            'timezone_offset': -5,
            'label': 'US/Global (UTC-5)'
        }
    }
}


# ============================================================================
# PART 1: SAME TIMEZONE - MAPREDUCE PATTERN MINING
# ============================================================================

class SameTimezoneMapReduce:
    """
    MapReduce-style analysis for SAME timezone
    Finds patterns in key-value pairs without time lag
    """

    def __init__(self, spark):
        self.spark = spark

    def map_sentiment_to_keys(self, sentiment_df):
        """
        MAP PHASE: Transform sentiment data into (key, value) pairs

        Key dimensions:
        - timezone (KR/GLOBAL)
        - asset (extracted from text)
        - sentiment_tier (STRONG_POSITIVE, POSITIVE, NEUTRAL, etc.)
        - hour_of_day (0-23 UTC)
        - day_of_week (Mon-Sun)

        Value:
        - sentiment_polarity
        - post_count
        """
        print("\n[MapReduce] MAP PHASE: Creating sentiment key-value pairs...")

        # Assign timezone based on subreddit
        timezone_map_expr = create_map(
            *[lit(x) for kv in TIMEZONE_CONFIG['subreddit_timezone'].items() for x in kv]
        )

        sentiment_with_tz = sentiment_df.withColumn(
            "timezone",
            timezone_map_expr[col("subreddit")]
        ).filter(col("timezone").isNotNull())

        # Convert timestamp
        sentiment_with_tz = sentiment_with_tz.withColumn(
            "timestamp",
            from_unixtime(col("timestamp"))
        )

        # Extract temporal features
        sentiment_with_tz = sentiment_with_tz.withColumn(
            "hour_utc",
            hour(col("timestamp"))
        ).withColumn(
            "day_of_week",
            dayofweek(col("timestamp"))
        )

        # Extract sentiment polarity and tier
        sentiment_with_tz = sentiment_with_tz.withColumn(
            "sentiment_polarity",
            col("sentiment.polarity").cast(DoubleType())
        ).withColumn(
            "sentiment_tier",
            when(col("sentiment_polarity") > 0.5, "STRONG_POSITIVE")
            .when(col("sentiment_polarity") > 0.1, "POSITIVE")
            .when(col("sentiment_polarity") < -0.5, "STRONG_NEGATIVE")
            .when(col("sentiment_polarity") < -0.1, "NEGATIVE")
            .otherwise("NEUTRAL")
        )

        # Extract mentioned assets
        def extract_assets(text):
            if not text:
                return ["GENERAL"]

            text_upper = text.upper()
            patterns = [
                r'\b(BTC|BITCOIN)\b',
                r'\b(ETH|ETHEREUM)\b',
                r'\b(SOL|SOLANA)\b',
                r'\b(XRP|RIPPLE)\b',
                r'\b(ADA|CARDANO)\b',
                r'\b(DOGE|DOGECOIN)\b',
                r'\$([A-Z]{2,5})\b'
            ]

            assets = []
            for pattern in patterns:
                matches = re.findall(pattern, text_upper)
                assets.extend(matches)

            # Normalize
            asset_map = {
                'BITCOIN': 'BTC', 'ETHEREUM': 'ETH', 'SOLANA': 'SOL',
                'RIPPLE': 'XRP', 'CARDANO': 'ADA', 'DOGECOIN': 'DOGE'
            }
            assets = [asset_map.get(a, a) for a in assets]

            return list(set(assets)) if assets else ["GENERAL"]

        extract_assets_udf = udf(extract_assets, ArrayType(StringType()))

        sentiment_mapped = sentiment_with_tz.withColumn(
            "mentioned_assets",
            extract_assets_udf(col("text"))
        ).withColumn(
            "asset",
            explode(col("mentioned_assets"))
        )

        # Create composite KEY
        sentiment_mapped = sentiment_mapped.withColumn(
            "composite_key",
            struct(
                col("timezone").alias("tz"),
                col("asset"),
                col("sentiment_tier"),
                col("hour_utc"),
                col("day_of_week")
            )
        )

        # VALUE = sentiment metrics
        sentiment_mapped = sentiment_mapped.select(
            col("composite_key"),
            col("sentiment_polarity").alias("value_polarity"),
            col("subreddit").alias("value_subreddit"),
            lit(1).alias("value_count")
        )

        print(f"[MapReduce] Mapped {sentiment_mapped.count()} sentiment key-value pairs")
        return sentiment_mapped


    def map_prices_to_keys(self, orderbook_df):
        """
        MAP PHASE: Transform price data into (key, value) pairs

        Key dimensions:
        - timezone (from region field)
        - asset (base currency)
        - hour_of_day
        - day_of_week

        Value:
        - price_change_pct
        - volatility
        """
        print("\n[MapReduce] MAP PHASE: Creating price key-value pairs...")

        # Use existing 'region' field as timezone
        price_with_tz = orderbook_df.withColumn(
            "timezone",
            col("region")  # Already KR or GLOBAL
        )

        # Extract temporal features
        price_with_tz = price_with_tz.withColumn(
            "hour_utc",
            hour(col("timestamp"))
        ).withColumn(
            "day_of_week",
            dayofweek(col("timestamp"))
        )

        # Calculate price metrics per (asset, exchange, hour) window
        window_spec = Window.partitionBy("base", "exchange", "hour_utc", "day_of_week").orderBy("timestamp")

        price_with_metrics = price_with_tz.withColumn(
            "prev_price",
            lag("mid_price").over(window_spec)
        ).withColumn(
            "price_change_pct",
            when(col("prev_price").isNotNull(),
                 ((col("mid_price") - col("prev_price")) / col("prev_price")) * 100)
            .otherwise(0)
        )

        # Create composite KEY (same structure as sentiment)
        price_mapped = price_with_metrics.withColumn(
            "composite_key",
            struct(
                col("timezone").alias("tz"),
                col("base").alias("asset"),
                lit("PRICE").alias("sentiment_tier"),  # Placeholder for join
                col("hour_utc"),
                col("day_of_week")
            )
        )

        # VALUE = price metrics
        price_mapped = price_mapped.select(
            col("composite_key"),
            col("price_change_pct").alias("value_price_change"),
            col("spread_bps").alias("value_spread"),
            col("exchange").alias("value_exchange"),
            lit(1).alias("value_count")
        )

        print(f"[MapReduce] Mapped {price_mapped.count()} price key-value pairs")
        return price_mapped


    def reduce_same_timezone(self, sentiment_kv, price_kv):
        """
        REDUCE PHASE: Aggregate by key to find patterns

        For SAME timezone, group by:
        - (timezone, asset, hour) → Find hourly patterns
        - (timezone, asset, sentiment_tier) → Sentiment impact
        - (timezone, day_of_week) → Weekly patterns
        """
        print("\n[MapReduce] REDUCE PHASE: Aggregating same-timezone patterns...")

        # REDUCE 1: By (timezone, asset, hour)
        hourly_sentiment = sentiment_kv.groupBy(
            col("composite_key.tz").alias("timezone"),
            col("composite_key.asset").alias("asset"),
            col("composite_key.hour_utc").alias("hour")
        ).agg(
            avg("value_polarity").alias("avg_sentiment"),
            stddev("value_polarity").alias("sentiment_std"),
            spark_sum("value_count").alias("post_volume")
        )

        hourly_prices = price_kv.groupBy(
            col("composite_key.tz").alias("timezone"),
            col("composite_key.asset").alias("asset"),
            col("composite_key.hour_utc").alias("hour")
        ).agg(
            avg("value_price_change").alias("avg_price_change"),
            stddev("value_price_change").alias("price_volatility"),
            avg("value_spread").alias("avg_spread")
        )

        # JOIN on (timezone, asset, hour) - SAME TIME
        hourly_correlation = hourly_sentiment.join(
            hourly_prices,
            ["timezone", "asset", "hour"],
            "inner"
        )

        print(f"[MapReduce] REDUCE: Found {hourly_correlation.count()} hourly patterns")

        # REDUCE 2: By (timezone, sentiment_tier)
        tier_sentiment = sentiment_kv.groupBy(
            col("composite_key.tz").alias("timezone"),
            col("composite_key.sentiment_tier").alias("sentiment_tier")
        ).agg(
            avg("value_polarity").alias("avg_sentiment"),
            spark_sum("value_count").alias("total_posts")
        )

        print(f"[MapReduce] REDUCE: Found {tier_sentiment.count()} sentiment tier patterns")

        # REDUCE 3: By (timezone, day_of_week)
        weekly_sentiment = sentiment_kv.groupBy(
            col("composite_key.tz").alias("timezone"),
            col("composite_key.day_of_week").alias("day_of_week")
        ).agg(
            avg("value_polarity").alias("avg_sentiment"),
            spark_sum("value_count").alias("post_volume")
        )

        weekly_prices = price_kv.groupBy(
            col("composite_key.tz").alias("timezone"),
            col("composite_key.day_of_week").alias("day_of_week")
        ).agg(
            avg("value_price_change").alias("avg_price_change")
        )

        weekly_correlation = weekly_sentiment.join(
            weekly_prices,
            ["timezone", "day_of_week"],
            "inner"
        )

        print(f"[MapReduce] REDUCE: Found {weekly_correlation.count()} weekly patterns")

        return {
            'hourly': hourly_correlation,
            'tier': tier_sentiment,
            'weekly': weekly_correlation
        }


# ============================================================================
# PART 2: CROSS TIMEZONE - LEAD-LAG CORRELATION
# ============================================================================

class CrossTimezoneLeadLag:
    """
    Cross-timezone lead-lag analysis
    Tests if sentiment in one timezone predicts prices in another
    """

    def __init__(self, spark):
        self.spark = spark

    def create_lagged_correlations(self, sentiment_df, orderbook_df, lag_hours=[2, 4, 8, 12]):
        """
        Create lead-lag correlations with multiple time lags

        Examples:
        - Korean Reddit (T) → Korean Prices (T + 2h)
        - Korean Reddit (T) → Global Prices (T + 12h)
        - Global Reddit (T) → Korean Prices (T + 6h)
        """
        print("\n[Lead-Lag] Creating cross-timezone correlations...")

        # Assign timezones
        timezone_map = create_map(
            *[lit(x) for kv in TIMEZONE_CONFIG['subreddit_timezone'].items() for x in kv]
        )

        sentiment_with_tz = sentiment_df.withColumn(
            "timezone",
            timezone_map[col("subreddit")]
        ).withColumn(
            "timestamp",
            from_unixtime(col("timestamp"))
        ).withColumn(
            "sentiment_polarity",
            col("sentiment.polarity").cast(DoubleType())
        )

        # Extract assets from sentiment
        def extract_base_asset(text):
            if not text:
                return None
            text_upper = text.upper()
            if 'BTC' in text_upper or 'BITCOIN' in text_upper:
                return 'BTC'
            elif 'ETH' in text_upper or 'ETHEREUM' in text_upper:
                return 'ETH'
            elif 'SOL' in text_upper or 'SOLANA' in text_upper:
                return 'SOL'
            return None

        extract_asset_udf = udf(extract_base_asset, StringType())

        sentiment_with_asset = sentiment_with_tz.withColumn(
            "asset",
            extract_asset_udf(col("text"))
        ).filter(col("asset").isNotNull())

        # Aggregate sentiment by 1-hour windows
        sentiment_agg = sentiment_with_asset.groupBy(
            window(col("timestamp"), "1 hour"),
            col("timezone").alias("sentiment_tz"),
            col("asset")
        ).agg(
            avg("sentiment_polarity").alias("avg_sentiment"),
            count("*").alias("post_count")
        ).select(
            col("window.start").alias("sentiment_time"),
            col("sentiment_tz"),
            col("asset"),
            col("avg_sentiment"),
            col("post_count")
        )

        # Aggregate prices by 1-hour windows
        price_agg = orderbook_df.groupBy(
            window(col("timestamp"), "1 hour"),
            col("region").alias("price_tz"),
            col("base").alias("asset")
        ).agg(
            avg("mid_price").alias("avg_price"),
            spark_max("mid_price").alias("max_price"),
            spark_min("mid_price").alias("min_price")
        ).select(
            col("window.start").alias("price_time"),
            col("price_tz"),
            col("asset"),
            col("avg_price"),
            ((col("max_price") - col("min_price")) / col("avg_price") * 100).alias("price_volatility")
        )

        # Create lead-lag correlations for each lag period
        all_correlations = []

        for lag_h in lag_hours:
            print(f"[Lead-Lag] Testing {lag_h}-hour lag...")

            # Join: sentiment(T) with price(T + lag)
            lagged_corr = sentiment_agg.join(
                price_agg,
                (sentiment_agg.asset == price_agg.asset) &
                (sentiment_agg.sentiment_time + expr(f"INTERVAL {lag_h} HOURS") == price_agg.price_time),
                "inner"
            ).select(
                sentiment_agg.sentiment_time,
                col("sentiment_tz"),
                col("price_tz"),
                sentiment_agg.asset,
                col("avg_sentiment"),
                col("post_count"),
                col("avg_price"),
                col("price_volatility"),
                lit(lag_h).alias("lag_hours")
            )

            corr_count = lagged_corr.count()
            print(f"[Lead-Lag] {lag_h}h lag: {corr_count} correlations found")

            all_correlations.append(lagged_corr)

        # Union all lag periods
        if all_correlations:
            combined = all_correlations[0]
            for df in all_correlations[1:]:
                combined = combined.union(df)

            return combined

        return None


# ============================================================================
# MAIN ANALYSIS PIPELINE
# ============================================================================

class TimezoneCorrelationPipeline:
    """Main pipeline combining both analyses"""

    def __init__(self, spark):
        self.spark = spark
        self.same_tz = SameTimezoneMapReduce(spark)
        self.cross_tz = CrossTimezoneLeadLag(spark)

    def run(self, sentiment_df, orderbook_df):
        """
        Run complete timezone-aware correlation analysis
        """
        print("=" * 80)
        print("TIMEZONE-AWARE CORRELATION ANALYSIS")
        print("=" * 80)

        # PART 1: Same Timezone MapReduce
        print("\n" + "=" * 80)
        print("PART 1: SAME TIMEZONE PATTERN MINING (MapReduce)")
        print("=" * 80)

        sentiment_kv = self.same_tz.map_sentiment_to_keys(sentiment_df)
        price_kv = self.same_tz.map_prices_to_keys(orderbook_df)
        same_tz_patterns = self.same_tz.reduce_same_timezone(sentiment_kv, price_kv)

        # Display results
        print("\n[Results] Hourly Patterns (same timezone, same time):")
        same_tz_patterns['hourly'].orderBy("timezone", "hour").show(20, truncate=False)

        print("\n[Results] Sentiment Tier Patterns:")
        same_tz_patterns['tier'].show(10, truncate=False)

        print("\n[Results] Weekly Patterns:")
        same_tz_patterns['weekly'].show(14, truncate=False)

        # PART 2: Cross Timezone Lead-Lag
        print("\n" + "=" * 80)
        print("PART 2: CROSS TIMEZONE LEAD-LAG CORRELATION")
        print("=" * 80)

        cross_tz_correlations = self.cross_tz.create_lagged_correlations(
            sentiment_df,
            orderbook_df,
            lag_hours=[2, 4, 8, 12]
        )

        if cross_tz_correlations:
            print("\n[Results] Lead-Lag Correlations:")
            cross_tz_correlations.orderBy("lag_hours", "sentiment_time").show(20, truncate=False)

            # Summary by lag and timezone pair
            print("\n[Results] Summary by Lag Period and Timezone Pair:")
            cross_tz_correlations.groupBy("lag_hours", "sentiment_tz", "price_tz").agg(
                count("*").alias("correlation_count"),
                avg("avg_sentiment").alias("mean_sentiment"),
                avg("price_volatility").alias("mean_volatility")
            ).orderBy("lag_hours", "sentiment_tz", "price_tz").show(truncate=False)

        return {
            'same_timezone': same_tz_patterns,
            'cross_timezone': cross_tz_correlations
        }


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    print("Timezone Correlation Analysis - Ready to integrate with spark_analytics.py")
    print("\nThis module provides:")
    print("1. Same Timezone MapReduce: Find patterns within same region/time")
    print("2. Cross Timezone Lead-Lag: Test predictive power across regions")
    print("\nConfiguration:")
    print(f"  Timezones: {list(TIMEZONE_CONFIG['trading_sessions'].keys())}")
    print(f"  Lag periods: [2, 4, 8, 12] hours")
