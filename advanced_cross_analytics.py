#!/usr/bin/env python3
"""
ADVANCED CROSS-ANALYSIS: Reddit ↔ Price Patterns
Captures time-lagged relationships and causal patterns
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, window, lag, lead, avg, count, sum as spark_sum,
    when, lit, abs as spark_abs, concat_ws, collect_list,
    unix_timestamp, from_unixtime, expr, explode, split,
    lower, regexp_replace, length, trim, row_number, dense_rank, first
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, LongType, StringType
from datetime import timedelta


class AdvancedCrossAnalytics:
    """
    Advanced time-lagged cross-analysis between Reddit and market data
    """

    # ==========================================================================
    # G1: LAGGING WORD ANALYSIS (Price Movement → Reddit Words)
    # ==========================================================================

    def analyze_words_after_price_moves(self, orderbook_df: DataFrame,
                                        sentiment_df: DataFrame,
                                        price_threshold_pct: float = 2.0) -> DataFrame:
        """
        G1: After a significant price move, what words appear on Reddit?

        Pattern: Price pumps/dumps → Specific words spike
        Use case: Understand market reaction language

        Example: BTC +5% → "moon", "lambo", "hodl" spike in next 1-4 hours
        """
        print(f"[G1] Analyzing words after {price_threshold_pct}% price moves...")

        # Step 1: Detect significant price movements (minute-level)
        crypto_df = orderbook_df.filter(col("venue_type") == "SPOT")

        # Calculate 1-hour rolling price changes (use lag without rangeBetween)
        price_window = Window.partitionBy("symbol", "exchange").orderBy(col("timestamp").cast("long"))

        price_moves = crypto_df.withColumn(
            "price_1h_ago",
            lag("mid_price", 1).over(price_window)
        ).withColumn(
            "price_change_pct",
            ((col("mid_price") - col("price_1h_ago")) / col("price_1h_ago") * 100)
        )

        # Filter significant moves
        significant_moves = price_moves.filter(
            spark_abs(col("price_change_pct")) >= price_threshold_pct
        ).withColumn(
            "move_type",
            when(col("price_change_pct") > 0, "PUMP").otherwise("DUMP")
        ).select(
            "timestamp",
            "symbol",
            "base",
            "mid_price",
            "price_change_pct",
            "move_type"
        )

        # Step 2: Tokenize Reddit text
        words_df = self._tokenize_sentiment(sentiment_df)

        # Step 3: For each price move, find words in next 1hr, 4hr, 24hr
        # Using window joins with time boundaries

        # Join on time windows (price move timestamp < word timestamp < price move + lag)
        cross_analysis = significant_moves.alias("pm").join(
            words_df.alias("w"),
            # Join condition: word appears AFTER price move within time window
            expr("""
                w.timestamp BETWEEN pm.timestamp
                AND (pm.timestamp + INTERVAL 4 HOURS)
            """),
            "inner"
        )

        # Aggregate words by price move and time lag
        word_patterns = cross_analysis.withColumn(
            "time_lag_seconds",
            unix_timestamp(col("w.timestamp")) - unix_timestamp(col("pm.timestamp"))
        ).withColumn(
            "time_lag_bucket",
            when(col("time_lag_seconds") <= 3600, "0-1hr")
            .when(col("time_lag_seconds") <= 14400, "1-4hr")
            .otherwise("4-24hr")
        ).groupBy(
            col("pm.symbol"),
            col("pm.move_type"),
            col("pm.timestamp").alias("price_move_time"),
            col("pm.price_change_pct"),
            col("time_lag_bucket"),
            col("w.word")
        ).agg(
            count("*").alias("word_frequency"),
            avg("w.sentiment_polarity").alias("avg_sentiment")
        )

        # Rank words by frequency for each price move
        window_rank = Window.partitionBy(
            "symbol", "price_move_time", "time_lag_bucket"
        ).orderBy(col("word_frequency").desc())

        result = word_patterns.withColumn(
            "word_rank",
            row_number().over(window_rank)
        ).filter(
            col("word_rank") <= 20  # Top 20 words per move
        ).withColumn(
            "analytics_type", lit("words_after_price_move")
        )

        print(f"  [OK] Found {result.count()} word-after-price patterns")
        return result


    # ==========================================================================
    # G2: LEADING WORD ANALYSIS (Reddit Word Spike → Price Movement)
    # ==========================================================================

    def analyze_price_after_word_spikes(self, sentiment_df: DataFrame,
                                       orderbook_df: DataFrame,
                                       frequency_increase_pct: float = 200.0) -> DataFrame:
        """
        G2: When a word frequency spikes, does price follow?

        Pattern: Word spike → Price movement
        Use case: Predictive indicators

        Example: "crash" mentions +300% → BTC -3% in next 2 hours
        """
        print(f"[G2] Analyzing price moves after {frequency_increase_pct}% word spikes...")

        # Step 1: Detect word frequency spikes
        words_df = self._tokenize_sentiment(sentiment_df)

        # Calculate hourly word frequencies
        hourly_words = words_df.groupBy(
            window(col("timestamp"), "1 hour").alias("hour_window"),
            col("subreddit"),
            col("word")
        ).agg(
            count("*").alias("word_frequency"),
            avg("sentiment_polarity").alias("avg_sentiment")
        ).withColumn(
            "timestamp",
            col("hour_window.start")
        ).drop("hour_window")

        # Compare with previous hour to detect spikes
        window_prev = Window.partitionBy("subreddit", "word").orderBy("timestamp")

        word_spikes = hourly_words.withColumn(
            "prev_frequency",
            lag("word_frequency").over(window_prev)
        ).withColumn(
            "frequency_change_pct",
            when(col("prev_frequency") > 0,
                 (col("word_frequency") - col("prev_frequency")) / col("prev_frequency") * 100
            ).otherwise(0)
        ).filter(
            col("frequency_change_pct") >= frequency_increase_pct
        ).withColumn(
            "spike_type",
            when(col("avg_sentiment") > 0.2, "BULLISH_SPIKE")
            .when(col("avg_sentiment") < -0.2, "BEARISH_SPIKE")
            .otherwise("NEUTRAL_SPIKE")
        )

        # Step 2: Get price movements
        crypto_df = orderbook_df.filter(col("venue_type") == "SPOT")

        # For each word spike, find price changes in next 1-4 hours
        cross_analysis = word_spikes.alias("ws").join(
            crypto_df.alias("p"),
            expr("""
                p.timestamp BETWEEN ws.timestamp
                AND (ws.timestamp + INTERVAL 4 HOURS)
            """),
            "inner"
        )

        # Calculate price change after word spike
        window_price = Window.partitionBy(
            "ws.timestamp", "ws.word", "p.symbol"
        ).orderBy("p.timestamp")

        price_patterns = cross_analysis.withColumn(
            "initial_price",
            first("p.mid_price").over(window_price)
        ).withColumn(
            "time_lag_seconds",
            unix_timestamp(col("p.timestamp")) - unix_timestamp(col("ws.timestamp"))
        ).withColumn(
            "time_lag_bucket",
            when(col("time_lag_seconds") <= 3600, "0-1hr")
            .when(col("time_lag_seconds") <= 14400, "1-4hr")
            .otherwise("4-24hr")
        ).groupBy(
            col("ws.word"),
            col("ws.spike_type"),
            col("ws.timestamp").alias("word_spike_time"),
            col("ws.frequency_change_pct").alias("word_frequency_change_pct"),
            col("p.symbol"),
            col("time_lag_bucket")
        ).agg(
            avg(
                (col("p.mid_price") - col("initial_price")) / col("initial_price") * 100
            ).alias("avg_price_change_pct"),
            count("*").alias("sample_count")
        ).withColumn(
            "prediction_accuracy",
            when(
                (col("spike_type") == "BULLISH_SPIKE") & (col("avg_price_change_pct") > 0), "CORRECT"
            ).when(
                (col("spike_type") == "BEARISH_SPIKE") & (col("avg_price_change_pct") < 0), "CORRECT"
            ).otherwise("INCORRECT")
        ).withColumn(
            "analytics_type", lit("price_after_word_spike")
        )

        print(f"  [OK] Found {price_patterns.count()} word-spike-to-price patterns")
        return price_patterns


    # ==========================================================================
    # G3: BIDIRECTIONAL CAUSALITY DETECTION
    # ==========================================================================

    def detect_bidirectional_causality(self, sentiment_df: DataFrame,
                                      orderbook_df: DataFrame,
                                      lag_hours: int = 4) -> DataFrame:
        """
        G3: Detect if word ↔ price relationship is bidirectional or one-way

        Tests:
        1. Does word spike predict price? (word → price)
        2. Does price move predict word? (price → word)
        3. Both? (bidirectional)
        4. Neither? (no relationship)
        """
        print(f"[G3] Detecting bidirectional causality with {lag_hours}hr lag...")

        # Get word spikes and price moves
        words_df = self._tokenize_sentiment(sentiment_df)
        crypto_df = orderbook_df.filter(col("venue_type") == "SPOT")

        # Create 15-minute buckets for alignment
        words_bucketed = words_df.withColumn(
            "time_bucket",
            from_unixtime(
                (unix_timestamp(col("timestamp")) / 900).cast("long") * 900
            ).cast("timestamp")
        ).groupBy(
            "time_bucket", "word"
        ).agg(
            count("*").alias("word_count"),
            avg("sentiment_polarity").alias("avg_sentiment")
        )

        price_bucketed = crypto_df.withColumn(
            "time_bucket",
            from_unixtime(
                (unix_timestamp(col("timestamp")) / 900).cast("long") * 900
            ).cast("timestamp")
        ).groupBy(
            "time_bucket", "symbol"
        ).agg(
            avg("mid_price").alias("avg_price")
        )

        # Calculate changes
        window_spec = Window.partitionBy("word").orderBy("time_bucket")
        words_changes = words_bucketed.withColumn(
            "word_count_prev",
            lag("word_count", lag_hours * 4).over(window_spec)  # 4 buckets per hour
        ).withColumn(
            "word_change_pct",
            when(col("word_count_prev") > 0,
                 (col("word_count") - col("word_count_prev")) / col("word_count_prev") * 100
            ).otherwise(0)
        )

        window_spec_price = Window.partitionBy("symbol").orderBy("time_bucket")
        price_changes = price_bucketed.withColumn(
            "price_prev",
            lag("avg_price", lag_hours * 4).over(window_spec_price)
        ).withColumn(
            "price_change_pct",
            when(col("price_prev") > 0,
                 (col("avg_price") - col("price_prev")) / col("price_prev") * 100
            ).otherwise(0)
        )

        # Join words and prices
        combined = words_changes.alias("w").join(
            price_changes.alias("p"),
            col("w.time_bucket") == col("p.time_bucket"),
            "inner"
        )

        # Detect causality patterns
        causality = combined.withColumn(
            "word_spike",
            when(spark_abs(col("word_change_pct")) > 100, True).otherwise(False)
        ).withColumn(
            "price_move",
            when(spark_abs(col("price_change_pct")) > 2, True).otherwise(False)
        ).groupBy("word", "symbol").agg(
            # Count co-occurrences
            spark_sum(
                when((col("word_spike") == True) & (col("price_move") == True), 1).otherwise(0)
            ).alias("both_move"),
            spark_sum(
                when((col("word_spike") == True) & (col("price_move") == False), 1).otherwise(0)
            ).alias("word_only"),
            spark_sum(
                when((col("word_spike") == False) & (col("price_move") == True), 1).otherwise(0)
            ).alias("price_only"),
            count("*").alias("total_observations")
        ).withColumn(
            "causality_type",
            when(
                (col("both_move") / col("total_observations") > 0.3) &
                (col("word_only") / col("total_observations") < 0.2) &
                (col("price_only") / col("total_observations") < 0.2),
                "BIDIRECTIONAL"
            ).when(
                col("word_only") / col("total_observations") > 0.3,
                "WORD_LEADS_PRICE"
            ).when(
                col("price_only") / col("total_observations") > 0.3,
                "PRICE_LEADS_WORD"
            ).otherwise("NO_CLEAR_RELATIONSHIP")
        ).withColumn(
            "analytics_type", lit("bidirectional_causality")
        ).withColumn(
            "timestamp", from_unixtime(unix_timestamp()).cast("timestamp")
        )

        print(f"  [OK] Analyzed causality for {causality.count()} word-symbol pairs")
        return causality


    # ==========================================================================
    # G4: PATTERN VELOCITY DETECTION
    # ==========================================================================

    def detect_pattern_velocity(self, sentiment_df: DataFrame,
                               orderbook_df: DataFrame) -> DataFrame:
        """
        G4: Measure the SPEED of word → price or price → word reactions

        Answers: How quickly does the market react to words, or words to market?
        """
        print("[G4] Detecting pattern velocity (reaction speed)...")

        words_df = self._tokenize_sentiment(sentiment_df)
        crypto_df = orderbook_df.filter(col("venue_type") == "SPOT")

        # For each significant word mention, find the FIRST price reaction
        significant_words = words_df.filter(
            col("word").isin("crash", "moon", "dump", "pump", "bull", "bear")
        )

        # Join with future prices
        velocity_analysis = significant_words.alias("w").join(
            crypto_df.alias("p"),
            expr("p.timestamp > w.timestamp AND p.timestamp < (w.timestamp + INTERVAL 6 HOURS)"),
            "inner"
        )

        # Find time to first 1% move
        window_first_move = Window.partitionBy(
            "w.timestamp", "w.word", "p.symbol"
        ).orderBy("p.timestamp")

        velocity = velocity_analysis.withColumn(
            "initial_price",
            first("p.mid_price").over(window_first_move)
        ).withColumn(
            "price_change_pct",
            (col("p.mid_price") - col("initial_price")) / col("initial_price") * 100
        ).filter(
            spark_abs(col("price_change_pct")) >= 1.0  # 1% move threshold
        ).withColumn(
            "reaction_time_seconds",
            unix_timestamp(col("p.timestamp")) - unix_timestamp(col("w.timestamp"))
        ).groupBy(
            col("w.word"),
            col("p.symbol")
        ).agg(
            avg("reaction_time_seconds").alias("avg_reaction_time_seconds"),
            (avg("reaction_time_seconds") / 60).alias("avg_reaction_time_minutes"),
            count("*").alias("occurrence_count")
        ).withColumn(
            "reaction_speed",
            when(col("avg_reaction_time_minutes") < 15, "VERY_FAST")
            .when(col("avg_reaction_time_minutes") < 60, "FAST")
            .when(col("avg_reaction_time_minutes") < 180, "MODERATE")
            .otherwise("SLOW")
        ).withColumn(
            "analytics_type", lit("pattern_velocity")
        ).withColumn(
            "timestamp", from_unixtime(unix_timestamp()).cast("timestamp")
        )

        print(f"  [OK] Measured velocity for {velocity.count()} word-symbol pairs")
        return velocity


    # ==========================================================================
    # HELPER METHODS
    # ==========================================================================

    def _tokenize_sentiment(self, sentiment_df: DataFrame) -> DataFrame:
        """Helper: Tokenize text into words with sentiment"""
        # Clean and tokenize
        words_df = sentiment_df.withColumn(
            "cleaned_text",
            lower(regexp_replace(col("text"), "[^a-zA-Z0-9\\s]", " "))
        ).withColumn(
            "words_array",
            split(trim(col("cleaned_text")), "\\s+")
        ).withColumn(
            "word",
            explode(col("words_array"))
        ).filter(
            length(col("word")) >= 3
        ).withColumn(
            "sentiment_polarity",
            when(col("sentiment.polarity").isNotNull(),
                 col("sentiment.polarity").cast(DoubleType()))
            .otherwise(0.0)
        ).select(
            from_unixtime(col("timestamp")).cast("timestamp").alias("timestamp"),
            "subreddit",
            "word",
            "sentiment_polarity"
        )

        return words_df
