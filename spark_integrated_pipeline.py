#!/usr/bin/env python3
"""
INTEGRATED SPARK ANALYTICS PIPELINE
Combines existing analytics with timezone-aware correlation
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

from pyspark.sql import SparkSession
from spark_analytics import SparkAnalyticsPipeline
from spark_timezone_correlation import TimezoneCorrelationPipeline


class IntegratedAnalyticsPipeline:
    """
    Combines:
    1. Original analytics (arbitrage, sentiment, prediction)
    2. Timezone-aware correlation (MapReduce + Lead-Lag)
    """

    def __init__(self):
        self.spark = None
        self.original_pipeline = None
        self.timezone_pipeline = None

    def initialize(self):
        """Initialize Spark session"""
        print("[integrated] Initializing Spark...")

        self.spark = SparkSession.builder \
            .appName("IntegratedFinancialAnalytics") \
            .master("local[*]") \
            .config("spark.jars.packages",
                   "org.postgresql:postgresql:42.5.0,"
                   "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                   "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0") \
            .config("spark.cassandra.connection.host", os.getenv("CASSANDRA_HOST", "localhost")) \
            .config("spark.cassandra.connection.port", os.getenv("CASSANDRA_PORT", "9042")) \
            .config("spark.mongodb.read.connection.uri", os.getenv("MONGODB_URI", "mongodb://localhost:27017")) \
            .config("spark.mongodb.write.connection.uri", os.getenv("MONGODB_URI", "mongodb://localhost:27017")) \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.sql.shuffle.partitions", "8") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")

        # Initialize sub-pipelines
        self.original_pipeline = SparkAnalyticsPipeline()
        self.original_pipeline.spark = self.spark

        self.timezone_pipeline = TimezoneCorrelationPipeline(self.spark)

        print("[integrated] Spark initialized")

    def run_complete_analysis(self):
        """
        Run all analytics:
        1. Original: Arbitrage, Sentiment, Prediction
        2. Timezone: MapReduce patterns + Lead-Lag correlation
        """
        print("\n" + "=" * 80)
        print("INTEGRATED ANALYTICS PIPELINE")
        print("=" * 80)

        try:
            # Read data sources
            print("\n[1] Reading data sources...")
            orderbook_df = self.original_pipeline.read_questdb_orderbook()
            sentiment_df = self.original_pipeline.read_lmdb_sentiment()

            # Check if we have enough data
            orderbook_count = orderbook_df.count()
            sentiment_count = sentiment_df.count()

            print(f"\n[Data Check] Orderbook records: {orderbook_count}")
            print(f"[Data Check] Sentiment records: {sentiment_count}")

            if orderbook_count == 0:
                print("\n[WARNING] No orderbook data found!")
                print("Make sure to run data collection first (python main.py)")
                return

            if sentiment_count == 0:
                print("\n[WARNING] No sentiment data found!")
                print("Timezone correlation will be limited to price-only analysis")

            # ========================================================================
            # ORIGINAL ANALYTICS
            # ========================================================================
            print("\n" + "=" * 80)
            print("ORIGINAL ANALYTICS")
            print("=" * 80)

            print("\n[2] Running arbitrage detection...")
            arbitrage_df = self.original_pipeline.detect_arbitrage(orderbook_df)

            if sentiment_count > 0:
                print("\n[3] Correlating sentiment with prices...")
                correlation_df = self.original_pipeline.correlate_sentiment_price(
                    orderbook_df, sentiment_df
                )
            else:
                correlation_df = None

            print("\n[4] Predicting price movements...")
            prediction_df = self.original_pipeline.predict_price_movement(orderbook_df)

            # ========================================================================
            # TIMEZONE-AWARE CORRELATION (NEW!)
            # ========================================================================
            if sentiment_count > 0:
                print("\n" + "=" * 80)
                print("TIMEZONE-AWARE CORRELATION ANALYSIS")
                print("=" * 80)

                timezone_results = self.timezone_pipeline.run(sentiment_df, orderbook_df)

                # ========================================================================
                # WRITE RESULTS TO ALL DATABASES
                # ========================================================================
                print("\n[5] Writing results to ALL databases...")

                # ============ CASSANDRA - Time-series storage ============
                print("\n[Cassandra] Writing ALL analytics...")
                self.original_pipeline.write_to_cassandra(arbitrage_df, "arbitrage_opportunities")
                self.original_pipeline.write_to_cassandra(prediction_df, "price_predictions")
                if correlation_df:
                    self.original_pipeline.write_to_cassandra(correlation_df, "sentiment_correlations")

                # Timezone analytics to Cassandra
                if timezone_results['same_timezone']:
                    self.original_pipeline.write_to_cassandra(
                        timezone_results['same_timezone']['hourly'],
                        "timezone_hourly_patterns"
                    )
                    self.original_pipeline.write_to_cassandra(
                        timezone_results['same_timezone']['weekly'],
                        "timezone_weekly_patterns"
                    )
                if timezone_results['cross_timezone']:
                    self.original_pipeline.write_to_cassandra(
                        timezone_results['cross_timezone'],
                        "timezone_lead_lag_correlations"
                    )

                # ============ NEO4J - Graph storage ============
                print("\n[Neo4j] Writing ALL analytics...")
                self.original_pipeline.write_to_neo4j(arbitrage_df, "ArbitrageOpportunity")
                self.original_pipeline.write_to_neo4j(prediction_df, "PricePrediction")
                if correlation_df:
                    self.original_pipeline.write_to_neo4j(correlation_df, "SentimentCorrelation")

                # Timezone analytics to Neo4j
                if timezone_results['same_timezone']:
                    self.original_pipeline.write_to_neo4j(
                        timezone_results['same_timezone']['hourly'],
                        "TimezoneHourlyPattern"
                    )
                    self.original_pipeline.write_to_neo4j(
                        timezone_results['same_timezone']['weekly'],
                        "TimezoneWeeklyPattern"
                    )
                if timezone_results['cross_timezone']:
                    self.original_pipeline.write_to_neo4j(
                        timezone_results['cross_timezone'],
                        "TimezonLeadLagCorrelation"
                    )

                # ============ MONGODB - Document storage ============
                print("\n[MongoDB] Writing ALL analytics...")
                self.original_pipeline.write_to_mongodb(arbitrage_df, "arbitrage_opportunities")
                self.original_pipeline.write_to_mongodb(prediction_df, "price_predictions")
                if correlation_df:
                    self.original_pipeline.write_to_mongodb(correlation_df, "sentiment_correlations")

                # Timezone analytics to MongoDB
                if timezone_results['same_timezone']:
                    self.original_pipeline.write_to_mongodb(
                        timezone_results['same_timezone']['hourly'],
                        "timezone_hourly_patterns"
                    )
                    self.original_pipeline.write_to_mongodb(
                        timezone_results['same_timezone']['weekly'],
                        "timezone_weekly_patterns"
                    )
                if timezone_results['cross_timezone']:
                    self.original_pipeline.write_to_mongodb(
                        timezone_results['cross_timezone'],
                        "timezone_lead_lag_correlations"
                    )

                # ========================================================================
                # SUMMARY
                # ========================================================================
                print("\n" + "=" * 80)
                print("ANALYTICS PIPELINE COMPLETED")
                print("=" * 80)
                print("\nOriginal Analytics:")
                print(f"  Arbitrage Opportunities: {arbitrage_df.count()}")
                print(f"  Price Predictions: {prediction_df.count()}")
                if correlation_df:
                    print(f"  Sentiment Correlations: {correlation_df.count()}")

                print("\nTimezone-Aware Analytics:")
                if timezone_results['same_timezone']:
                    print(f"  Hourly Patterns: {timezone_results['same_timezone']['hourly'].count()}")
                    print(f"  Weekly Patterns: {timezone_results['same_timezone']['weekly'].count()}")
                if timezone_results['cross_timezone']:
                    print(f"  Lead-Lag Correlations: {timezone_results['cross_timezone'].count()}")

                print("\nData written to ALL 3 databases:")
                print("  ✅ Cassandra: ALL analytics (arbitrage, predictions, sentiment, timezone)")
                print("  ✅ Neo4j: ALL analytics as graph nodes")
                print("  ✅ MongoDB: ALL analytics as documents")
                print("=" * 80)

            else:
                print("\n[SKIP] Timezone correlation skipped (no sentiment data)")
                print("Only running price-based analytics")

        except Exception as e:
            print(f"\n[ERROR] Pipeline failed: {e}")
            import traceback
            traceback.print_exc()
            raise

        finally:
            if self.spark:
                self.spark.stop()
                print("\n[integrated] Spark session stopped")


def main():
    """Main entry point"""
    pipeline = IntegratedAnalyticsPipeline()

    try:
        pipeline.initialize()
        pipeline.run_complete_analysis()

        print("\n✓ Pipeline completed successfully!")
        print("\nView results:")
        print("  python query_all_data.py")

    except KeyboardInterrupt:
        print("\n\nPipeline stopped by user")
    except Exception as e:
        print(f"\n\nPipeline error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
