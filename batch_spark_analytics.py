#!/usr/bin/env python3
"""
BATCH SPARK ANALYTICS
PySpark-based analytics for multi-source financial data:
- Stock price analysis & technical indicators
- Economic indicator correlation analysis
- News sentiment analysis with NLP
- Cross-source analytics
- Results saved to MongoDB/Cassandra for visualization
"""

import os
import sys
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Fix Windows encoding
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

load_dotenv()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml.stat import Correlation
import pandas as pd
import numpy as np

# Data directories
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data" / "batch"
OUTPUT_DIR = BASE_DIR / "data" / "batch" / "spark_output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Database configurations
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "financial_analytics")

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "financial_data")


def create_spark_session():
    """Create Spark session with all necessary configurations"""
    print("\n" + "="*60)
    print("INITIALIZING SPARK SESSION")
    print("="*60)

    # Check for Java
    java_home = os.getenv("JAVA_HOME")
    if java_home:
        print(f"JAVA_HOME: {java_home}")

    spark = SparkSession.builder \
        .appName("BatchFinancialAnalytics") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print(f"Spark version: {spark.version}")
    print(f"Spark UI: http://localhost:4040")

    return spark


class StockAnalytics:
    """Stock market analytics using Spark"""

    def __init__(self, spark):
        self.spark = spark
        self.df = None

    def load_data(self):
        """Load stock data"""
        print("\n[StockAnalytics] Loading stock data...")

        stock_file = str(DATA_DIR / "stocks" / "stock_prices.csv")
        self.df = self.spark.read.csv(stock_file, header=True, inferSchema=True)

        # Convert date column
        self.df = self.df.withColumn("date", F.to_date(F.col("date")))

        print(f"  Loaded {self.df.count()} records")
        print(f"  Symbols: {self.df.select('symbol').distinct().count()}")

        return self.df

    def calculate_technical_indicators(self):
        """Calculate technical indicators for all stocks"""
        print("\n[StockAnalytics] Calculating technical indicators...")

        # Window specifications
        window_20 = Window.partitionBy("symbol").orderBy("date").rowsBetween(-19, 0)
        window_50 = Window.partitionBy("symbol").orderBy("date").rowsBetween(-49, 0)
        window_14 = Window.partitionBy("symbol").orderBy("date").rowsBetween(-13, 0)

        # Calculate indicators
        df = self.df \
            .withColumn("daily_return",
                (F.col("close") - F.lag("close", 1).over(Window.partitionBy("symbol").orderBy("date"))) /
                F.lag("close", 1).over(Window.partitionBy("symbol").orderBy("date"))) \
            .withColumn("sma_20", F.avg("close").over(window_20)) \
            .withColumn("sma_50", F.avg("close").over(window_50)) \
            .withColumn("volatility_20d", F.stddev("close").over(window_20)) \
            .withColumn("volume_sma_20", F.avg("volume").over(window_20)) \
            .withColumn("volume_ratio", F.col("volume") / F.col("volume_sma_20"))

        # Calculate RSI (simplified)
        df = df \
            .withColumn("price_change", F.col("close") - F.lag("close", 1).over(Window.partitionBy("symbol").orderBy("date"))) \
            .withColumn("gain", F.when(F.col("price_change") > 0, F.col("price_change")).otherwise(0)) \
            .withColumn("loss", F.when(F.col("price_change") < 0, F.abs(F.col("price_change"))).otherwise(0)) \
            .withColumn("avg_gain", F.avg("gain").over(window_14)) \
            .withColumn("avg_loss", F.avg("loss").over(window_14)) \
            .withColumn("rs", F.col("avg_gain") / F.col("avg_loss")) \
            .withColumn("rsi_14", 100 - (100 / (1 + F.col("rs"))))

        # Bollinger Bands
        df = df \
            .withColumn("bb_upper", F.col("sma_20") + 2 * F.col("volatility_20d")) \
            .withColumn("bb_lower", F.col("sma_20") - 2 * F.col("volatility_20d"))

        # Clean up intermediate columns
        df = df.drop("price_change", "gain", "loss", "avg_gain", "avg_loss", "rs")

        self.df = df.na.fill(0)

        print("  Technical indicators calculated:")
        print("    - Daily returns")
        print("    - SMA (20, 50)")
        print("    - Volatility (20-day)")
        print("    - RSI (14-day)")
        print("    - Bollinger Bands")
        print("    - Volume ratio")

        return self.df

    def calculate_correlations(self):
        """Calculate correlation matrix between stocks"""
        print("\n[StockAnalytics] Calculating stock correlations...")

        # Pivot to get daily returns by symbol
        pivot_df = self.df \
            .filter(F.col("daily_return").isNotNull()) \
            .groupBy("date") \
            .pivot("symbol") \
            .agg(F.first("daily_return"))

        # Get symbols
        symbols = [c for c in pivot_df.columns if c != "date"]

        # Convert to pandas for correlation calculation
        pdf = pivot_df.toPandas()
        pdf = pdf.drop("date", axis=1).dropna()

        if len(pdf) > 0:
            corr_matrix = pdf.corr()

            # Save correlation matrix
            corr_file = OUTPUT_DIR / "stock_correlations.csv"
            corr_matrix.to_csv(corr_file)
            print(f"  Saved correlation matrix to {corr_file}")

            # Find strongest correlations
            correlations = []
            for i, s1 in enumerate(symbols):
                for s2 in symbols[i+1:]:
                    if s1 in corr_matrix.columns and s2 in corr_matrix.columns:
                        corr = corr_matrix.loc[s1, s2]
                        if not pd.isna(corr):
                            correlations.append({
                                'symbol1': s1,
                                'symbol2': s2,
                                'correlation': round(corr, 4)
                            })

            # Sort by absolute correlation
            correlations.sort(key=lambda x: abs(x['correlation']), reverse=True)

            print(f"\n  Top 10 correlations:")
            for c in correlations[:10]:
                print(f"    {c['symbol1']} - {c['symbol2']}: {c['correlation']:.3f}")

            return correlations

        return []

    def calculate_summary_stats(self):
        """Calculate summary statistics per stock"""
        print("\n[StockAnalytics] Calculating summary statistics...")

        summary = self.df.groupBy("symbol") \
            .agg(
                F.count("*").alias("total_records"),
                F.min("date").alias("start_date"),
                F.max("date").alias("end_date"),
                F.min("close").alias("min_price"),
                F.max("close").alias("max_price"),
                F.avg("close").alias("avg_price"),
                F.stddev("close").alias("price_std"),
                F.avg("daily_return").alias("avg_daily_return"),
                F.stddev("daily_return").alias("return_volatility"),
                F.avg("volume").alias("avg_volume"),
                F.last("close").alias("latest_price")
            )

        # Calculate annualized metrics
        summary = summary \
            .withColumn("annualized_return", F.col("avg_daily_return") * 252 * 100) \
            .withColumn("annualized_volatility", F.col("return_volatility") * F.sqrt(F.lit(252)) * 100) \
            .withColumn("sharpe_ratio", F.col("annualized_return") / F.col("annualized_volatility"))

        # Save to CSV
        summary_pdf = summary.toPandas()
        summary_file = OUTPUT_DIR / "stock_summary_stats.csv"
        summary_pdf.to_csv(summary_file, index=False)
        print(f"  Saved summary statistics to {summary_file}")

        return summary


class EconomicAnalytics:
    """Economic indicator analytics using Spark"""

    def __init__(self, spark):
        self.spark = spark
        self.df = None

    def load_data(self):
        """Load economic data"""
        print("\n[EconomicAnalytics] Loading economic data...")

        # FRED data
        fred_file = str(DATA_DIR / "fred" / "economic_indicators.csv")
        self.df = self.spark.read.csv(fred_file, header=True, inferSchema=True)

        print(f"  Loaded {self.df.count()} FRED records")
        print(f"  Indicators: {self.df.select('series_id').distinct().count()}")

        return self.df

    def calculate_trends(self):
        """Calculate trends for economic indicators"""
        print("\n[EconomicAnalytics] Calculating economic trends...")

        window = Window.partitionBy("series_id").orderBy("year")

        # Calculate year-over-year changes
        df = self.df \
            .withColumn("prev_value", F.lag("value", 1).over(window)) \
            .withColumn("yoy_change",
                (F.col("value") - F.col("prev_value")) / F.col("prev_value") * 100) \
            .withColumn("yoy_change_abs", F.col("value") - F.col("prev_value"))

        # Calculate rolling averages
        window_5 = Window.partitionBy("series_id").orderBy("year").rowsBetween(-4, 0)
        df = df.withColumn("rolling_avg_5y", F.avg("value").over(window_5))

        self.df = df

        # Save processed data
        processed_pdf = df.toPandas()
        processed_file = OUTPUT_DIR / "economic_trends.csv"
        processed_pdf.to_csv(processed_file, index=False)
        print(f"  Saved economic trends to {processed_file}")

        return df


class WorldBankAnalytics:
    """World Bank data analytics using Spark"""

    def __init__(self, spark):
        self.spark = spark
        self.df = None

    def load_data(self):
        """Load World Bank data"""
        print("\n[WorldBankAnalytics] Loading World Bank data...")

        wb_file = str(DATA_DIR / "worldbank" / "world_bank_indicators.csv")
        self.df = self.spark.read.csv(wb_file, header=True, inferSchema=True)

        print(f"  Loaded {self.df.count()} World Bank records")
        print(f"  Countries: {self.df.select('country_code').distinct().count()}")
        print(f"  Indicators: {self.df.select('indicator_code').distinct().count()}")

        return self.df

    def analyze_gdp_trends(self):
        """Analyze GDP trends across countries"""
        print("\n[WorldBankAnalytics] Analyzing GDP trends...")

        # Filter to GDP indicator
        gdp_df = self.df.filter(F.col("indicator_code") == "NY.GDP.MKTP.CD")

        # Calculate year-over-year growth
        window = Window.partitionBy("country_code").orderBy("year")

        gdp_df = gdp_df \
            .withColumn("prev_gdp", F.lag("value", 1).over(window)) \
            .withColumn("gdp_growth",
                (F.col("value") - F.col("prev_gdp")) / F.col("prev_gdp") * 100)

        # Calculate country rankings by GDP
        latest_year = gdp_df.agg(F.max("year")).collect()[0][0]

        rankings = gdp_df \
            .filter(F.col("year") == latest_year) \
            .orderBy(F.col("value").desc()) \
            .select("country_code", "country_name", "value", "gdp_growth") \
            .withColumn("rank", F.row_number().over(Window.orderBy(F.col("value").desc())))

        # Save rankings
        rankings_pdf = rankings.limit(50).toPandas()
        rankings_file = OUTPUT_DIR / "gdp_rankings.csv"
        rankings_pdf.to_csv(rankings_file, index=False)
        print(f"  Saved GDP rankings to {rankings_file}")

        # Save full GDP trends
        gdp_pdf = gdp_df.toPandas()
        gdp_file = OUTPUT_DIR / "gdp_trends.csv"
        gdp_pdf.to_csv(gdp_file, index=False)
        print(f"  Saved GDP trends to {gdp_file}")

        return gdp_df


class SentimentAnalytics:
    """News sentiment analytics using Spark"""

    def __init__(self, spark):
        self.spark = spark
        self.df = None

    def load_data(self):
        """Load news headlines data"""
        print("\n[SentimentAnalytics] Loading news data...")

        news_file = str(DATA_DIR / "news" / "financial_news_headlines.csv")
        self.df = self.spark.read.csv(news_file, header=True, inferSchema=True)

        # Convert date
        self.df = self.df.withColumn("date", F.to_date(F.col("date")))

        print(f"  Loaded {self.df.count()} news headlines")

        return self.df

    def analyze_sentiment_trends(self):
        """Analyze sentiment trends over time"""
        print("\n[SentimentAnalytics] Analyzing sentiment trends...")

        # Daily sentiment aggregation
        daily_sentiment = self.df.groupBy("date") \
            .agg(
                F.avg("sentiment_score").alias("avg_sentiment"),
                F.stddev("sentiment_score").alias("sentiment_std"),
                F.count("*").alias("headline_count"),
                F.sum(F.when(F.col("sentiment") == "positive", 1).otherwise(0)).alias("positive_count"),
                F.sum(F.when(F.col("sentiment") == "negative", 1).otherwise(0)).alias("negative_count"),
                F.sum(F.when(F.col("sentiment") == "neutral", 1).otherwise(0)).alias("neutral_count")
            ) \
            .withColumn("positive_ratio", F.col("positive_count") / F.col("headline_count")) \
            .withColumn("negative_ratio", F.col("negative_count") / F.col("headline_count")) \
            .orderBy("date")

        # Save daily sentiment
        daily_pdf = daily_sentiment.toPandas()
        daily_file = OUTPUT_DIR / "daily_sentiment_analysis.csv"
        daily_pdf.to_csv(daily_file, index=False)
        print(f"  Saved daily sentiment analysis to {daily_file}")

        return daily_sentiment

    def analyze_company_sentiment(self):
        """Analyze sentiment by company"""
        print("\n[SentimentAnalytics] Analyzing company sentiment...")

        company_sentiment = self.df \
            .filter(F.col("company_mentioned").isNotNull()) \
            .groupBy("company_mentioned") \
            .agg(
                F.avg("sentiment_score").alias("avg_sentiment"),
                F.count("*").alias("mention_count"),
                F.sum(F.when(F.col("sentiment") == "positive", 1).otherwise(0)).alias("positive_mentions"),
                F.sum(F.when(F.col("sentiment") == "negative", 1).otherwise(0)).alias("negative_mentions")
            ) \
            .withColumn("sentiment_ratio",
                F.col("positive_mentions") / (F.col("positive_mentions") + F.col("negative_mentions"))) \
            .orderBy(F.col("mention_count").desc())

        # Save company sentiment
        company_pdf = company_sentiment.toPandas()
        company_file = OUTPUT_DIR / "company_sentiment_analysis.csv"
        company_pdf.to_csv(company_file, index=False)
        print(f"  Saved company sentiment analysis to {company_file}")

        return company_sentiment

    def word_frequency_analysis(self):
        """Analyze word frequencies in headlines"""
        print("\n[SentimentAnalytics] Analyzing word frequencies...")

        # Tokenize headlines
        tokenizer = Tokenizer(inputCol="headline", outputCol="words")
        words_df = tokenizer.transform(self.df)

        # Remove stop words
        remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
        filtered_df = remover.transform(words_df)

        # Explode words and count
        word_counts = filtered_df \
            .select(F.explode("filtered_words").alias("word")) \
            .filter(F.length("word") > 2) \
            .groupBy("word") \
            .count() \
            .orderBy(F.col("count").desc())

        # Save word frequencies
        word_pdf = word_counts.limit(500).toPandas()
        word_file = OUTPUT_DIR / "word_frequencies.csv"
        word_pdf.to_csv(word_file, index=False)
        print(f"  Saved word frequencies to {word_file}")

        # Word frequencies by sentiment
        for sentiment in ["positive", "negative", "neutral"]:
            sentiment_df = self.df.filter(F.col("sentiment") == sentiment)
            tokenized = tokenizer.transform(sentiment_df)
            filtered = remover.transform(tokenized)

            counts = filtered \
                .select(F.explode("filtered_words").alias("word")) \
                .filter(F.length("word") > 2) \
                .groupBy("word") \
                .count() \
                .orderBy(F.col("count").desc())

            counts_pdf = counts.limit(100).toPandas()
            counts_file = OUTPUT_DIR / f"word_frequencies_{sentiment}.csv"
            counts_pdf.to_csv(counts_file, index=False)

        print("  Saved word frequencies by sentiment")

        return word_counts


class CrossSourceAnalytics:
    """Cross-source analytics combining all data"""

    def __init__(self, spark, stock_df, sentiment_df, economic_df):
        self.spark = spark
        self.stock_df = stock_df
        self.sentiment_df = sentiment_df
        self.economic_df = economic_df

    def stock_sentiment_correlation(self):
        """Analyze correlation between stock returns and sentiment"""
        print("\n[CrossSourceAnalytics] Analyzing stock-sentiment correlation...")

        # Get SPY daily returns
        spy_returns = self.stock_df \
            .filter(F.col("symbol") == "SPY") \
            .select("date", "daily_return") \
            .withColumnRenamed("daily_return", "spy_return")

        # Get daily sentiment
        daily_sentiment = self.sentiment_df.groupBy("date") \
            .agg(F.avg("sentiment_score").alias("avg_sentiment"))

        # Join and correlate
        combined = spy_returns.join(daily_sentiment, "date", "inner")

        # Convert to pandas for correlation
        combined_pdf = combined.toPandas()
        combined_pdf = combined_pdf.dropna()

        if len(combined_pdf) > 10:
            correlation = combined_pdf["spy_return"].corr(combined_pdf["avg_sentiment"])
            print(f"  SPY Returns vs Sentiment Correlation: {correlation:.4f}")

            # Save combined data
            combined_file = OUTPUT_DIR / "stock_sentiment_combined.csv"
            combined_pdf.to_csv(combined_file, index=False)
            print(f"  Saved stock-sentiment data to {combined_file}")

            return correlation

        return None

    def generate_comprehensive_report(self):
        """Generate a comprehensive analytics report"""
        print("\n[CrossSourceAnalytics] Generating comprehensive report...")

        report = {
            "generated_at": datetime.now().isoformat(),
            "data_sources": {
                "stocks": self.stock_df.count(),
                "news_headlines": self.sentiment_df.count(),
                "economic_indicators": self.economic_df.count() if self.economic_df else 0
            },
            "stock_summary": {},
            "sentiment_summary": {},
            "top_correlations": []
        }

        # Stock summary
        stock_summary = self.stock_df.groupBy("symbol").agg(
            F.last("close").alias("latest_price"),
            F.avg("daily_return").alias("avg_return")
        ).collect()

        for row in stock_summary:
            report["stock_summary"][row["symbol"]] = {
                "latest_price": round(row["latest_price"], 2) if row["latest_price"] else None,
                "avg_return": round(row["avg_return"] * 100, 4) if row["avg_return"] else None
            }

        # Sentiment summary
        sentiment_summary = self.sentiment_df.agg(
            F.avg("sentiment_score").alias("avg_sentiment"),
            F.count("*").alias("total_headlines")
        ).collect()[0]

        report["sentiment_summary"] = {
            "avg_sentiment": round(sentiment_summary["avg_sentiment"], 4),
            "total_headlines": sentiment_summary["total_headlines"]
        }

        # Save report as JSON
        import json
        report_file = OUTPUT_DIR / "analytics_report.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        print(f"  Saved comprehensive report to {report_file}")

        return report


def save_to_mongodb(output_dir):
    """Save analytics results to MongoDB"""
    print("\n" + "-"*40)
    print("Saving results to MongoDB...")
    print("-"*40)

    try:
        from pymongo import MongoClient
        client = MongoClient(MONGODB_URI)
        db = client[MONGODB_DATABASE]

        # Save each output file as a collection
        for csv_file in output_dir.glob("*.csv"):
            collection_name = csv_file.stem
            df = pd.read_csv(csv_file)

            # Clear and insert
            db[collection_name].delete_many({})
            if len(df) > 0:
                records = df.to_dict('records')
                db[collection_name].insert_many(records)
                print(f"  Saved {len(records)} records to {collection_name}")

        client.close()
        print("  MongoDB save complete")

    except Exception as e:
        print(f"  MongoDB save failed: {e}")


def main():
    """Run all batch analytics"""
    print("="*60)
    print("BATCH SPARK ANALYTICS")
    print("="*60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Data directory: {DATA_DIR}")
    print(f"Output directory: {OUTPUT_DIR}")

    # Check if data exists
    if not DATA_DIR.exists() or not any(DATA_DIR.rglob("*.csv")):
        print("\nERROR: No data files found!")
        print("Run batch_data_downloader.py first.")
        return

    # Create Spark session
    spark = create_spark_session()

    try:
        # Stock Analytics
        print("\n" + "="*60)
        print("STOCK MARKET ANALYTICS")
        print("="*60)

        stock_analytics = StockAnalytics(spark)
        stock_df = stock_analytics.load_data()
        stock_df = stock_analytics.calculate_technical_indicators()
        stock_analytics.calculate_correlations()
        stock_analytics.calculate_summary_stats()

        # Economic Analytics
        print("\n" + "="*60)
        print("ECONOMIC INDICATOR ANALYTICS")
        print("="*60)

        economic_analytics = EconomicAnalytics(spark)
        economic_df = economic_analytics.load_data()
        economic_analytics.calculate_trends()

        # World Bank Analytics
        print("\n" + "="*60)
        print("WORLD BANK DATA ANALYTICS")
        print("="*60)

        wb_analytics = WorldBankAnalytics(spark)
        wb_df = wb_analytics.load_data()
        wb_analytics.analyze_gdp_trends()

        # Sentiment Analytics
        print("\n" + "="*60)
        print("NEWS SENTIMENT ANALYTICS")
        print("="*60)

        sentiment_analytics = SentimentAnalytics(spark)
        sentiment_df = sentiment_analytics.load_data()
        sentiment_analytics.analyze_sentiment_trends()
        sentiment_analytics.analyze_company_sentiment()
        sentiment_analytics.word_frequency_analysis()

        # Cross-Source Analytics
        print("\n" + "="*60)
        print("CROSS-SOURCE ANALYTICS")
        print("="*60)

        cross_analytics = CrossSourceAnalytics(spark, stock_df, sentiment_df, economic_df)
        cross_analytics.stock_sentiment_correlation()
        cross_analytics.generate_comprehensive_report()

        # Save to MongoDB
        save_to_mongodb(OUTPUT_DIR)

        print("\n" + "="*60)
        print("ANALYTICS COMPLETE!")
        print("="*60)
        print(f"\nOutput files saved to: {OUTPUT_DIR}")
        print("\nFiles generated:")
        for f in OUTPUT_DIR.glob("*"):
            size = f.stat().st_size / 1024
            print(f"  {f.name}: {size:.1f} KB")

    finally:
        spark.stop()
        print(f"\nFinished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
