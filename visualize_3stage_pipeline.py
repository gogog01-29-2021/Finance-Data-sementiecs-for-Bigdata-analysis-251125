#!/usr/bin/env python3
"""
3-STAGE PIPELINE VISUALIZATION
Shows data at all three stages: Raw -> Processing -> Database
"""

import os
import json
import lmdb
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np
from pymongo import MongoClient
from dotenv import load_dotenv

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import seaborn as sns
from matplotlib.gridspec import GridSpec

load_dotenv()

# Configuration
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "financial_analytics")
OUTPUT_DIR = Path("visualizations/3stage")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Data paths
LMDB_REDDIT = Path("data/reddit/reddit_sentiment_lmdb.db")
LMDB_TWITTER = Path("data/twitter/twitter_sentiment.db")
LMDB_ONCHAIN = Path("data/onchain/onchain_data.db")

sns.set_style("darkgrid")
plt.rcParams['figure.figsize'] = (18, 12)
plt.rcParams['font.size'] = 9


class ThreeStageVisualizer:
    """Visualize pipeline at all three stages"""

    def __init__(self):
        self.mongo_client = None
        self.raw_data = {}
        self.processed_data = {}
        self.db_data = {}

    # ========================================================================
    # STAGE 1: RAW DATA VISUALIZATION
    # ========================================================================

    def load_raw_data(self):
        """Load raw data from LMDB databases"""
        print("\n[STAGE 1] Loading RAW data...")

        # Reddit data
        if LMDB_REDDIT.exists():
            reddit_records = self._read_lmdb(LMDB_REDDIT, "sentiment:")
            self.raw_data['reddit'] = reddit_records
            print(f"  Reddit: {len(reddit_records)} posts/comments")
        else:
            self.raw_data['reddit'] = []
            print(f"  Reddit: No data")

        # Twitter data
        if LMDB_TWITTER.exists():
            twitter_records = self._read_lmdb(LMDB_TWITTER, "tweet:")
            self.raw_data['twitter'] = twitter_records
            print(f"  Twitter: {len(twitter_records)} tweets")
        else:
            self.raw_data['twitter'] = []
            print(f"  Twitter: No data")

        # On-chain data
        if LMDB_ONCHAIN.exists():
            onchain_records = self._read_lmdb(LMDB_ONCHAIN, "tx:")
            self.raw_data['onchain'] = onchain_records
            print(f"  On-chain: {len(onchain_records)} transactions")
        else:
            self.raw_data['onchain'] = []
            print(f"  On-chain: No data")

    def _read_lmdb(self, db_path: Path, prefix: str) -> list:
        """Read records from LMDB"""
        env = lmdb.open(str(db_path), readonly=True)
        records = []

        with env.begin() as txn:
            cursor = txn.cursor()
            for key, value in cursor:
                if key.startswith(prefix.encode()):
                    record = json.loads(value.decode())
                    records.append(record)

        env.close()
        return records

    def visualize_stage1_raw_data(self):
        """
        STAGE 1 VISUALIZATION: Raw Collected Data
        Shows what was collected before any processing
        """
        print("\n[VIZ-STAGE1] Creating raw data visualization...")

        fig = plt.figure(figsize=(18, 12))
        fig.suptitle('STAGE 1: RAW DATA COLLECTION (Before Processing)',
                     fontsize=18, fontweight='bold', y=0.995)

        gs = GridSpec(3, 3, figure=fig, hspace=0.4, wspace=0.3)

        # ===== REDDIT RAW DATA =====
        reddit_data = self.raw_data.get('reddit', [])

        # Plot 1: Reddit post count over time
        ax1 = fig.add_subplot(gs[0, 0])
        if reddit_data:
            df = pd.DataFrame(reddit_data)
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
            df['hour'] = df['timestamp'].dt.floor('H')
            hourly_counts = df.groupby('hour').size()

            hourly_counts.plot(kind='bar', ax=ax1, color='#FF4500')
            ax1.set_title('Reddit: Posts Over Time (Raw)', fontweight='bold')
            ax1.set_xlabel('Hour')
            ax1.set_ylabel('Post Count')
            ax1.tick_params(axis='x', rotation=45)
        else:
            ax1.text(0.5, 0.5, 'No Reddit Data', ha='center', va='center', fontsize=14)
            ax1.set_title('Reddit: Posts Over Time (Raw)', fontweight='bold')

        # Plot 2: Reddit subreddit distribution
        ax2 = fig.add_subplot(gs[0, 1])
        if reddit_data:
            df = pd.DataFrame(reddit_data)
            if 'subreddit' in df.columns:
                subreddit_counts = df['subreddit'].value_counts().head(8)
                subreddit_counts.plot(kind='barh', ax=ax2, color='orangered')
                ax2.set_title('Reddit: Top Subreddits (Raw)', fontweight='bold')
                ax2.set_xlabel('Post Count')
        else:
            ax2.text(0.5, 0.5, 'No Data', ha='center', va='center')
            ax2.set_title('Reddit: Top Subreddits (Raw)', fontweight='bold')

        # Plot 3: Reddit raw text sample
        ax3 = fig.add_subplot(gs[0, 2])
        ax3.axis('off')
        if reddit_data:
            sample_texts = reddit_data[:5]
            text_display = "Sample Raw Reddit Posts:\n\n"
            for i, record in enumerate(sample_texts, 1):
                text = record.get('text', '')[:80]
                text_display += f"{i}. {text}...\n\n"

            ax3.text(0.05, 0.95, text_display, transform=ax3.transAxes,
                    fontsize=8, verticalalignment='top', family='monospace',
                    bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.3))
            ax3.set_title('Reddit: Raw Text Samples', fontweight='bold')
        else:
            ax3.text(0.5, 0.5, 'No Data', ha='center', va='center')

        # ===== TWITTER RAW DATA =====
        twitter_data = self.raw_data.get('twitter', [])

        # Plot 4: Twitter tweet count
        ax4 = fig.add_subplot(gs[1, 0])
        if twitter_data:
            df = pd.DataFrame(twitter_data)
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
            df['hour'] = df['timestamp'].dt.floor('H')
            hourly_counts = df.groupby('hour').size()

            hourly_counts.plot(kind='bar', ax=ax4, color='#1DA1F2')
            ax4.set_title('Twitter: Tweets Over Time (Raw)', fontweight='bold')
            ax4.set_xlabel('Hour')
            ax4.set_ylabel('Tweet Count')
            ax4.tick_params(axis='x', rotation=45)
        else:
            ax4.text(0.5, 0.5, 'No Twitter Data', ha='center', va='center')
            ax4.set_title('Twitter: Tweets Over Time (Raw)', fontweight='bold')

        # Plot 5: Twitter author distribution
        ax5 = fig.add_subplot(gs[1, 1])
        if twitter_data:
            df = pd.DataFrame(twitter_data)
            if 'author' in df.columns:
                author_counts = df['author'].value_counts().head(8)
                author_counts.plot(kind='barh', ax=ax5, color='skyblue')
                ax5.set_title('Twitter: Top Authors (Raw)', fontweight='bold')
                ax5.set_xlabel('Tweet Count')
        else:
            ax5.text(0.5, 0.5, 'No Data', ha='center', va='center')
            ax5.set_title('Twitter: Top Authors (Raw)', fontweight='bold')

        # Plot 6: Twitter raw text sample
        ax6 = fig.add_subplot(gs[1, 2])
        ax6.axis('off')
        if twitter_data:
            sample_tweets = twitter_data[:5]
            text_display = "Sample Raw Tweets:\n\n"
            for i, record in enumerate(sample_tweets, 1):
                text = record.get('text', '')[:80]
                text_display += f"{i}. {text}...\n\n"

            ax6.text(0.05, 0.95, text_display, transform=ax6.transAxes,
                    fontsize=8, verticalalignment='top', family='monospace',
                    bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.3))
            ax6.set_title('Twitter: Raw Text Samples', fontweight='bold')
        else:
            ax6.text(0.5, 0.5, 'No Data', ha='center', va='center')

        # ===== ON-CHAIN RAW DATA =====
        onchain_data = self.raw_data.get('onchain', [])

        # Plot 7: On-chain transaction timeline
        ax7 = fig.add_subplot(gs[2, 0])
        if onchain_data:
            df = pd.DataFrame(onchain_data)
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
            df['hour'] = df['timestamp'].dt.floor('H')
            hourly_counts = df.groupby('hour').size()

            hourly_counts.plot(kind='bar', ax=ax7, color='gold')
            ax7.set_title('On-Chain: Transactions Over Time (Raw)', fontweight='bold')
            ax7.set_xlabel('Hour')
            ax7.set_ylabel('TX Count')
            ax7.tick_params(axis='x', rotation=45)
        else:
            ax7.text(0.5, 0.5, 'No On-Chain Data', ha='center', va='center')
            ax7.set_title('On-Chain: Transactions Over Time (Raw)', fontweight='bold')

        # Plot 8: On-chain asset distribution
        ax8 = fig.add_subplot(gs[2, 1])
        if onchain_data:
            df = pd.DataFrame(onchain_data)
            if 'asset' in df.columns:
                asset_counts = df['asset'].value_counts()
                asset_counts.plot(kind='bar', ax=ax8, color='goldenrod')
                ax8.set_title('On-Chain: Asset Distribution (Raw)', fontweight='bold')
                ax8.set_xlabel('Asset')
                ax8.set_ylabel('TX Count')
        else:
            ax8.text(0.5, 0.5, 'No Data', ha='center', va='center')
            ax8.set_title('On-Chain: Asset Distribution (Raw)', fontweight='bold')

        # Plot 9: On-chain transaction summary
        ax9 = fig.add_subplot(gs[2, 2])
        ax9.axis('off')
        if onchain_data:
            df = pd.DataFrame(onchain_data)

            summary = f"""
ON-CHAIN RAW DATA SUMMARY:

Total Transactions: {len(df)}
Assets: {df['asset'].nunique() if 'asset' in df.columns else 0}

Sample Transactions:
"""
            for i, record in enumerate(onchain_data[:3], 1):
                amount = record.get('amount', 0)
                asset = record.get('asset', 'N/A')
                summary += f"\n{i}. {amount:.2f} {asset}"

            ax9.text(0.05, 0.95, summary, transform=ax9.transAxes,
                    fontsize=9, verticalalignment='top', family='monospace',
                    bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.3))
            ax9.set_title('On-Chain: Summary (Raw)', fontweight='bold')
        else:
            ax9.text(0.5, 0.5, 'No Data', ha='center', va='center')

        output_path = OUTPUT_DIR / "stage1_raw_data.png"
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"  [OK] Saved: {output_path}")
        plt.close()

    # ========================================================================
    # STAGE 2: PROCESSED DATA VISUALIZATION (BEFORE DB)
    # ========================================================================

    def process_data_for_visualization(self):
        """
        Simulate the processing stage to show intermediate results
        This shows what happens AFTER processing but BEFORE storing in DB
        """
        print("\n[STAGE 2] Processing data (intermediate results)...")

        processed = {
            'entities_extracted': [],
            'topics_detected': [],
            'sentiment_scores': [],
            'divergences_found': []
        }

        # Process Reddit data
        for record in self.raw_data.get('reddit', []):
            sentiment = record.get('sentiment', {})

            # Extract entities
            entities = sentiment.get('entities', {})
            for symbol, count in entities.items():
                processed['entities_extracted'].append({
                    'source': 'reddit',
                    'symbol': symbol,
                    'count': count,
                    'sentiment': sentiment.get('polarity', 0)
                })

            # Extract topics
            topics = sentiment.get('topics', [])
            for topic in topics:
                processed['topics_detected'].append({
                    'source': 'reddit',
                    'topic': topic,
                    'sentiment': sentiment.get('polarity', 0)
                })

            # Sentiment scores
            if 'polarity' in sentiment:
                processed['sentiment_scores'].append({
                    'source': 'reddit',
                    'polarity': sentiment.get('polarity', 0),
                    'urgency': sentiment.get('urgency_score', 0)
                })

        # Process Twitter data
        for record in self.raw_data.get('twitter', []):
            sentiment = record.get('sentiment', {})

            entities = sentiment.get('entities', {})
            for symbol, count in entities.items():
                processed['entities_extracted'].append({
                    'source': 'twitter',
                    'symbol': symbol,
                    'count': count,
                    'sentiment': sentiment.get('polarity', 0)
                })

            topics = sentiment.get('topics', [])
            for topic in topics:
                processed['topics_detected'].append({
                    'source': 'twitter',
                    'topic': topic,
                    'sentiment': sentiment.get('polarity', 0)
                })

            if 'polarity' in sentiment:
                processed['sentiment_scores'].append({
                    'source': 'twitter',
                    'polarity': sentiment.get('polarity', 0),
                    'urgency': sentiment.get('urgency_score', 0),
                    'influence': sentiment.get('influence_score', 0)
                })

        # Process On-chain data (simulated divergences)
        onchain_signals = {}
        for record in self.raw_data.get('onchain', []):
            analysis = record.get('analysis', {})
            asset = record.get('asset', 'BTC')
            signal = analysis.get('signal', 'transfer')

            if asset not in onchain_signals:
                onchain_signals[asset] = {'accumulation': 0, 'distribution': 0}

            if signal == 'accumulation':
                onchain_signals[asset]['accumulation'] += 1
            elif signal == 'distribution':
                onchain_signals[asset]['distribution'] += 1

        # Detect divergences (social sentiment vs onchain behavior)
        sentiment_by_asset = {}
        for item in processed['entities_extracted']:
            symbol = item['symbol']
            if symbol not in sentiment_by_asset:
                sentiment_by_asset[symbol] = []
            sentiment_by_asset[symbol].append(item['sentiment'])

        for symbol, sentiments in sentiment_by_asset.items():
            avg_sentiment = np.mean(sentiments)
            onchain = onchain_signals.get(symbol, {'accumulation': 0, 'distribution': 0})

            # Bearish divergence: positive sentiment but distribution
            if avg_sentiment > 0.2 and onchain['distribution'] > onchain['accumulation']:
                processed['divergences_found'].append({
                    'symbol': symbol,
                    'type': 'BEARISH_DIVERGENCE',
                    'sentiment': avg_sentiment,
                    'distribution_count': onchain['distribution']
                })

            # Bullish divergence: negative sentiment but accumulation
            elif avg_sentiment < -0.2 and onchain['accumulation'] > onchain['distribution']:
                processed['divergences_found'].append({
                    'symbol': symbol,
                    'type': 'BULLISH_DIVERGENCE',
                    'sentiment': avg_sentiment,
                    'accumulation_count': onchain['accumulation']
                })

        self.processed_data = processed

        print(f"  Entities extracted: {len(processed['entities_extracted'])}")
        print(f"  Topics detected: {len(processed['topics_detected'])}")
        print(f"  Sentiment scores: {len(processed['sentiment_scores'])}")
        print(f"  Divergences found: {len(processed['divergences_found'])}")

    def visualize_stage2_processed_data(self):
        """
        STAGE 2 VISUALIZATION: Processed Data (Before DB)
        Shows intermediate analytics results
        """
        print("\n[VIZ-STAGE2] Creating processed data visualization...")

        fig = plt.figure(figsize=(18, 12))
        fig.suptitle('STAGE 2: PROCESSED DATA (After Processing, Before Database)',
                     fontsize=18, fontweight='bold', y=0.995)

        gs = GridSpec(3, 3, figure=fig, hspace=0.4, wspace=0.3)

        # Plot 1: Entities Extracted
        ax1 = fig.add_subplot(gs[0, 0])
        entities_df = pd.DataFrame(self.processed_data['entities_extracted'])
        if not entities_df.empty:
            entity_counts = entities_df.groupby('symbol')['count'].sum().sort_values(ascending=False).head(10)
            entity_counts.plot(kind='bar', ax=ax1, color='steelblue')
            ax1.set_title('Entities Extracted (Processing)', fontweight='bold')
            ax1.set_xlabel('Crypto Symbol')
            ax1.set_ylabel('Mention Count')
            ax1.tick_params(axis='x', rotation=45)
        else:
            ax1.text(0.5, 0.5, 'No entities', ha='center', va='center')

        # Plot 2: Topics Detected
        ax2 = fig.add_subplot(gs[0, 1])
        topics_df = pd.DataFrame(self.processed_data['topics_detected'])
        if not topics_df.empty:
            topic_counts = topics_df['topic'].value_counts()
            colors_map = {
                'bullish': 'green', 'bearish': 'red', 'technical_analysis': 'blue',
                'regulation': 'orange', 'long_term': 'purple', 'fomo': 'magenta'
            }
            colors = [colors_map.get(t, 'gray') for t in topic_counts.index]
            topic_counts.plot(kind='bar', ax=ax2, color=colors)
            ax2.set_title('Topics Detected (Processing)', fontweight='bold')
            ax2.set_xlabel('Topic')
            ax2.set_ylabel('Count')
            ax2.tick_params(axis='x', rotation=45)
        else:
            ax2.text(0.5, 0.5, 'No topics', ha='center', va='center')

        # Plot 3: Sentiment Distribution
        ax3 = fig.add_subplot(gs[0, 2])
        sentiment_df = pd.DataFrame(self.processed_data['sentiment_scores'])
        if not sentiment_df.empty:
            ax3.hist(sentiment_df['polarity'], bins=20, color='teal', alpha=0.7, edgecolor='black')
            ax3.axvline(x=0, color='red', linestyle='--', linewidth=2)
            ax3.set_title('Sentiment Distribution (Processing)', fontweight='bold')
            ax3.set_xlabel('Sentiment Polarity')
            ax3.set_ylabel('Frequency')
        else:
            ax3.text(0.5, 0.5, 'No sentiment', ha='center', va='center')

        # Plot 4: Entity-Specific Sentiment
        ax4 = fig.add_subplot(gs[1, 0])
        if not entities_df.empty:
            entity_sentiment = entities_df.groupby('symbol')['sentiment'].mean().sort_values(ascending=False).head(10)
            colors = ['green' if x > 0 else 'red' for x in entity_sentiment.values]
            entity_sentiment.plot(kind='barh', ax=ax4, color=colors)
            ax4.set_title('Sentiment by Entity (Processing)', fontweight='bold')
            ax4.set_xlabel('Avg Sentiment')
            ax4.axvline(x=0, color='black', linestyle='--')
        else:
            ax4.text(0.5, 0.5, 'No data', ha='center', va='center')

        # Plot 5: Source Comparison
        ax5 = fig.add_subplot(gs[1, 1])
        if not entities_df.empty:
            source_comparison = entities_df.groupby(['symbol', 'source'])['sentiment'].mean().unstack(fill_value=0)
            if not source_comparison.empty:
                source_comparison.head(8).plot(kind='bar', ax=ax5)
                ax5.set_title('Reddit vs Twitter Sentiment (Processing)', fontweight='bold')
                ax5.set_xlabel('Symbol')
                ax5.set_ylabel('Avg Sentiment')
                ax5.tick_params(axis='x', rotation=45)
                ax5.legend(title='Source')
                ax5.axhline(y=0, color='black', linestyle='--')
        else:
            ax5.text(0.5, 0.5, 'No data', ha='center', va='center')

        # Plot 6: Topic-Sentiment Correlation
        ax6 = fig.add_subplot(gs[1, 2])
        if not topics_df.empty:
            topic_sentiment = topics_df.groupby('topic')['sentiment'].mean().sort_values()
            colors = ['green' if x > 0 else 'red' for x in topic_sentiment.values]
            topic_sentiment.plot(kind='barh', ax=ax6, color=colors)
            ax6.set_title('Sentiment by Topic (Processing)', fontweight='bold')
            ax6.set_xlabel('Avg Sentiment')
            ax6.axvline(x=0, color='black', linestyle='--')
        else:
            ax6.text(0.5, 0.5, 'No data', ha='center', va='center')

        # Plot 7: Divergences Detected (KEY INSIGHT!)
        ax7 = fig.add_subplot(gs[2, :])
        divergences_df = pd.DataFrame(self.processed_data['divergences_found'])
        if not divergences_df.empty:
            div_counts = divergences_df.groupby(['symbol', 'type']).size().unstack(fill_value=0)
            div_counts.plot(kind='bar', stacked=True, ax=ax7, color=['green', 'red'])
            ax7.set_title('[KEY INSIGHT] Divergences Detected: Sentiment vs On-Chain (Processing)',
                         fontweight='bold', fontsize=14)
            ax7.set_xlabel('Symbol')
            ax7.set_ylabel('Divergence Count')
            ax7.tick_params(axis='x', rotation=45)
            ax7.legend(title='Type')

            # Add annotation
            ax7.text(0.5, 0.95,
                    'Divergence = Social sentiment does NOT match on-chain behavior',
                    transform=ax7.transAxes, ha='center', va='top',
                    bbox=dict(boxstyle='round', facecolor='yellow', alpha=0.5),
                    fontsize=10, fontweight='bold')
        else:
            ax7.text(0.5, 0.5, 'No divergences detected', ha='center', va='center', fontsize=14)
            ax7.set_title('[KEY INSIGHT] Divergences Detected (Processing)', fontweight='bold')

        output_path = OUTPUT_DIR / "stage2_processed_data.png"
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"  [OK] Saved: {output_path}")
        plt.close()

    # ========================================================================
    # STAGE 3: DATABASE RESULTS VISUALIZATION (AFTER DB)
    # ========================================================================

    def load_database_results(self):
        """Load final results from MongoDB"""
        print("\n[STAGE 3] Loading DATABASE results...")

        try:
            self.mongo_client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=2000)
            self.mongo_client.server_info()

            db = self.mongo_client[MONGODB_DATABASE]
            collection = db['enhanced_semantic_analytics']

            # Fetch all analytics
            cursor = collection.find({})
            db_records = list(cursor)

            if db_records:
                df = pd.DataFrame(db_records)
                if '_id' in df.columns:
                    df = df.drop('_id', axis=1)

                # Group by analytics type
                for analytics_type in df['analytics_type'].unique():
                    type_df = df[df['analytics_type'] == analytics_type]
                    self.db_data[analytics_type] = type_df

                print(f"  Loaded {len(db_records)} records from database")
                print(f"  Analytics types: {list(self.db_data.keys())}")
            else:
                print(f"  No data in database")

        except Exception as e:
            print(f"  [FAIL] Database connection: {e}")

    def visualize_stage3_database_results(self):
        """
        STAGE 3 VISUALIZATION: Final Database Results
        Shows what's stored in MongoDB
        """
        print("\n[VIZ-STAGE3] Creating database results visualization...")

        if not self.db_data:
            print("  [SKIP] No database data to visualize")
            return

        fig = plt.figure(figsize=(18, 12))
        fig.suptitle('STAGE 3: DATABASE RESULTS (Final Stored Analytics)',
                     fontsize=18, fontweight='bold', y=0.995)

        gs = GridSpec(3, 3, figure=fig, hspace=0.4, wspace=0.3)

        # Plot 1: Analytics Types Stored
        ax1 = fig.add_subplot(gs[0, 0])
        type_counts = {k: len(v) for k, v in self.db_data.items()}
        if type_counts:
            pd.Series(type_counts).plot(kind='bar', ax=ax1, color='purple')
            ax1.set_title('Analytics Types in Database', fontweight='bold')
            ax1.set_xlabel('Analytics Type')
            ax1.set_ylabel('Record Count')
            ax1.tick_params(axis='x', rotation=45)

        # Plot 2: Entity Sentiment (from DB)
        ax2 = fig.add_subplot(gs[0, 1])
        if 'entity_sentiment' in self.db_data:
            df = self.db_data['entity_sentiment']
            if not df.empty and 'symbol' in df.columns:
                mentions = df.groupby('symbol')['total_mentions'].sum().nlargest(8)
                mentions.plot(kind='bar', ax=ax2, color='steelblue')
                ax2.set_title('Top Symbols (Database)', fontweight='bold')
                ax2.set_ylabel('Mentions')
                ax2.tick_params(axis='x', rotation=45)

        # Plot 3: Sentiment Scores (from DB)
        ax3 = fig.add_subplot(gs[0, 2])
        if 'entity_sentiment' in self.db_data:
            df = self.db_data['entity_sentiment']
            if not df.empty and 'avg_sentiment' in df.columns:
                ax3.hist(df['avg_sentiment'], bins=15, color='teal', alpha=0.7)
                ax3.axvline(x=0, color='red', linestyle='--', linewidth=2)
                ax3.set_title('Sentiment Distribution (Database)', fontweight='bold')
                ax3.set_xlabel('Sentiment')

        # Plot 4: Topic Correlation (from DB)
        ax4 = fig.add_subplot(gs[1, 0])
        if 'topic_correlation' in self.db_data:
            df = self.db_data['topic_correlation']
            if not df.empty and 'topic' in df.columns:
                topic_counts = df.groupby('topic').size().nlargest(8)
                topic_counts.plot(kind='barh', ax=ax4, color='orange')
                ax4.set_title('Topics (Database)', fontweight='bold')
                ax4.set_xlabel('Count')

        # Plot 5: Divergences (from DB) - KEY INSIGHT!
        ax5 = fig.add_subplot(gs[1, 1:])
        if 'sentiment_onchain_divergence' in self.db_data:
            df = self.db_data['sentiment_onchain_divergence']
            if not df.empty:
                div_summary = df.groupby(['symbol', 'divergence_type']).size().unstack(fill_value=0)
                div_summary.plot(kind='bar', stacked=True, ax=ax5, color=['green', 'red'])
                ax5.set_title('[KEY INSIGHT] Divergences Stored in Database',
                             fontweight='bold', fontsize=14)
                ax5.set_xlabel('Symbol')
                ax5.set_ylabel('Count')
                ax5.tick_params(axis='x', rotation=45)
                ax5.legend(title='Type')

        # Plot 6: Database Summary
        ax6 = fig.add_subplot(gs[2, :])
        ax6.axis('off')

        summary = f"""
DATABASE STORAGE SUMMARY:

Total Records: {sum(len(v) for v in self.db_data.values())}
Analytics Types: {len(self.db_data)}

Breakdown by Type:
"""
        for atype, df in self.db_data.items():
            summary += f"\n- {atype}: {len(df)} records"

        summary += "\n\nDatabase: financial_analytics.enhanced_semantic_analytics"
        summary += "\nQuery: db.enhanced_semantic_analytics.find()"

        ax6.text(0.1, 0.9, summary, transform=ax6.transAxes,
                fontsize=11, verticalalignment='top', family='monospace',
                bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.3))

        output_path = OUTPUT_DIR / "stage3_database_results.png"
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"  [OK] Saved: {output_path}")
        plt.close()

    # ========================================================================
    # MASTER COMPARISON VISUALIZATION
    # ========================================================================

    def visualize_3stage_comparison(self):
        """
        MASTER VISUALIZATION: Side-by-side comparison of all 3 stages
        """
        print("\n[VIZ-MASTER] Creating 3-stage comparison...")

        fig = plt.figure(figsize=(20, 10))
        fig.suptitle('COMPLETE PIPELINE: 3-STAGE VISUALIZATION',
                     fontsize=20, fontweight='bold', y=0.995)

        gs = GridSpec(2, 3, figure=fig, hspace=0.3, wspace=0.3)

        # STAGE 1: Raw Data Summary
        ax1 = fig.add_subplot(gs[0, 0])
        ax1.axis('off')
        stage1_summary = f"""
STAGE 1: RAW DATA

Reddit Posts: {len(self.raw_data.get('reddit', []))}
Twitter Tweets: {len(self.raw_data.get('twitter', []))}
On-Chain TXs: {len(self.raw_data.get('onchain', []))}

Status: Collected from sources
Format: Unprocessed text + metadata
"""
        ax1.text(0.1, 0.9, stage1_summary, transform=ax1.transAxes,
                fontsize=12, verticalalignment='top', family='monospace',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
        ax1.set_title('STAGE 1: Raw Collection', fontsize=14, fontweight='bold')

        # STAGE 2: Processed Data Summary
        ax2 = fig.add_subplot(gs[0, 1])
        ax2.axis('off')
        stage2_summary = f"""
STAGE 2: PROCESSING

Entities Extracted: {len(self.processed_data.get('entities_extracted', []))}
Topics Detected: {len(self.processed_data.get('topics_detected', []))}
Sentiment Scores: {len(self.processed_data.get('sentiment_scores', []))}
Divergences Found: {len(self.processed_data.get('divergences_found', []))}

Status: Analyzed & Enriched
Format: Structured analytics
"""
        ax2.text(0.1, 0.9, stage2_summary, transform=ax2.transAxes,
                fontsize=12, verticalalignment='top', family='monospace',
                bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.5))
        ax2.set_title('STAGE 2: Analytics Processing', fontsize=14, fontweight='bold')

        # STAGE 3: Database Summary
        ax3 = fig.add_subplot(gs[0, 2])
        ax3.axis('off')
        total_db_records = sum(len(v) for v in self.db_data.values())
        stage3_summary = f"""
STAGE 3: DATABASE

Total Records: {total_db_records}
Analytics Types: {len(self.db_data)}

Types Stored:
{chr(10).join(f"- {k}: {len(v)}" for k, v in list(self.db_data.items())[:5])}

Status: Persisted in MongoDB
Format: Queryable documents
"""
        ax3.text(0.1, 0.9, stage3_summary, transform=ax3.transAxes,
                fontsize=12, verticalalignment='top', family='monospace',
                bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.5))
        ax3.set_title('STAGE 3: Database Storage', fontsize=14, fontweight='bold')

        # Data Flow Diagram
        ax4 = fig.add_subplot(gs[1, :])
        ax4.axis('off')

        # Draw flow
        flow_text = """
        [RAW DATA]  →  [PROCESSING]  →  [DATABASE]  →  [VISUALIZATION]
           ↓               ↓                ↓               ↓
        Reddit       Entities         MongoDB      Dashboards
        Twitter      Topics           Analytics     Charts
        On-Chain     Sentiment        Results       Insights
                     Divergences
        """

        ax4.text(0.5, 0.6, flow_text, transform=ax4.transAxes,
                fontsize=13, verticalalignment='center', ha='center',
                family='monospace',
                bbox=dict(boxstyle='round', facecolor='yellow', alpha=0.3))

        # Add arrows and labels
        ax4.arrow(0.15, 0.5, 0.15, 0, head_width=0.05, head_length=0.02,
                 fc='blue', ec='blue', transform=ax4.transAxes, linewidth=3)
        ax4.arrow(0.45, 0.5, 0.15, 0, head_width=0.05, head_length=0.02,
                 fc='blue', ec='blue', transform=ax4.transAxes, linewidth=3)
        ax4.arrow(0.75, 0.5, 0.15, 0, head_width=0.05, head_length=0.02,
                 fc='blue', ec='blue', transform=ax4.transAxes, linewidth=3)

        ax4.set_title('PIPELINE FLOW', fontsize=16, fontweight='bold')

        output_path = OUTPUT_DIR / "stage0_3stage_comparison.png"
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"  [OK] Saved: {output_path}")
        plt.close()

    def generate_all_3stage_visualizations(self):
        """Generate all 3-stage visualizations"""
        print("=" * 80)
        print("3-STAGE PIPELINE VISUALIZATION")
        print("=" * 80)
        print(f"Output: {OUTPUT_DIR.absolute()}")
        print("=" * 80)

        # Load and visualize each stage
        self.load_raw_data()
        self.visualize_stage1_raw_data()

        self.process_data_for_visualization()
        self.visualize_stage2_processed_data()

        self.load_database_results()
        self.visualize_stage3_database_results()

        self.visualize_3stage_comparison()

        print("\n" + "=" * 80)
        print("[OK] ALL 3-STAGE VISUALIZATIONS GENERATED")
        print("=" * 80)
        print(f"\nCheck {OUTPUT_DIR.absolute()} for:")
        print("  - stage0_3stage_comparison.png (Master comparison)")
        print("  - stage1_raw_data.png (Raw collected data)")
        print("  - stage2_processed_data.png (Intermediate analytics)")
        print("  - stage3_database_results.png (Final DB results)")
        print("=" * 80)

    def close(self):
        """Cleanup"""
        if self.mongo_client:
            self.mongo_client.close()


def main():
    """Main entry point"""
    visualizer = ThreeStageVisualizer()

    try:
        visualizer.generate_all_3stage_visualizations()
    except Exception as e:
        print(f"\n[FAIL] Visualization error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        visualizer.close()


if __name__ == "__main__":
    main()
