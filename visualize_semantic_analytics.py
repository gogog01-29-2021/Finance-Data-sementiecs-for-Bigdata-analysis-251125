#!/usr/bin/env python3
"""
SEMANTIC ANALYTICS VISUALIZATION
Creates comprehensive visualizations of enhanced semantic analytics
"""

import os
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np
from pymongo import MongoClient
from dotenv import load_dotenv

# Plotting libraries
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import seaborn as sns
from matplotlib.gridspec import GridSpec

load_dotenv()

# Configuration
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "financial_analytics")
OUTPUT_DIR = Path("visualizations")
OUTPUT_DIR.mkdir(exist_ok=True)

# Set style
sns.set_style("darkgrid")
plt.rcParams['figure.figsize'] = (15, 10)
plt.rcParams['font.size'] = 10


class SemanticAnalyticsVisualizer:
    """Visualize enhanced semantic analytics"""

    def __init__(self):
        self.mongo_client = MongoClient(MONGODB_URI)
        self.db = self.mongo_client[MONGODB_DATABASE]
        self.collection = self.db["enhanced_semantic_analytics"]

    def fetch_data(self, analytics_type: str) -> pd.DataFrame:
        """Fetch data from MongoDB"""
        cursor = self.collection.find({"analytics_type": analytics_type})
        df = pd.DataFrame(list(cursor))

        if df.empty:
            print(f"[viz] No data found for {analytics_type}")
            return None

        # Remove MongoDB _id field
        if '_id' in df.columns:
            df = df.drop('_id', axis=1)

        print(f"[viz] Loaded {len(df)} records for {analytics_type}")
        return df

    def visualize_entity_sentiment(self):
        """
        VISUALIZATION 1: Entity-Specific Sentiment
        Shows sentiment by crypto symbol and source (Reddit vs Twitter)
        """
        print("\n[viz-1] Creating entity sentiment visualization...")

        df = self.fetch_data("entity_sentiment")
        if df is None or df.empty:
            print("  [SKIP] No entity sentiment data")
            return

        fig = plt.figure(figsize=(16, 10))
        gs = GridSpec(2, 2, figure=fig, hspace=0.3, wspace=0.3)

        # Plot 1: Mentions by Symbol
        ax1 = fig.add_subplot(gs[0, 0])
        mentions_df = df.groupby('symbol')['total_mentions'].sum().sort_values(ascending=False).head(10)
        mentions_df.plot(kind='bar', ax=ax1, color='steelblue')
        ax1.set_title('Top 10 Most Mentioned Cryptocurrencies', fontsize=14, fontweight='bold')
        ax1.set_xlabel('Symbol')
        ax1.set_ylabel('Total Mentions')
        ax1.tick_params(axis='x', rotation=45)

        # Plot 2: Sentiment by Symbol
        ax2 = fig.add_subplot(gs[0, 1])
        sentiment_df = df.groupby('symbol')['avg_sentiment'].mean().sort_values(ascending=False).head(10)

        colors = ['green' if x > 0 else 'red' for x in sentiment_df.values]
        sentiment_df.plot(kind='barh', ax=ax2, color=colors)
        ax2.set_title('Average Sentiment by Symbol', fontsize=14, fontweight='bold')
        ax2.set_xlabel('Average Sentiment')
        ax2.set_ylabel('Symbol')
        ax2.axvline(x=0, color='black', linestyle='--', linewidth=1)

        # Plot 3: Reddit vs Twitter Comparison
        ax3 = fig.add_subplot(gs[1, 0])
        source_comparison = df.groupby(['symbol', 'source'])['avg_sentiment'].mean().unstack(fill_value=0)

        if not source_comparison.empty:
            source_comparison.head(8).plot(kind='bar', ax=ax3, width=0.8)
            ax3.set_title('Reddit vs Twitter Sentiment', fontsize=14, fontweight='bold')
            ax3.set_xlabel('Symbol')
            ax3.set_ylabel('Average Sentiment')
            ax3.tick_params(axis='x', rotation=45)
            ax3.legend(title='Source')
            ax3.axhline(y=0, color='black', linestyle='--', linewidth=1)

        # Plot 4: Urgency vs Sentiment Scatter
        ax4 = fig.add_subplot(gs[1, 1])
        scatter_df = df[df['avg_urgency'] > 0]

        if not scatter_df.empty:
            scatter = ax4.scatter(
                scatter_df['avg_sentiment'],
                scatter_df['avg_urgency'],
                s=scatter_df['total_mentions'] * 3,
                alpha=0.6,
                c=scatter_df['avg_sentiment'],
                cmap='RdYlGn'
            )

            for idx, row in scatter_df.iterrows():
                if row['total_mentions'] > scatter_df['total_mentions'].quantile(0.7):
                    ax4.annotate(
                        row['symbol'],
                        (row['avg_sentiment'], row['avg_urgency']),
                        fontsize=8
                    )

            ax4.set_title('Sentiment vs Urgency (size = mentions)', fontsize=14, fontweight='bold')
            ax4.set_xlabel('Average Sentiment')
            ax4.set_ylabel('Average Urgency Score')
            plt.colorbar(scatter, ax=ax4, label='Sentiment')

        plt.suptitle('Entity-Specific Sentiment Analysis', fontsize=16, fontweight='bold', y=0.995)

        output_path = OUTPUT_DIR / "1_entity_sentiment.png"
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"  ✓ Saved: {output_path}")
        plt.close()

    def visualize_topic_correlation(self):
        """
        VISUALIZATION 2: Topic-Price Correlation
        Shows which topics are trending for each crypto
        """
        print("\n[viz-2] Creating topic correlation visualization...")

        df = self.fetch_data("topic_correlation")
        if df is None or df.empty:
            print("  [SKIP] No topic correlation data")
            return

        fig = plt.figure(figsize=(16, 10))
        gs = GridSpec(2, 2, figure=fig, hspace=0.3, wspace=0.3)

        # Plot 1: Topic Distribution
        ax1 = fig.add_subplot(gs[0, :])
        topic_counts = df.groupby('topic')['topic_mentions'].sum().sort_values(ascending=False)

        colors_map = {
            'bullish': 'green',
            'bearish': 'red',
            'technical_analysis': 'blue',
            'regulation': 'orange',
            'long_term': 'purple',
            'fomo': 'magenta',
            'whale_activity': 'cyan'
        }

        colors = [colors_map.get(topic, 'gray') for topic in topic_counts.index]

        topic_counts.plot(kind='bar', ax=ax1, color=colors)
        ax1.set_title('Topic Distribution Across All Cryptocurrencies', fontsize=14, fontweight='bold')
        ax1.set_xlabel('Topic')
        ax1.set_ylabel('Total Mentions')
        ax1.tick_params(axis='x', rotation=45)

        # Plot 2: Topic Heatmap by Symbol
        ax2 = fig.add_subplot(gs[1, 0])

        if 'symbol' in df.columns and 'topic' in df.columns:
            heatmap_data = df.groupby(['symbol', 'topic'])['topic_mentions'].sum().unstack(fill_value=0)

            if not heatmap_data.empty:
                # Limit to top 10 symbols by total mentions
                top_symbols = heatmap_data.sum(axis=1).nlargest(10).index
                heatmap_data = heatmap_data.loc[top_symbols]

                sns.heatmap(
                    heatmap_data,
                    annot=True,
                    fmt='.0f',
                    cmap='YlOrRd',
                    ax=ax2,
                    cbar_kws={'label': 'Mentions'}
                )
                ax2.set_title('Topic Mentions by Symbol (Heatmap)', fontsize=14, fontweight='bold')
                ax2.set_xlabel('Topic')
                ax2.set_ylabel('Symbol')

        # Plot 3: Bullish vs Bearish Ratio
        ax3 = fig.add_subplot(gs[1, 1])

        bullish_bearish = df[df['topic'].isin(['bullish', 'bearish'])]

        if not bullish_bearish.empty:
            ratio_df = bullish_bearish.groupby(['symbol', 'topic'])['topic_mentions'].sum().unstack(fill_value=0)

            if 'bullish' in ratio_df.columns and 'bearish' in ratio_df.columns:
                ratio_df['ratio'] = ratio_df['bullish'] / (ratio_df['bearish'] + 1)  # Avoid division by zero
                ratio_df = ratio_df.sort_values('ratio', ascending=False).head(10)

                colors = ['green' if x > 1 else 'red' for x in ratio_df['ratio'].values]

                ratio_df['ratio'].plot(kind='barh', ax=ax3, color=colors)
                ax3.set_title('Bullish/Bearish Ratio by Symbol', fontsize=14, fontweight='bold')
                ax3.set_xlabel('Ratio (>1 = Bullish, <1 = Bearish)')
                ax3.set_ylabel('Symbol')
                ax3.axvline(x=1, color='black', linestyle='--', linewidth=1)

        plt.suptitle('Topic-Price Correlation Analysis', fontsize=16, fontweight='bold', y=0.995)

        output_path = OUTPUT_DIR / "2_topic_correlation.png"
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"  ✓ Saved: {output_path}")
        plt.close()

    def visualize_sentiment_onchain_divergence(self):
        """
        VISUALIZATION 3: Sentiment vs On-Chain Divergence (KEY INSIGHT!)
        Shows when sentiment doesn't match actual behavior
        """
        print("\n[viz-3] Creating sentiment-onchain divergence visualization...")

        df = self.fetch_data("sentiment_onchain_divergence")
        if df is None or df.empty:
            print("  [SKIP] No divergence data")
            return

        fig = plt.figure(figsize=(16, 10))
        gs = GridSpec(2, 2, figure=fig, hspace=0.3, wspace=0.3)

        # Plot 1: Divergence Types Distribution
        ax1 = fig.add_subplot(gs[0, 0])
        divergence_counts = df['divergence_type'].value_counts()

        colors = {
            'BEARISH_DIVERGENCE': 'red',
            'BULLISH_DIVERGENCE': 'green'
        }

        plot_colors = [colors.get(x, 'gray') for x in divergence_counts.index]

        divergence_counts.plot(kind='bar', ax=ax1, color=plot_colors)
        ax1.set_title('Divergence Types Distribution', fontsize=14, fontweight='bold')
        ax1.set_xlabel('Divergence Type')
        ax1.set_ylabel('Count')
        ax1.tick_params(axis='x', rotation=45)

        # Plot 2: Divergence by Symbol
        ax2 = fig.add_subplot(gs[0, 1])
        symbol_divergence = df.groupby(['symbol', 'divergence_type']).size().unstack(fill_value=0)

        if not symbol_divergence.empty:
            symbol_divergence.plot(kind='bar', stacked=True, ax=ax2, color=['green', 'red'])
            ax2.set_title('Divergences by Symbol', fontsize=14, fontweight='bold')
            ax2.set_xlabel('Symbol')
            ax2.set_ylabel('Count')
            ax2.tick_params(axis='x', rotation=45)
            ax2.legend(title='Type')

        # Plot 3: Sentiment vs Whale Activity
        ax3 = fig.add_subplot(gs[1, 0])

        if 'avg_sentiment' in df.columns and 'whale_count' in df.columns:
            scatter = ax3.scatter(
                df['avg_sentiment'],
                df['whale_count'],
                s=100,
                alpha=0.6,
                c=['red' if x == 'BEARISH_DIVERGENCE' else 'green' for x in df['divergence_type']]
            )

            for idx, row in df.iterrows():
                if row['whale_count'] > 0:
                    ax3.annotate(
                        row['symbol'],
                        (row['avg_sentiment'], row['whale_count']),
                        fontsize=8
                    )

            ax3.set_title('Sentiment vs Whale Activity', fontsize=14, fontweight='bold')
            ax3.set_xlabel('Average Sentiment')
            ax3.set_ylabel('Whale Transaction Count')
            ax3.axvline(x=0, color='black', linestyle='--', linewidth=1)

            # Legend
            red_patch = mpatches.Patch(color='red', label='Bearish Divergence')
            green_patch = mpatches.Patch(color='green', label='Bullish Divergence')
            ax3.legend(handles=[red_patch, green_patch])

        # Plot 4: Accumulation vs Distribution
        ax4 = fig.add_subplot(gs[1, 1])

        if 'accumulation_count' in df.columns and 'distribution_count' in df.columns:
            acc_dist = df.groupby('symbol')[['accumulation_count', 'distribution_count']].sum()
            acc_dist = acc_dist.nlargest(10, 'accumulation_count')

            acc_dist.plot(kind='barh', ax=ax4)
            ax4.set_title('Accumulation vs Distribution by Symbol', fontsize=14, fontweight='bold')
            ax4.set_xlabel('Transaction Count')
            ax4.set_ylabel('Symbol')
            ax4.legend(title='Type')

        plt.suptitle('Sentiment vs On-Chain Behavior Divergence (KEY INSIGHT!)', fontsize=16, fontweight='bold', y=0.995)

        output_path = OUTPUT_DIR / "3_sentiment_onchain_divergence.png"
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"  ✓ Saved: {output_path}")
        plt.close()

    def visualize_influence_weighted(self):
        """
        VISUALIZATION 4: Influence-Weighted Sentiment
        Shows how sentiment changes when weighted by user influence
        """
        print("\n[viz-4] Creating influence-weighted sentiment visualization...")

        df = self.fetch_data("influence_weighted")
        if df is None or df.empty:
            print("  [SKIP] No influence-weighted data")
            return

        fig = plt.figure(figsize=(16, 8))
        gs = GridSpec(1, 2, figure=fig, hspace=0.3, wspace=0.3)

        # Plot 1: Raw vs Weighted Sentiment
        ax1 = fig.add_subplot(gs[0, 0])

        if not df.empty:
            top_symbols = df.nlargest(15, 'mention_count')

            x = np.arange(len(top_symbols))
            width = 0.35

            ax1.bar(x - width/2, top_symbols['avg_raw_sentiment'], width, label='Raw Sentiment', alpha=0.8)
            ax1.bar(x + width/2, top_symbols['avg_weighted_sentiment'], width, label='Influence-Weighted', alpha=0.8)

            ax1.set_title('Raw vs Influence-Weighted Sentiment', fontsize=14, fontweight='bold')
            ax1.set_xlabel('Symbol')
            ax1.set_ylabel('Average Sentiment')
            ax1.set_xticks(x)
            ax1.set_xticklabels(top_symbols['symbol'], rotation=45)
            ax1.legend()
            ax1.axhline(y=0, color='black', linestyle='--', linewidth=1)

        # Plot 2: Sentiment Adjustment
        ax2 = fig.add_subplot(gs[0, 1])

        if 'sentiment_adjustment' in df.columns:
            adjustment_df = df.nlargest(15, 'mention_count')

            colors = ['green' if x > 0 else 'red' for x in adjustment_df['sentiment_adjustment'].values]

            adjustment_df.set_index('symbol')['sentiment_adjustment'].plot(kind='barh', ax=ax2, color=colors)
            ax2.set_title('Sentiment Adjustment from Influence Weighting', fontsize=14, fontweight='bold')
            ax2.set_xlabel('Sentiment Adjustment')
            ax2.set_ylabel('Symbol')
            ax2.axvline(x=0, color='black', linestyle='--', linewidth=1)

        plt.suptitle('Influence-Weighted Sentiment Analysis', fontsize=16, fontweight='bold', y=0.98)

        output_path = OUTPUT_DIR / "4_influence_weighted.png"
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"  ✓ Saved: {output_path}")
        plt.close()

    def create_summary_dashboard(self):
        """
        Create a comprehensive summary dashboard
        """
        print("\n[viz-summary] Creating summary dashboard...")

        fig = plt.figure(figsize=(20, 12))
        fig.suptitle('Enhanced Semantic Analytics - Complete Dashboard', fontsize=20, fontweight='bold')

        # Add timestamp
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        fig.text(0.99, 0.01, f'Generated: {timestamp}', ha='right', fontsize=10, style='italic')

        # Create a complex grid
        gs = GridSpec(3, 3, figure=fig, hspace=0.4, wspace=0.3)

        # Fetch all data
        entity_df = self.fetch_data("entity_sentiment")
        topic_df = self.fetch_data("topic_correlation")
        divergence_df = self.fetch_data("sentiment_onchain_divergence")
        influence_df = self.fetch_data("influence_weighted")

        # Plot 1: Top Mentioned Symbols
        if entity_df is not None and not entity_df.empty:
            ax1 = fig.add_subplot(gs[0, 0])
            mentions = entity_df.groupby('symbol')['total_mentions'].sum().nlargest(8)
            mentions.plot(kind='bar', ax=ax1, color='steelblue')
            ax1.set_title('Top 8 Mentioned Cryptos', fontweight='bold')
            ax1.set_xlabel('Symbol')
            ax1.set_ylabel('Mentions')
            ax1.tick_params(axis='x', rotation=45)

        # Plot 2: Sentiment Distribution
        if entity_df is not None and not entity_df.empty:
            ax2 = fig.add_subplot(gs[0, 1])
            ax2.hist(entity_df['avg_sentiment'], bins=20, color='teal', alpha=0.7, edgecolor='black')
            ax2.set_title('Sentiment Distribution', fontweight='bold')
            ax2.set_xlabel('Sentiment Score')
            ax2.set_ylabel('Frequency')
            ax2.axvline(x=0, color='red', linestyle='--', linewidth=2)

        # Plot 3: Topic Breakdown
        if topic_df is not None and not topic_df.empty:
            ax3 = fig.add_subplot(gs[0, 2])
            topics = topic_df.groupby('topic')['topic_mentions'].sum()
            ax3.pie(topics, labels=topics.index, autopct='%1.1f%%', startangle=90)
            ax3.set_title('Topic Distribution', fontweight='bold')

        # Plot 4: Divergence Alert
        if divergence_df is not None and not divergence_df.empty:
            ax4 = fig.add_subplot(gs[1, :])
            div_summary = divergence_df.groupby(['symbol', 'divergence_type']).size().unstack(fill_value=0)
            div_summary.plot(kind='bar', stacked=True, ax=ax4, color=['green', 'red'])
            ax4.set_title('⚠️ DIVERGENCE ALERTS: Sentiment vs On-Chain Behavior', fontweight='bold', fontsize=14)
            ax4.set_xlabel('Symbol')
            ax4.set_ylabel('Divergence Count')
            ax4.tick_params(axis='x', rotation=45)
            ax4.legend(title='Type')

        # Plot 5: Influence Impact
        if influence_df is not None and not influence_df.empty:
            ax5 = fig.add_subplot(gs[2, 0])
            top_influence = influence_df.nlargest(8, 'mention_count')
            ax5.scatter(
                top_influence['avg_raw_sentiment'],
                top_influence['avg_weighted_sentiment'],
                s=top_influence['mention_count'] * 2,
                alpha=0.6,
                c='purple'
            )

            for idx, row in top_influence.iterrows():
                ax5.annotate(row['symbol'], (row['avg_raw_sentiment'], row['avg_weighted_sentiment']), fontsize=8)

            ax5.plot([-1, 1], [-1, 1], 'k--', alpha=0.3)
            ax5.set_title('Influence Impact', fontweight='bold')
            ax5.set_xlabel('Raw Sentiment')
            ax5.set_ylabel('Weighted Sentiment')

        # Plot 6: Source Comparison
        if entity_df is not None and not entity_df.empty and 'source' in entity_df.columns:
            ax6 = fig.add_subplot(gs[2, 1])
            source_data = entity_df.groupby('source')['total_mentions'].sum()
            source_data.plot(kind='bar', ax=ax6, color=['#1DA1F2', '#FF4500'])
            ax6.set_title('Reddit vs Twitter Mentions', fontweight='bold')
            ax6.set_xlabel('Source')
            ax6.set_ylabel('Total Mentions')
            ax6.tick_params(axis='x', rotation=0)

        # Plot 7: Key Metrics Box
        ax7 = fig.add_subplot(gs[2, 2])
        ax7.axis('off')

        metrics_text = "📊 KEY METRICS\n\n"

        if entity_df is not None and not entity_df.empty:
            metrics_text += f"Total Mentions: {entity_df['total_mentions'].sum():.0f}\n"
            metrics_text += f"Avg Sentiment: {entity_df['avg_sentiment'].mean():.3f}\n"
            metrics_text += f"Unique Symbols: {entity_df['symbol'].nunique()}\n\n"

        if divergence_df is not None and not divergence_df.empty:
            metrics_text += f"⚠️ Divergences: {len(divergence_df)}\n"
            bearish_div = len(divergence_df[divergence_df['divergence_type'] == 'BEARISH_DIVERGENCE'])
            bullish_div = len(divergence_df[divergence_df['divergence_type'] == 'BULLISH_DIVERGENCE'])
            metrics_text += f"  Bearish: {bearish_div}\n"
            metrics_text += f"  Bullish: {bullish_div}\n\n"

        if topic_df is not None and not topic_df.empty:
            total_topics = topic_df['topic_mentions'].sum()
            metrics_text += f"Topic Mentions: {total_topics:.0f}\n"

        ax7.text(0.1, 0.9, metrics_text, transform=ax7.transAxes,
                fontsize=12, verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.3))

        output_path = OUTPUT_DIR / "0_summary_dashboard.png"
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"  ✓ Saved: {output_path}")
        plt.close()

    def generate_all_visualizations(self):
        """Generate all visualizations"""
        print("=" * 80)
        print("SEMANTIC ANALYTICS VISUALIZATION")
        print("=" * 80)
        print(f"Output Directory: {OUTPUT_DIR.absolute()}")
        print("=" * 80)

        self.create_summary_dashboard()
        self.visualize_entity_sentiment()
        self.visualize_topic_correlation()
        self.visualize_sentiment_onchain_divergence()
        self.visualize_influence_weighted()

        print("\n" + "=" * 80)
        print("✓ ALL VISUALIZATIONS GENERATED")
        print("=" * 80)
        print(f"Check {OUTPUT_DIR.absolute()} for PNG files")
        print("=" * 80)

    def close(self):
        """Close MongoDB connection"""
        self.mongo_client.close()


def main():
    """Main entry point"""
    visualizer = SemanticAnalyticsVisualizer()

    try:
        visualizer.generate_all_visualizations()
    except Exception as e:
        print(f"\n✗ Visualization failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        visualizer.close()


if __name__ == "__main__":
    main()
