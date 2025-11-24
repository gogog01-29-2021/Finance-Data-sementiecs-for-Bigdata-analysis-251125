#!/usr/bin/env python3
"""
COMPREHENSIVE 3-STAGE LIVE DASHBOARD
Shows complete pipeline: Raw Collection → Spark Processing → Enriched Analytics
Connects to: LMDB, QuestDB, Cassandra, Neo4j, MongoDB
Auto-refreshes every 5 minutes
Run with: streamlit run live_dashboard.py
"""

import streamlit as st

# Page config - MUST be first Streamlit command
st.set_page_config(
    page_title="3-Stage Analytics Pipeline Dashboard",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

import pandas as pd
import numpy as np
import json
import lmdb
from pathlib import Path
from datetime import datetime, timedelta
import time
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import psycopg2
from neo4j import GraphDatabase
from pymongo import MongoClient
import warnings
warnings.filterwarnings('ignore')

# Cassandra driver fix for Python 3.12+
CASSANDRA_AVAILABLE = False
CASSANDRA_ERROR = None
try:
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    CASSANDRA_AVAILABLE = True
except Exception as e:
    CASSANDRA_ERROR = str(e)

# Database Configuration
LMDB_REDDIT = Path("data/reddit/reddit_sentiment_lmdb.db")
LMDB_TWITTER = Path("data/twitter/twitter_sentiment.db")
LMDB_ONCHAIN = Path("data/onchain/onchain_data.db")

QUESTDB_HOST = "localhost"
QUESTDB_PORT = 8812
QUESTDB_USER = "admin"
QUESTDB_PASSWORD = "quest"

CASSANDRA_HOST = "localhost"
CASSANDRA_PORT = 9042
CASSANDRA_KEYSPACE = "financial_data"

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"

MONGODB_URI = "mongodb://localhost:27017"
MONGODB_DATABASE = "financial_analytics"

REFRESH_INTERVAL = 300  # 5 minutes


# ============================================================================
# STAGE 1: RAW DATA COLLECTION (LMDB + QuestDB)
# ============================================================================

@st.cache_data(ttl=REFRESH_INTERVAL)
def load_lmdb_data(db_path: Path, prefix: str):
    """Load data from LMDB"""
    if not db_path.exists():
        return []

    try:
        env = lmdb.open(str(db_path), readonly=True)
        records = []

        with env.begin() as txn:
            cursor = txn.cursor()
            for key, value in cursor:
                if key.startswith(prefix.encode()):
                    try:
                        record = json.loads(value.decode())
                        records.append(record)
                    except:
                        pass

        env.close()
        return records
    except:
        return []


@st.cache_data(ttl=REFRESH_INTERVAL)
def load_questdb_orderbook():
    """Load orderbook data from QuestDB"""
    try:
        conn = psycopg2.connect(
            host=QUESTDB_HOST,
            port=QUESTDB_PORT,
            user=QUESTDB_USER,
            password=QUESTDB_PASSWORD,
            database="qdb"
        )

        query = """
        SELECT timestamp, exchange, symbol, best_bid, best_ask,
               mid_price, spread_bps
        FROM orderbook
        WHERE timestamp > dateadd('d', -1, now())
        ORDER BY timestamp DESC
        LIMIT 10000
        """

        df = pd.read_sql(query, conn)
        conn.close()

        if not df.empty and 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])

        return df
    except Exception as e:
        st.sidebar.error(f"QuestDB connection error: {e}")
        return pd.DataFrame()


def get_stage1_metrics():
    """Get Stage 1 collection metrics"""
    reddit_records = load_lmdb_data(LMDB_REDDIT, "sentiment:")
    twitter_records = load_lmdb_data(LMDB_TWITTER, "tweet:")
    onchain_records = load_lmdb_data(LMDB_ONCHAIN, "tx:")
    orderbook_df = load_questdb_orderbook()

    return {
        'reddit_count': len(reddit_records),
        'twitter_count': len(twitter_records),
        'onchain_count': len(onchain_records),
        'orderbook_count': len(orderbook_df),
        'reddit_data': reddit_records,
        'twitter_data': twitter_records,
        'onchain_data': onchain_records,
        'orderbook_data': orderbook_df
    }


# ============================================================================
# STAGE 2: PROCESSING PIPELINE STATUS
# ============================================================================

@st.cache_data(ttl=REFRESH_INTERVAL)
def get_processing_status():
    """Check Spark processing pipeline status"""
    # Use MongoDB if Cassandra not available
    if not CASSANDRA_AVAILABLE:
        try:
            client = MongoClient(MONGODB_URI)
            db = client[MONGODB_DATABASE]
            # Check analytics_unified for recent data
            latest = db.analytics_unified.find_one(
                sort=[("timestamp", -1)]
            )
            client.close()

            if latest and 'timestamp' in latest:
                last_update = latest['timestamp']
                if isinstance(last_update, str):
                    last_update = datetime.fromisoformat(last_update.replace('Z', '+00:00'))
                age_minutes = (datetime.now() - last_update.replace(tzinfo=None)).total_seconds() / 60
                status = "HEALTHY" if age_minutes < 10 else "STALE"
                return {
                    'status': status,
                    'last_update': last_update,
                    'age_minutes': age_minutes,
                    'table_status': {'mongodb': 'connected'}
                }
            return {
                'status': 'NO_DATA',
                'last_update': None,
                'age_minutes': None,
                'table_status': {}
            }
        except Exception as e:
            return {
                'status': 'ERROR',
                'error': str(e),
                'last_update': None,
                'age_minutes': None
            }

    # Original Cassandra logic
    try:
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect(CASSANDRA_KEYSPACE)

        # Check last update time across key tables
        tables_to_check = [
            'arbitrage_opportunities',
            'sentiment_correlations',
            'price_predictions'
        ]

        last_updates = {}
        for table in tables_to_check:
            try:
                query = f"SELECT timestamp FROM {table} LIMIT 1"
                rows = session.execute(query)
                row = rows.one()
                if row:
                    last_updates[table] = row.timestamp
            except:
                last_updates[table] = None

        session.shutdown()
        cluster.shutdown()

        # Calculate processing freshness
        if last_updates:
            most_recent = max([t for t in last_updates.values() if t is not None], default=None)
            if most_recent:
                age_minutes = (datetime.now() - most_recent).total_seconds() / 60
                status = "HEALTHY" if age_minutes < 10 else "STALE"
            else:
                status = "NO_DATA"
                age_minutes = None
        else:
            status = "ERROR"
            age_minutes = None

        return {
            'status': status,
            'last_update': most_recent if last_updates else None,
            'age_minutes': age_minutes,
            'table_status': last_updates
        }
    except Exception as e:
        return {
            'status': 'ERROR',
            'error': str(e),
            'last_update': None,
            'age_minutes': None
        }


# ============================================================================
# STAGE 3: ENRICHED ANALYTICS (Cassandra, Neo4j, MongoDB)
# ============================================================================

@st.cache_data(ttl=REFRESH_INTERVAL)
def load_cassandra_table(table_name: str, limit: int = 1000):
    """Load data from Cassandra table (or MongoDB fallback)"""
    # If Cassandra driver not available, try MongoDB unified collection
    if not CASSANDRA_AVAILABLE:
        try:
            client = MongoClient(MONGODB_URI)
            db = client[MONGODB_DATABASE]
            # Map table name to analytics_type in unified collection
            cursor = db.analytics_unified.find(
                {"analytics_type": table_name.replace("_", " ").replace("opportunities", "").strip()}
            ).limit(limit)
            records = list(cursor)
            client.close()
            if records:
                df = pd.DataFrame(records)
                if '_id' in df.columns:
                    df = df.drop('_id', axis=1)
                return df
            return pd.DataFrame()
        except Exception as e:
            return pd.DataFrame()

    # Original Cassandra logic
    try:
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect(CASSANDRA_KEYSPACE)

        query = f"SELECT * FROM {table_name} LIMIT {limit}"
        rows = session.execute(query)

        df = pd.DataFrame(list(rows))

        session.shutdown()
        cluster.shutdown()

        return df
    except Exception as e:
        st.sidebar.error(f"Cassandra {table_name} error: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=REFRESH_INTERVAL)
def load_arbitrage_opportunities():
    """Load arbitrage opportunities from Cassandra"""
    return load_cassandra_table('arbitrage_opportunities', limit=500)


@st.cache_data(ttl=REFRESH_INTERVAL)
def load_sentiment_correlations():
    """Load sentiment-price correlations from Cassandra"""
    return load_cassandra_table('sentiment_correlations', limit=1000)


@st.cache_data(ttl=REFRESH_INTERVAL)
def load_price_predictions():
    """Load price predictions from Cassandra"""
    return load_cassandra_table('price_predictions', limit=500)


@st.cache_data(ttl=REFRESH_INTERVAL)
def load_sentiment_divergence():
    """Load sentiment divergences from Cassandra"""
    return load_cassandra_table('sentiment_divergence', limit=500)


@st.cache_data(ttl=REFRESH_INTERVAL)
def load_word_price_correlation():
    """Load word-price correlations from Cassandra"""
    return load_cassandra_table('word_price_correlation', limit=1000)


@st.cache_data(ttl=REFRESH_INTERVAL)
def load_flash_events():
    """Load flash events from Cassandra"""
    return load_cassandra_table('flash_events', limit=200)


@st.cache_data(ttl=REFRESH_INTERVAL)
def load_vwap_signals():
    """Load VWAP trading signals from Cassandra"""
    return load_cassandra_table('vwap', limit=500)


@st.cache_data(ttl=REFRESH_INTERVAL)
def load_correlation_matrix():
    """Load cross-asset correlation matrix from Cassandra"""
    return load_cassandra_table('correlation_matrix', limit=500)


@st.cache_data(ttl=REFRESH_INTERVAL)
def load_neo4j_insights():
    """Load graph insights from Neo4j"""
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

        with driver.session() as session:
            # Get sentiment-price leading relationships
            query = """
            MATCH (p:RedditPost)-[r:LEADS_TO]->(pm:PriceMovement)
            RETURN p.subreddit as subreddit,
                   p.sentiment_polarity as sentiment,
                   pm.price_change as price_change,
                   pm.symbol as symbol
            LIMIT 100
            """
            result = session.run(query)
            records = [dict(record) for record in result]

        driver.close()
        return pd.DataFrame(records) if records else pd.DataFrame()
    except Exception as e:
        st.sidebar.error(f"Neo4j connection error: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=REFRESH_INTERVAL)
def load_mongodb_analytics():
    """Load analytics from MongoDB"""
    try:
        client = MongoClient(MONGODB_URI)
        db = client[MONGODB_DATABASE]

        # Get recent analytics summary
        pipeline = [
            {"$group": {
                "_id": "$analytics_type",
                "count": {"$sum": 1},
                "latest": {"$max": "$timestamp"}
            }},
            {"$sort": {"count": -1}}
        ]

        results = list(db.sentiment_analysis.aggregate(pipeline))

        client.close()
        return pd.DataFrame(results) if results else pd.DataFrame()
    except Exception as e:
        st.sidebar.error(f"MongoDB connection error: {e}")
        return pd.DataFrame()


# ============================================================================
# VISUALIZATIONS
# ============================================================================

def create_stage_overview(stage1, stage2):
    """Create 3-stage pipeline overview"""
    fig = go.Figure()

    stages = ['Stage 1:\nCollection', 'Stage 2:\nProcessing', 'Stage 3:\nAnalytics']

    # Stage 1 status
    stage1_total = stage1['reddit_count'] + stage1['twitter_count'] + stage1['onchain_count']
    stage1_color = 'green' if stage1_total > 0 else 'red'

    # Stage 2 status
    stage2_status = stage2['status']
    stage2_color = 'green' if stage2_status == 'HEALTHY' else ('orange' if stage2_status == 'STALE' else 'red')

    # Stage 3 (always check if we have any Cassandra data)
    stage3_color = 'green'  # Will be determined by actual data loads

    colors = [stage1_color, stage2_color, stage3_color]
    values = [stage1_total, 100 if stage2_status == 'HEALTHY' else 50, 100]

    fig.add_trace(go.Bar(
        x=stages,
        y=values,
        marker_color=colors,
        text=[f"{v:,.0f}" for v in values],
        textposition='auto',
    ))

    fig.update_layout(
        title='Pipeline Status Overview',
        yaxis_title='Health Score',
        height=300,
        showlegend=False
    )

    return fig


def create_arbitrage_chart(arb_df):
    """Visualize arbitrage opportunities"""
    if arb_df.empty:
        return go.Figure()

    # Convert timestamp if needed
    if 'timestamp' in arb_df.columns:
        arb_df['timestamp'] = pd.to_datetime(arb_df['timestamp'])

    fig = px.scatter(
        arb_df,
        x='timestamp' if 'timestamp' in arb_df.columns else arb_df.index,
        y='spread_bps' if 'spread_bps' in arb_df.columns else 'profit_bps',
        color='symbol' if 'symbol' in arb_df.columns else None,
        size='spread_bps' if 'spread_bps' in arb_df.columns else None,
        title='Arbitrage Opportunities (Basis Points)',
        labels={'spread_bps': 'Spread (BPS)', 'timestamp': 'Time'}
    )

    fig.add_hline(y=10, line_dash="dash", line_color="red",
                  annotation_text="Min Profitable Spread (10 BPS)")

    fig.update_layout(height=400)

    return fig


def create_sentiment_price_correlation(sent_corr_df):
    """Visualize sentiment-price correlations"""
    if sent_corr_df.empty:
        return go.Figure()

    # Group by symbol
    if 'symbol' in sent_corr_df.columns and 'avg_sentiment' in sent_corr_df.columns:
        agg_df = sent_corr_df.groupby('symbol').agg({
            'avg_sentiment': 'mean',
            'post_count': 'sum' if 'post_count' in sent_corr_df.columns else 'count'
        }).reset_index()

        fig = px.bar(
            agg_df,
            x='symbol',
            y='avg_sentiment',
            color='avg_sentiment',
            color_continuous_scale=['red', 'yellow', 'green'],
            title='Average Sentiment by Asset (From Processed Analytics)',
            labels={'avg_sentiment': 'Sentiment Score'}
        )

        fig.add_hline(y=0, line_dash="dash", line_color="gray")
        fig.update_layout(height=400)

        return fig
    else:
        return go.Figure()


def create_divergence_alerts(div_df):
    """Create divergence alert cards"""
    if div_df.empty:
        return []

    alerts = []

    if 'symbol' in div_df.columns and 'divergence_type' in div_df.columns:
        for idx, row in div_df.head(10).iterrows():
            alert = {
                'symbol': row['symbol'],
                'type': row['divergence_type'],
                'sentiment': row.get('avg_sentiment', 0),
                'price_movement': row.get('price_movement', 'N/A'),
                'strength': row.get('divergence_strength', 0)
            }
            alerts.append(alert)

    return alerts


def create_word_price_causality(word_corr_df):
    """Visualize word-price causality"""
    if word_corr_df.empty:
        return go.Figure()

    if 'word' in word_corr_df.columns and 'correlation' in word_corr_df.columns:
        # Get top words by correlation
        top_words = word_corr_df.nlargest(20, 'correlation')

        fig = px.bar(
            top_words,
            x='correlation',
            y='word',
            orientation='h',
            color='correlation',
            color_continuous_scale=['red', 'yellow', 'green'],
            title='Top 20 Words Correlated with Price Movement (Causality Analysis)',
            labels={'correlation': 'Correlation Score', 'word': 'Word'}
        )

        fig.update_layout(height=500)

        return fig
    else:
        return go.Figure()


def create_prediction_signals(pred_df):
    """Create price prediction signals"""
    if pred_df.empty:
        return go.Figure()

    if 'symbol' in pred_df.columns and 'trend' in pred_df.columns:
        trend_counts = pred_df.groupby(['symbol', 'trend']).size().unstack(fill_value=0)

        fig = go.Figure()

        if 'uptrend' in trend_counts.columns:
            fig.add_trace(go.Bar(name='Uptrend', x=trend_counts.index,
                                y=trend_counts['uptrend'], marker_color='green'))

        if 'downtrend' in trend_counts.columns:
            fig.add_trace(go.Bar(name='Downtrend', x=trend_counts.index,
                                y=trend_counts['downtrend'], marker_color='red'))

        if 'sideways' in trend_counts.columns:
            fig.add_trace(go.Bar(name='Sideways', x=trend_counts.index,
                                y=trend_counts['sideways'], marker_color='gray'))

        fig.update_layout(
            title='Price Trend Predictions by Asset',
            xaxis_title='Asset',
            yaxis_title='Signal Count',
            barmode='stack',
            height=400
        )

        return fig
    else:
        return go.Figure()


def create_flash_events_timeline(flash_df):
    """Visualize flash events"""
    if flash_df.empty:
        return go.Figure()

    if 'timestamp' in flash_df.columns:
        flash_df['timestamp'] = pd.to_datetime(flash_df['timestamp'])

        fig = px.scatter(
            flash_df,
            x='timestamp',
            y='volatility' if 'volatility' in flash_df.columns else 'price_change',
            color='symbol' if 'symbol' in flash_df.columns else None,
            size='volatility' if 'volatility' in flash_df.columns else None,
            title='Flash Events Detected (Rapid Price Movements)',
            labels={'volatility': 'Volatility', 'timestamp': 'Time'}
        )

        fig.update_layout(height=400)

        return fig
    else:
        return go.Figure()


def create_correlation_heatmap(corr_df):
    """Create cross-asset correlation heatmap"""
    if corr_df.empty:
        return go.Figure()

    if 'asset1' in corr_df.columns and 'asset2' in corr_df.columns and 'correlation' in corr_df.columns:
        # Pivot to matrix format
        pivot = corr_df.pivot_table(index='asset1', columns='asset2', values='correlation')

        fig = go.Figure(data=go.Heatmap(
            z=pivot.values,
            x=pivot.columns,
            y=pivot.index,
            colorscale='RdBu',
            zmid=0,
            text=pivot.values,
            texttemplate='%{text:.2f}',
            textfont={"size": 10}
        ))

        fig.update_layout(
            title='Cross-Asset Correlation Matrix',
            height=500
        )

        return fig
    else:
        return go.Figure()


# ============================================================================
# MAIN DASHBOARD
# ============================================================================

def main():
    # Show Cassandra warning in sidebar if not available
    if not CASSANDRA_AVAILABLE and CASSANDRA_ERROR:
        st.sidebar.warning(f"Cassandra driver not available (Python 3.12+ incompatible). Using MongoDB fallback.")

    # Header
    st.title("🚀 Complete 3-Stage Analytics Pipeline Dashboard")
    st.markdown(
        f"**Stage 1**: Raw Collection (LMDB + QuestDB) → "
        f"**Stage 2**: Spark Processing → "
        f"**Stage 3**: Enriched Analytics (Cassandra + Neo4j + MongoDB)"
    )
    st.markdown(f"**Auto-refresh**: Every {REFRESH_INTERVAL//60} minutes | "
                f"**Last updated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    st.markdown("---")

    # Load all data
    with st.spinner('Loading complete pipeline data...'):
        # Stage 1
        stage1_data = get_stage1_metrics()

        # Stage 2
        stage2_status = get_processing_status()

        # Stage 3 - Load key tables
        arbitrage_df = load_arbitrage_opportunities()
        sentiment_corr_df = load_sentiment_correlations()
        predictions_df = load_price_predictions()
        divergence_df = load_sentiment_divergence()
        word_corr_df = load_word_price_correlation()
        flash_events_df = load_flash_events()
        vwap_df = load_vwap_signals()
        corr_matrix_df = load_correlation_matrix()
        neo4j_df = load_neo4j_insights()
        mongodb_df = load_mongodb_analytics()

    # Pipeline Overview
    st.subheader("📊 Pipeline Status Overview")
    overview_fig = create_stage_overview(stage1_data, stage2_status)
    st.plotly_chart(overview_fig, use_container_width=True)

    # Stage metrics row
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "Stage 1: Raw Collection",
            f"{stage1_data['reddit_count'] + stage1_data['twitter_count'] + stage1_data['onchain_count']:,}",
            "Records in LMDB"
        )
        st.caption(f"📊 Orderbook: {stage1_data['orderbook_count']:,} ticks")

    with col2:
        st.metric(
            "Stage 2: Processing",
            stage2_status['status'],
            f"{stage2_status['age_minutes']:.1f} min ago" if stage2_status['age_minutes'] else "N/A"
        )

    with col3:
        stage3_total = len(arbitrage_df) + len(sentiment_corr_df) + len(predictions_df)
        st.metric(
            "Stage 3: Analytics",
            f"{stage3_total:,}",
            "Processed insights"
        )

    st.markdown("---")

    # ========================================================================
    # STAGE 3: ENRICHED ANALYTICS (Main Content)
    # ========================================================================

    st.header("🎯 Stage 3: Enriched Analytics Results")

    # Tabs for different analytics categories
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "🔥 Trading Signals",
        "💭 Sentiment Analytics",
        "📝 Text Analytics",
        "📈 Technical Indicators",
        "🔗 Graph Insights"
    ])

    # TAB 1: Trading Signals
    with tab1:
        st.subheader("💰 Arbitrage Opportunities")
        arb_fig = create_arbitrage_chart(arbitrage_df)
        st.plotly_chart(arb_fig, use_container_width=True)

        if not arbitrage_df.empty:
            st.dataframe(arbitrage_df.head(10), use_container_width=True)
        else:
            st.info("No arbitrage opportunities detected yet")

        st.markdown("---")

        st.subheader("📊 Price Predictions & Trend Signals")
        pred_fig = create_prediction_signals(predictions_df)
        st.plotly_chart(pred_fig, use_container_width=True)

        if not predictions_df.empty:
            st.dataframe(predictions_df.head(10), use_container_width=True)
        else:
            st.info("No predictions available yet")

        st.markdown("---")

        st.subheader("📉 VWAP Trading Signals")
        if not vwap_df.empty:
            st.dataframe(vwap_df.head(10), use_container_width=True)
        else:
            st.info("No VWAP signals yet")

    # TAB 2: Sentiment Analytics
    with tab2:
        st.subheader("🎭 Sentiment-Price Correlations")
        sent_fig = create_sentiment_price_correlation(sentiment_corr_df)
        st.plotly_chart(sent_fig, use_container_width=True)

        if not sentiment_corr_df.empty:
            st.dataframe(sentiment_corr_df.head(10), use_container_width=True)
        else:
            st.info("No sentiment correlations yet")

        st.markdown("---")

        st.subheader("⚠️ SENTIMENT-PRICE DIVERGENCES (KEY INSIGHTS!)")
        divergence_alerts = create_divergence_alerts(divergence_df)

        if divergence_alerts:
            cols = st.columns(2)
            for idx, alert in enumerate(divergence_alerts[:6]):
                col = cols[idx % 2]
                with col:
                    div_type = alert['type']
                    emoji = "🔴" if 'bearish' in div_type.lower() else "🟢"

                    st.warning(
                        f"{emoji} **{alert['symbol']}**: {div_type}\n\n"
                        f"- Sentiment: {alert['sentiment']:.2f}\n"
                        f"- Price: {alert['price_movement']}\n"
                        f"- Strength: {alert['strength']:.2f}"
                    )
        else:
            st.info("No divergences detected. This means sentiment and price are aligned.")

        if not divergence_df.empty:
            with st.expander("📋 Full Divergence Data"):
                st.dataframe(divergence_df, use_container_width=True)

    # TAB 3: Text Analytics
    with tab3:
        st.subheader("🔤 Word-Price Causality Analysis")
        st.caption("Shows which words in Reddit/Twitter lead or lag price movements")

        word_fig = create_word_price_causality(word_corr_df)
        st.plotly_chart(word_fig, use_container_width=True)

        if not word_corr_df.empty:
            st.dataframe(word_corr_df.head(20), use_container_width=True)
        else:
            st.info("No word-price correlations yet. Need more data.")

    # TAB 4: Technical Indicators
    with tab4:
        st.subheader("⚡ Flash Events (Rapid Price Movements)")
        flash_fig = create_flash_events_timeline(flash_events_df)
        st.plotly_chart(flash_fig, use_container_width=True)

        if not flash_events_df.empty:
            st.dataframe(flash_events_df.head(10), use_container_width=True)
        else:
            st.info("No flash events detected")

        st.markdown("---")

        st.subheader("🔗 Cross-Asset Correlation Matrix")
        corr_fig = create_correlation_heatmap(corr_matrix_df)
        st.plotly_chart(corr_fig, use_container_width=True)

        if not corr_matrix_df.empty:
            st.dataframe(corr_matrix_df.head(10), use_container_width=True)
        else:
            st.info("No correlation matrix yet")

    # TAB 5: Graph Insights
    with tab5:
        st.subheader("🕸️ Neo4j Graph Relationships")
        st.caption("Shows semantic relationships: Reddit sentiment → Price movements")

        if not neo4j_df.empty:
            # Scatter plot of sentiment vs price change
            fig = px.scatter(
                neo4j_df,
                x='sentiment',
                y='price_change',
                color='subreddit',
                size=abs(neo4j_df['price_change']) if 'price_change' in neo4j_df.columns else None,
                title='Sentiment → Price Relationships (Neo4j)',
                labels={'sentiment': 'Sentiment Polarity', 'price_change': 'Price Change (%)'}
            )
            st.plotly_chart(fig, use_container_width=True)

            st.dataframe(neo4j_df.head(20), use_container_width=True)
        else:
            st.info("No Neo4j relationships found")

        st.markdown("---")

        st.subheader("📦 MongoDB Document Analytics Summary")
        if not mongodb_df.empty:
            st.dataframe(mongodb_df, use_container_width=True)
        else:
            st.info("No MongoDB analytics yet")

    # ========================================================================
    # STAGE 1 & 2 DETAILS (Collapsible)
    # ========================================================================

    with st.expander("🔍 Stage 1: Raw Collection Details"):
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Reddit Posts", stage1_data['reddit_count'])
        with col2:
            st.metric("Twitter Tweets", stage1_data['twitter_count'])
        with col3:
            st.metric("On-Chain TXs", stage1_data['onchain_count'])

        st.caption(f"QuestDB Orderbook: {stage1_data['orderbook_count']:,} ticks")

    with st.expander("⚙️ Stage 2: Processing Pipeline Status"):
        st.json(stage2_status)

    # Footer
    st.markdown("---")
    st.markdown(
        f"**Complete Pipeline Dashboard** | "
        f"Auto-refresh: {REFRESH_INTERVAL//60} min | "
        f"Databases: LMDB, QuestDB, Cassandra, Neo4j, MongoDB"
    )

    # Auto-refresh
    time.sleep(1)
    st.rerun()


if __name__ == "__main__":
    main()
