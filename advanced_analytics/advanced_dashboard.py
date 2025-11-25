#!/usr/bin/env python3
"""
ADVANCED CROSS-SOURCE ANALYTICS DASHBOARD
Visualizes deep analysis results:
- PageRank Word Importance
- Word Co-occurrence Network
- Formal vs Informal Language
- Word-Price Causality
- Social Media vs News Comparison
"""

import streamlit as st

st.set_page_config(
    page_title="Advanced Cross-Source Analytics",
    page_icon="🔬",
    layout="wide",
    initial_sidebar_state="expanded"
)

import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path
import json
import networkx as nx

# Directories
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data" / "batch"
OUTPUT_DIR = Path(__file__).parent / "spark_output"


@st.cache_data(ttl=300)
def load_output(filename):
    """Load analysis output file"""
    file_path = OUTPUT_DIR / filename
    if file_path.exists():
        if filename.endswith('.json'):
            with open(file_path, 'r') as f:
                return json.load(f)
        else:
            return pd.read_csv(file_path)
    return None


def main():
    st.title("🔬 Advanced Cross-Source Analytics Dashboard")

    st.markdown("""
    **Deep Analysis Features:**
    - **PageRank Algorithm** - Google-style word importance ranking
    - **Word Co-occurrence Network** - Graph visualization of word relationships
    - **Formal vs Informal** - News headlines vs Social media comparison
    - **Causality Analysis** - Word → Price and Price → Word relationships
    """)

    # Sidebar
    st.sidebar.title("Analysis Sections")
    section = st.sidebar.radio(
        "Select Analysis",
        ["📊 Overview",
         "🏆 PageRank Word Importance",
         "🕸️ Word Network Graph",
         "📝 Formal vs Informal",
         "📈 Word-Price Causality",
         "💬 Social Media Analysis",
         "🔄 Granger Causality"]
    )

    if section == "📊 Overview":
        show_overview()
    elif section == "🏆 PageRank Word Importance":
        show_pagerank()
    elif section == "🕸️ Word Network Graph":
        show_word_network()
    elif section == "📝 Formal vs Informal":
        show_formal_informal()
    elif section == "📈 Word-Price Causality":
        show_word_price()
    elif section == "💬 Social Media Analysis":
        show_social_media()
    elif section == "🔄 Granger Causality":
        show_granger()


def show_overview():
    """Show dashboard overview"""
    st.header("Analysis Overview")

    # Check available files
    available_files = list(OUTPUT_DIR.glob("*.csv")) + list(OUTPUT_DIR.glob("*.json"))

    if not available_files:
        st.warning("No analysis output found. Run `python advanced_spark_cross_analysis.py` first.")
        st.code("cd advanced_analytics && python advanced_spark_cross_analysis.py")
        return

    st.success(f"✓ {len(available_files)} analysis files available")

    # Load report
    report = load_output("advanced_analysis_report.json")
    if report:
        st.subheader("Analysis Report")
        col1, col2 = st.columns(2)

        with col1:
            st.write("**Generated at:**", report.get('generated_at', 'N/A'))
            st.write("**Engine:**", report.get('engine', 'N/A'))

        with col2:
            st.write("**Analyses Performed:**")
            for analysis in report.get('analyses_performed', []):
                st.write(f"  - {analysis}")

    # Quick stats
    st.subheader("Quick Statistics")

    col1, col2, col3 = st.columns(3)

    with col1:
        pagerank = load_output("word_pagerank.csv")
        if pagerank is not None:
            st.metric("PageRank Words", len(pagerank))
            st.write("Top 5 by PageRank:")
            for _, row in pagerank.head(5).iterrows():
                st.write(f"  {row['word']}: {row['pagerank']:.4f}")

    with col2:
        edges = load_output("word_graph_edges.csv")
        if edges is not None:
            st.metric("Word Connections", len(edges))
            st.write("Strongest connections:")
            for _, row in edges.head(5).iterrows():
                st.write(f"  {row['source']}↔{row['target']}: {row['weight']}")

    with col3:
        sentiment = load_output("sentiment_by_source.csv")
        if sentiment is not None:
            st.metric("Sources Analyzed", len(sentiment))
            for _, row in sentiment.iterrows():
                st.write(f"  {row['source']}: {row['avg_sentiment']:.3f}")


def show_pagerank():
    """Show PageRank analysis"""
    st.header("🏆 PageRank Word Importance")

    st.markdown("""
    **PageRank Algorithm** (Google's original search ranking algorithm) applied to words:
    - Words are nodes in a graph
    - Co-occurrence in headlines creates edges
    - PageRank finds the most "influential" words based on connections
    """)

    pagerank = load_output("word_pagerank.csv")

    if pagerank is None:
        st.warning("PageRank data not found.")
        return

    # Top words bar chart
    fig = px.bar(
        pagerank.head(30),
        x='pagerank',
        y='word',
        orientation='h',
        title='Top 30 Words by PageRank Score',
        color='pagerank',
        color_continuous_scale='Viridis'
    )
    fig.update_layout(height=700, yaxis={'categoryorder': 'total ascending'})
    st.plotly_chart(fig, use_container_width=True)

    # PageRank vs Frequency scatter
    st.subheader("PageRank vs Raw Frequency")

    fig = px.scatter(
        pagerank.head(100),
        x='frequency',
        y='pagerank',
        text='word',
        title='PageRank Score vs Word Frequency',
        labels={'frequency': 'Raw Frequency', 'pagerank': 'PageRank Score'}
    )
    fig.update_traces(textposition='top center', textfont_size=8)
    fig.update_layout(height=500)
    st.plotly_chart(fig, use_container_width=True)

    st.info("""
    **Interpretation:**
    - Words above the trend line have high influence relative to their frequency
    - These words are "hubs" that connect different topics
    - High PageRank + Low Frequency = Specialized but important term
    """)

    # Full table
    st.subheader("Full PageRank Results")
    st.dataframe(pagerank, use_container_width=True)


def show_word_network():
    """Show word co-occurrence network"""
    st.header("🕸️ Word Co-occurrence Network")

    edges = load_output("word_graph_edges.csv")

    if edges is None:
        st.warning("Word network data not found.")
        return

    # Filter controls
    min_weight = st.slider("Minimum connection strength", 1, int(edges['weight'].max()), 10)
    filtered_edges = edges[edges['weight'] >= min_weight]

    st.write(f"Showing {len(filtered_edges)} connections (min weight = {min_weight})")

    # Build network graph
    G = nx.Graph()
    for _, row in filtered_edges.iterrows():
        G.add_edge(row['source'], row['target'], weight=row['weight'])

    if len(G.nodes()) == 0:
        st.warning("No connections at this threshold. Try lowering the minimum weight.")
        return

    # Calculate positions
    pos = nx.spring_layout(G, k=2, iterations=50, seed=42)

    # Create Plotly figure
    edge_x = []
    edge_y = []
    edge_weights = []

    for edge in G.edges(data=True):
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])
        edge_weights.append(edge[2]['weight'])

    # Edge trace
    edge_trace = go.Scatter(
        x=edge_x, y=edge_y,
        line=dict(width=0.5, color='#888'),
        hoverinfo='none',
        mode='lines'
    )

    # Node trace
    node_x = [pos[node][0] for node in G.nodes()]
    node_y = [pos[node][1] for node in G.nodes()]
    node_degrees = [G.degree(node) for node in G.nodes()]

    node_trace = go.Scatter(
        x=node_x, y=node_y,
        mode='markers+text',
        hoverinfo='text',
        text=list(G.nodes()),
        textposition='top center',
        textfont=dict(size=10),
        marker=dict(
            showscale=True,
            colorscale='YlGnBu',
            size=[min(d * 3 + 10, 50) for d in node_degrees],
            color=node_degrees,
            colorbar=dict(title='Connections'),
            line_width=2
        )
    )

    fig = go.Figure(data=[edge_trace, node_trace],
                    layout=go.Layout(
                        title='Word Co-occurrence Network',
                        showlegend=False,
                        hovermode='closest',
                        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                        height=700
                    ))

    st.plotly_chart(fig, use_container_width=True)

    # Network statistics
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Nodes (Words)", len(G.nodes()))
    with col2:
        st.metric("Edges (Connections)", len(G.edges()))
    with col3:
        if len(G.nodes()) > 0:
            density = nx.density(G)
            st.metric("Network Density", f"{density:.4f}")

    # Top connections table
    st.subheader("Strongest Word Connections")
    st.dataframe(filtered_edges.head(20), use_container_width=True)


def show_formal_informal():
    """Show formal vs informal language comparison"""
    st.header("📝 Formal vs Informal Language Analysis")

    comparison = load_output("word_comparison_sources.csv")
    sentiment = load_output("sentiment_by_source.csv")

    if comparison is None:
        st.warning("Comparison data not found.")
        return

    st.markdown("""
    **Sources Compared:**
    - **News Headlines** - Formal, professional language
    - **YouTube Comments** - Casual, emoji-heavy
    - **Reddit (WSB-style)** - Very informal, meme language
    """)

    # Sentiment comparison
    if sentiment is not None:
        st.subheader("Sentiment by Source")

        fig = go.Figure()

        for _, row in sentiment.iterrows():
            fig.add_trace(go.Bar(
                name=row['source'].title(),
                x=['Positive', 'Negative', 'Neutral'],
                y=[row['positive_pct'], row['negative_pct'], row['neutral_pct']]
            ))

        fig.update_layout(
            barmode='group',
            title='Sentiment Distribution by Source',
            yaxis_title='Percentage (%)',
            height=400
        )
        st.plotly_chart(fig, use_container_width=True)

    # Word comparison
    st.subheader("Top Words by Source")

    # Filter to show meaningful comparisons
    comparison = comparison.sort_values('total_count', ascending=False).head(50)

    fig = go.Figure()

    fig.add_trace(go.Bar(name='News', x=comparison['word'], y=comparison['news_count'],
                         marker_color='blue'))
    fig.add_trace(go.Bar(name='YouTube', x=comparison['word'], y=comparison['youtube_count'],
                         marker_color='red'))
    fig.add_trace(go.Bar(name='Reddit', x=comparison['word'], y=comparison['reddit_count'],
                         marker_color='orange'))

    fig.update_layout(
        barmode='group',
        title='Word Frequency Comparison Across Sources',
        xaxis_tickangle=-45,
        height=500
    )
    st.plotly_chart(fig, use_container_width=True)

    # Source-specific words
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("News-Specific Words (Formal)")
        news_specific = comparison[
            (comparison['news_rank'] > 0) &
            (comparison['youtube_rank'] == -1) &
            (comparison['reddit_rank'] == -1)
        ].head(15)
        if not news_specific.empty:
            for _, row in news_specific.iterrows():
                st.write(f"  • {row['word']}: {row['news_count']}")
        else:
            st.write("  No news-specific words found")

    with col2:
        st.subheader("Social Media-Specific Words (Informal)")
        social_specific = comparison[
            (comparison['news_rank'] == -1) &
            ((comparison['youtube_rank'] > 0) | (comparison['reddit_rank'] > 0))
        ].head(15)
        if not social_specific.empty:
            for _, row in social_specific.iterrows():
                st.write(f"  • {row['word']}: YT={row['youtube_count']}, Reddit={row['reddit_count']}")
        else:
            st.write("  No social-specific words found")

    st.dataframe(comparison, use_container_width=True)


def show_word_price():
    """Show word-price causality analysis"""
    st.header("📈 Word-Price Causality Analysis")

    word_before = load_output("word_before_price_lag1.csv")

    if word_before is None:
        st.warning("Word-price data not found.")
        return

    st.markdown("""
    **Analysis:** Which words predict price movements?
    - **Positive bias** = Word appears more before UP days
    - **Negative bias** = Word appears more before DOWN days
    """)

    # Top predictive words
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("🟢 Words Predicting UP")
        up_words = word_before.nlargest(15, 'predictive_bias')
        fig = px.bar(
            up_words,
            x='predictive_bias',
            y='word',
            orientation='h',
            color='predictive_bias',
            color_continuous_scale='Greens',
            title='Top Words Before UP Days'
        )
        fig.update_layout(yaxis={'categoryorder': 'total ascending'}, height=400)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("🔴 Words Predicting DOWN")
        down_words = word_before.nsmallest(15, 'predictive_bias')
        fig = px.bar(
            down_words,
            x='predictive_bias',
            y='word',
            orientation='h',
            color='predictive_bias',
            color_continuous_scale='Reds_r',
            title='Top Words Before DOWN Days'
        )
        fig.update_layout(yaxis={'categoryorder': 'total descending'}, height=400)
        st.plotly_chart(fig, use_container_width=True)

    # Distribution of predictive bias
    st.subheader("Distribution of Predictive Bias")
    fig = px.histogram(
        word_before,
        x='predictive_bias',
        nbins=50,
        title='Distribution of Word Predictive Bias',
        labels={'predictive_bias': 'Predictive Bias (positive = predicts UP)'}
    )
    fig.add_vline(x=0, line_dash="dash", line_color="gray")
    st.plotly_chart(fig, use_container_width=True)

    # Full table
    st.subheader("All Word-Price Predictions")
    st.dataframe(word_before.sort_values('predictive_bias', key=abs, ascending=False),
                 use_container_width=True)


def show_social_media():
    """Show social media analysis"""
    st.header("💬 Social Media Analysis")

    youtube = load_output("youtube_comments.csv")
    reddit = load_output("reddit_comments.csv")

    if youtube is None and reddit is None:
        st.warning("Social media data not found.")
        return

    st.markdown("""
    **Synthetic Social Media Data** for analysis comparison:
    - YouTube finance video comments
    - Reddit (WSB-style) posts
    """)

    tabs = st.tabs(["YouTube", "Reddit"])

    with tabs[0]:
        if youtube is not None:
            st.subheader("YouTube Comments Analysis")

            col1, col2 = st.columns(2)
            with col1:
                st.metric("Total Comments", len(youtube))
            with col2:
                st.metric("Avg Sentiment", f"{youtube['sentiment_score'].mean():.3f}")

            # Sentiment distribution
            sentiment_counts = youtube['sentiment'].value_counts()
            fig = px.pie(
                values=sentiment_counts.values,
                names=sentiment_counts.index,
                title='YouTube Sentiment Distribution',
                color=sentiment_counts.index,
                color_discrete_map={'positive': 'green', 'negative': 'red', 'neutral': 'gray'}
            )
            st.plotly_chart(fig, use_container_width=True)

            # Stock mentions
            stock_mentions = youtube['stock_mentioned'].value_counts()
            fig = px.bar(
                x=stock_mentions.index,
                y=stock_mentions.values,
                title='Stock Mentions in YouTube Comments'
            )
            st.plotly_chart(fig, use_container_width=True)

            # Sample comments
            st.subheader("Sample Comments")
            sample = youtube.sample(min(10, len(youtube)))
            for _, row in sample.iterrows():
                if row['sentiment'] == 'positive':
                    st.success(row['text'])
                elif row['sentiment'] == 'negative':
                    st.error(row['text'])
                else:
                    st.info(row['text'])

    with tabs[1]:
        if reddit is not None:
            st.subheader("Reddit (WSB-style) Analysis")

            col1, col2 = st.columns(2)
            with col1:
                st.metric("Total Posts", len(reddit))
            with col2:
                st.metric("Avg Sentiment", f"{reddit['sentiment_score'].mean():.3f}")

            # Sentiment distribution
            sentiment_counts = reddit['sentiment'].value_counts()
            fig = px.pie(
                values=sentiment_counts.values,
                names=sentiment_counts.index,
                title='Reddit Sentiment Distribution',
                color=sentiment_counts.index,
                color_discrete_map={'positive': 'green', 'negative': 'red', 'neutral': 'gray'}
            )
            st.plotly_chart(fig, use_container_width=True)

            # Stock mentions
            stock_mentions = reddit['stock_mentioned'].value_counts()
            fig = px.bar(
                x=stock_mentions.index,
                y=stock_mentions.values,
                title='Stock Mentions in Reddit Posts'
            )
            st.plotly_chart(fig, use_container_width=True)

            # Sample posts
            st.subheader("Sample Posts")
            sample = reddit.sample(min(10, len(reddit)))
            for _, row in sample.iterrows():
                if row['sentiment'] == 'positive':
                    st.success(row['text'])
                elif row['sentiment'] == 'negative':
                    st.error(row['text'])
                else:
                    st.info(row['text'])


def show_granger():
    """Show Granger causality analysis"""
    st.header("🔄 Granger Causality Analysis")

    granger = load_output("granger_correlations.csv")

    if granger is None:
        st.warning("Granger causality data not found.")
        return

    st.markdown("""
    **Granger Causality** tests whether past values of one variable help predict another:
    - **Sentiment → Return**: Does yesterday's sentiment predict today's return?
    - **Return → Sentiment**: Does yesterday's return predict today's sentiment?
    """)

    # Split by direction
    s2r = granger[granger['direction'] == 'sentiment_to_return']
    r2s = granger[granger['direction'] == 'return_to_sentiment']

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Sentiment → Return")
        fig = px.bar(
            s2r,
            x='lag',
            y='correlation',
            title='Correlation: Past Sentiment vs Future Return',
            labels={'lag': 'Lag (days)', 'correlation': 'Correlation'},
            color='correlation',
            color_continuous_scale='RdYlGn'
        )
        fig.add_hline(y=0, line_dash="dash")
        st.plotly_chart(fig, use_container_width=True)

        max_corr = s2r.loc[s2r['correlation'].abs().idxmax()]
        st.info(f"Strongest signal at lag {int(max_corr['lag'])}: r = {max_corr['correlation']:.4f}")

    with col2:
        st.subheader("Return → Sentiment")
        fig = px.bar(
            r2s,
            x='lag',
            y='correlation',
            title='Correlation: Past Return vs Future Sentiment',
            labels={'lag': 'Lag (days)', 'correlation': 'Correlation'},
            color='correlation',
            color_continuous_scale='RdYlGn'
        )
        fig.add_hline(y=0, line_dash="dash")
        st.plotly_chart(fig, use_container_width=True)

        max_corr = r2s.loc[r2s['correlation'].abs().idxmax()]
        st.info(f"Strongest signal at lag {int(max_corr['lag'])}: r = {max_corr['correlation']:.4f}")

    # Interpretation
    st.subheader("Interpretation")

    st.markdown("""
    **Key Questions Answered:**

    1. **Does news sentiment predict stock returns?**
       - Look at "Sentiment → Return" chart
       - Positive correlation at lag 1 = bullish news predicts up days

    2. **Do stock returns influence news sentiment?**
       - Look at "Return → Sentiment" chart
       - Positive correlation = good market days lead to more positive news

    3. **Which direction is stronger?**
       - Compare the magnitude of correlations
       - Tells us about market efficiency and news reactivity
    """)

    # Full table
    st.subheader("Full Correlation Data")
    st.dataframe(granger, use_container_width=True)


if __name__ == "__main__":
    main()
