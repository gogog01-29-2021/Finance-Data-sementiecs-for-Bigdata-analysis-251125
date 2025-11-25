#!/usr/bin/env python3
"""
BATCH DATA ANALYTICS DASHBOARD
Comprehensive visualization for the multi-source data pipeline:
- RAW DATA: World Bank, FRED, Stock Market, News Headlines
- SPARK PROCESSED: Technical indicators, correlations, sentiment analysis
- Cross-source Analytics
"""

import streamlit as st

# Page config - MUST be first Streamlit command
st.set_page_config(
    page_title="Multi-Source Financial Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
from pathlib import Path
import json

# Data directories
DATA_DIR = Path(__file__).parent / "data" / "batch"
SPARK_OUTPUT_DIR = DATA_DIR / "spark_output"


# ============================================================
# DATA LOADING FUNCTIONS
# ============================================================

@st.cache_data(ttl=300)
def load_raw_stock_data():
    """Load raw stock data from CSV"""
    csv_file = DATA_DIR / "stocks" / "stock_prices.csv"
    if csv_file.exists():
        df = pd.read_csv(csv_file)
        df['date'] = pd.to_datetime(df['date'])
        return df
    return pd.DataFrame()


@st.cache_data(ttl=300)
def load_raw_economic_data():
    """Load raw economic data from CSV"""
    csv_file = DATA_DIR / "fred" / "economic_indicators.csv"
    if csv_file.exists():
        return pd.read_csv(csv_file)
    return pd.DataFrame()


@st.cache_data(ttl=300)
def load_raw_worldbank_data():
    """Load raw World Bank data from CSV"""
    csv_file = DATA_DIR / "worldbank" / "world_bank_indicators.csv"
    if csv_file.exists():
        return pd.read_csv(csv_file)
    return pd.DataFrame()


@st.cache_data(ttl=300)
def load_raw_news_data():
    """Load raw news headlines from CSV"""
    csv_file = DATA_DIR / "news" / "financial_news_headlines.csv"
    if csv_file.exists():
        df = pd.read_csv(csv_file)
        df['date'] = pd.to_datetime(df['date'])
        return df
    return pd.DataFrame()


@st.cache_data(ttl=300)
def load_spark_output(filename):
    """Load Spark analytics output"""
    file_path = SPARK_OUTPUT_DIR / filename
    if file_path.exists():
        if filename.endswith('.json'):
            with open(file_path, 'r') as f:
                return json.load(f)
        else:
            return pd.read_csv(file_path)
    return None


# ============================================================
# MAIN DASHBOARD
# ============================================================

def main():
    st.title("📊 Multi-Source Financial Analytics Dashboard")

    st.markdown("""
    **Data Pipeline Architecture:**
    - **Stage 1 (Raw Data):** World Bank (50+ years) | FRED Economic | Stock Market | News Headlines
    - **Stage 2 (Spark Processing):** Technical Indicators | Correlations | Sentiment Analysis | NLP
    - **Stage 3 (Visualization):** Interactive Charts | Cross-Source Analytics
    """)

    # Sidebar navigation
    st.sidebar.title("Navigation")

    section = st.sidebar.radio(
        "Select Section",
        ["📈 Overview",
         "📂 Raw Data Explorer",
         "⚡ Spark Analytics Results",
         "🔗 Cross-Source Analysis"]
    )

    if section == "📈 Overview":
        show_overview()
    elif section == "📂 Raw Data Explorer":
        show_raw_data()
    elif section == "⚡ Spark Analytics Results":
        show_spark_results()
    elif section == "🔗 Cross-Source Analysis":
        show_cross_analysis()


def show_overview():
    """Show dashboard overview with key metrics"""
    st.header("Dashboard Overview")

    # Load data counts
    stocks = load_raw_stock_data()
    economic = load_raw_economic_data()
    worldbank = load_raw_worldbank_data()
    news = load_raw_news_data()

    # Data availability metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("📈 Stock Records",
                  f"{len(stocks):,}" if not stocks.empty else "0",
                  f"{stocks['symbol'].nunique()} symbols" if not stocks.empty else "N/A")

    with col2:
        st.metric("💹 Economic Records",
                  f"{len(economic):,}" if not economic.empty else "0",
                  f"{economic['series_id'].nunique()} indicators" if not economic.empty else "N/A")

    with col3:
        st.metric("🌍 World Bank Records",
                  f"{len(worldbank):,}" if not worldbank.empty else "0",
                  f"{worldbank['country_code'].nunique()} countries" if not worldbank.empty else "N/A")

    with col4:
        st.metric("📰 News Headlines",
                  f"{len(news):,}" if not news.empty else "0",
                  f"{(news['sentiment']=='positive').mean()*100:.1f}% positive" if not news.empty else "N/A")

    st.divider()

    # Check Spark output availability
    st.subheader("Spark Analytics Status")

    spark_files = list(SPARK_OUTPUT_DIR.glob("*.csv")) + list(SPARK_OUTPUT_DIR.glob("*.json"))

    if spark_files:
        st.success(f"✓ {len(spark_files)} Spark output files available")

        col1, col2 = st.columns(2)
        with col1:
            st.write("**Available analytics:**")
            for f in spark_files[:10]:
                st.write(f"- {f.name}")
        with col2:
            # Load and show report summary if available
            report = load_spark_output("analytics_report.json")
            if report:
                st.write("**Latest Analysis:**")
                st.write(f"Generated: {report.get('generated_at', 'N/A')}")
                st.write(f"Stock records: {report.get('data_sources', {}).get('stocks', 'N/A'):,}")
                st.write(f"News headlines: {report.get('data_sources', {}).get('news_headlines', 'N/A'):,}")
    else:
        st.warning("⚠ No Spark analytics output found. Run `python batch_spark_analytics.py` first.")

    st.divider()

    # Quick visualizations
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Stock Price Overview")
        if not stocks.empty:
            # Latest prices
            latest = stocks.sort_values('date').groupby('symbol').last().reset_index()
            fig = px.bar(
                latest.sort_values('close', ascending=True).tail(15),
                y='symbol',
                x='close',
                orientation='h',
                title='Latest Stock Prices',
                color='close',
                color_continuous_scale='Viridis'
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("News Sentiment Distribution")
        if not news.empty:
            sentiment_counts = news['sentiment'].value_counts()
            fig = px.pie(
                values=sentiment_counts.values,
                names=sentiment_counts.index,
                title='Sentiment Distribution',
                color=sentiment_counts.index,
                color_discrete_map={
                    'positive': '#2ecc71',
                    'negative': '#e74c3c',
                    'neutral': '#95a5a6'
                }
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)


def show_raw_data():
    """Show raw data exploration"""
    st.header("📂 Raw Data Explorer")

    data_type = st.selectbox(
        "Select Data Source",
        ["Stock Market Data", "Economic Indicators (FRED)",
         "World Bank Data", "News Headlines"]
    )

    if data_type == "Stock Market Data":
        show_raw_stocks()
    elif data_type == "Economic Indicators (FRED)":
        show_raw_economic()
    elif data_type == "World Bank Data":
        show_raw_worldbank()
    elif data_type == "News Headlines":
        show_raw_news()


def show_raw_stocks():
    """Show raw stock data"""
    stocks = load_raw_stock_data()

    if stocks.empty:
        st.error("No stock data found. Run batch_data_downloader.py first.")
        return

    st.subheader("Raw Stock Market Data")

    # Data info
    col1, col2, col3 = st.columns(3)
    with col1:
        st.info(f"**Total Records:** {len(stocks):,}")
    with col2:
        st.info(f"**Symbols:** {stocks['symbol'].nunique()}")
    with col3:
        st.info(f"**Date Range:** {stocks['date'].min().date()} to {stocks['date'].max().date()}")

    # Symbol selection
    symbols = sorted(stocks['symbol'].unique())
    selected = st.multiselect("Select Symbols", symbols, default=symbols[:5])

    if selected:
        filtered = stocks[stocks['symbol'].isin(selected)]

        # Price chart
        fig = px.line(
            filtered,
            x='date',
            y='close',
            color='symbol',
            title='Stock Prices Over Time'
        )
        st.plotly_chart(fig, use_container_width=True)

        # Volume chart
        fig = px.bar(
            filtered.groupby(['date', 'symbol'])['volume'].sum().reset_index(),
            x='date',
            y='volume',
            color='symbol',
            title='Trading Volume'
        )
        st.plotly_chart(fig, use_container_width=True)

        # Raw data table
        st.subheader("Raw Data Table")
        st.dataframe(filtered.tail(100), use_container_width=True)


def show_raw_economic():
    """Show raw economic data"""
    economic = load_raw_economic_data()

    if economic.empty:
        st.error("No economic data found.")
        return

    st.subheader("Raw Economic Indicators (FRED-style)")

    # Indicator selection
    indicators = economic['series_id'].unique()
    selected = st.multiselect(
        "Select Indicators",
        indicators,
        default=list(indicators[:4])
    )

    if selected:
        for indicator in selected:
            data = economic[economic['series_id'] == indicator]
            name = data['series_name'].iloc[0]

            fig = px.line(
                data,
                x='year',
                y='value',
                title=name,
                markers=True
            )
            st.plotly_chart(fig, use_container_width=True)

        # Raw data
        st.subheader("Raw Data Table")
        st.dataframe(economic[economic['series_id'].isin(selected)], use_container_width=True)


def show_raw_worldbank():
    """Show raw World Bank data"""
    wb = load_raw_worldbank_data()

    if wb.empty:
        st.error("No World Bank data found.")
        return

    st.subheader("Raw World Bank Data (50+ Years)")

    col1, col2 = st.columns(2)

    with col1:
        # Country selection
        countries = sorted(wb['country_code'].unique())
        selected_countries = st.multiselect(
            "Select Countries",
            countries,
            default=['USA', 'CHN', 'JPN', 'DEU', 'GBR'][:min(5, len(countries))]
        )

    with col2:
        # Indicator selection
        indicators = wb['indicator_code'].unique()
        indicator_names = {
            row['indicator_code']: row['indicator_name']
            for _, row in wb.drop_duplicates('indicator_code').iterrows()
        }
        selected_indicator = st.selectbox(
            "Select Indicator",
            indicators,
            format_func=lambda x: indicator_names.get(x, x)[:50]
        )

    if selected_countries and selected_indicator:
        filtered = wb[
            (wb['country_code'].isin(selected_countries)) &
            (wb['indicator_code'] == selected_indicator)
        ]

        fig = px.line(
            filtered,
            x='year',
            y='value',
            color='country_name',
            title=indicator_names.get(selected_indicator, selected_indicator)
        )
        st.plotly_chart(fig, use_container_width=True)

        # Data table
        st.subheader("Raw Data Table")
        pivot = filtered.pivot_table(index='year', columns='country_name', values='value')
        st.dataframe(pivot.tail(30), use_container_width=True)


def show_raw_news():
    """Show raw news data"""
    news = load_raw_news_data()

    if news.empty:
        st.error("No news data found.")
        return

    st.subheader("Raw News Headlines")

    # Filters
    col1, col2, col3 = st.columns(3)

    with col1:
        sentiment_filter = st.multiselect(
            "Filter by Sentiment",
            ['positive', 'negative', 'neutral'],
            default=['positive', 'negative', 'neutral']
        )

    with col2:
        companies = news['company_mentioned'].dropna().unique()
        company_filter = st.multiselect(
            "Filter by Company",
            companies,
            default=[]
        )

    with col3:
        sources = news['source'].unique()
        source_filter = st.multiselect(
            "Filter by Source",
            sources,
            default=[]
        )

    # Apply filters
    filtered = news[news['sentiment'].isin(sentiment_filter)]
    if company_filter:
        filtered = filtered[filtered['company_mentioned'].isin(company_filter)]
    if source_filter:
        filtered = filtered[filtered['source'].isin(source_filter)]

    # Sentiment over time
    daily = filtered.groupby(filtered['date'].dt.date).agg({
        'sentiment_score': 'mean',
        'headline': 'count'
    }).reset_index()
    daily.columns = ['date', 'avg_sentiment', 'count']

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        subplot_titles=['Average Sentiment', 'Headlines Count'])

    fig.add_trace(go.Scatter(x=daily['date'], y=daily['avg_sentiment'],
                             mode='lines', name='Sentiment'), row=1, col=1)
    fig.add_trace(go.Bar(x=daily['date'], y=daily['count'],
                         name='Count'), row=2, col=1)
    fig.update_layout(height=500)
    st.plotly_chart(fig, use_container_width=True)

    # Headlines table
    st.subheader("Headlines")
    display_cols = ['date', 'headline', 'sentiment', 'sentiment_score', 'company_mentioned', 'source']
    st.dataframe(filtered[display_cols].head(100), use_container_width=True)


def show_spark_results():
    """Show Spark analytics results"""
    st.header("⚡ Spark Analytics Results")

    if not SPARK_OUTPUT_DIR.exists():
        st.error("Spark output directory not found. Run `python batch_spark_analytics.py` first.")
        return

    spark_files = list(SPARK_OUTPUT_DIR.glob("*.csv"))

    if not spark_files:
        st.warning("No Spark analytics output found. Run `python batch_spark_analytics.py` first.")
        return

    analysis_type = st.selectbox(
        "Select Analysis",
        ["Stock Summary & Technical Indicators",
         "Stock Correlations",
         "Sentiment Analysis",
         "Word Frequencies",
         "GDP Rankings",
         "Economic Trends",
         "Analytics Report"]
    )

    if analysis_type == "Stock Summary & Technical Indicators":
        show_spark_stock_summary()
    elif analysis_type == "Stock Correlations":
        show_spark_correlations()
    elif analysis_type == "Sentiment Analysis":
        show_spark_sentiment()
    elif analysis_type == "Word Frequencies":
        show_spark_word_freq()
    elif analysis_type == "GDP Rankings":
        show_spark_gdp()
    elif analysis_type == "Economic Trends":
        show_spark_economic()
    elif analysis_type == "Analytics Report":
        show_spark_report()


def show_spark_stock_summary():
    """Show Spark stock summary"""
    summary = load_spark_output("stock_summary_stats.csv")

    if summary is None:
        st.warning("Stock summary not found.")
        return

    st.subheader("Spark-Processed Stock Summary")

    # Key metrics
    fig = px.bar(
        summary.sort_values('annualized_return', ascending=False),
        x='symbol',
        y='annualized_return',
        color='annualized_volatility',
        title='Annualized Returns by Symbol',
        labels={'annualized_return': 'Annualized Return (%)',
                'annualized_volatility': 'Volatility (%)'}
    )
    st.plotly_chart(fig, use_container_width=True)

    # Risk-Return scatter
    fig = px.scatter(
        summary,
        x='annualized_volatility',
        y='annualized_return',
        text='symbol',
        title='Risk-Return Profile',
        labels={'annualized_volatility': 'Volatility (%)',
                'annualized_return': 'Return (%)'}
    )
    fig.update_traces(textposition='top center')
    st.plotly_chart(fig, use_container_width=True)

    # Sharpe ratios
    fig = px.bar(
        summary.sort_values('sharpe_ratio', ascending=False),
        x='symbol',
        y='sharpe_ratio',
        title='Sharpe Ratios',
        color='sharpe_ratio',
        color_continuous_scale='RdYlGn'
    )
    st.plotly_chart(fig, use_container_width=True)

    # Data table
    st.subheader("Full Summary Table")
    st.dataframe(summary, use_container_width=True)


def show_spark_correlations():
    """Show Spark correlation analysis"""
    corr = load_spark_output("stock_correlations.csv")

    if corr is None:
        st.warning("Correlation data not found.")
        return

    st.subheader("Spark-Processed Stock Correlations")

    # Heatmap
    fig = px.imshow(
        corr.set_index(corr.columns[0]),
        text_auto='.2f',
        aspect='auto',
        color_continuous_scale='RdBu_r',
        title='Stock Correlation Matrix'
    )
    fig.update_layout(height=600)
    st.plotly_chart(fig, use_container_width=True)


def show_spark_sentiment():
    """Show Spark sentiment analysis"""
    daily = load_spark_output("daily_sentiment_analysis.csv")
    company = load_spark_output("company_sentiment_analysis.csv")

    if daily is not None:
        st.subheader("Daily Sentiment Analysis (Spark Processed)")

        daily['date'] = pd.to_datetime(daily['date'])

        fig = make_subplots(rows=3, cols=1, shared_xaxes=True,
                            subplot_titles=['Average Sentiment', 'Sentiment Ratio', 'Headlines Count'])

        fig.add_trace(go.Scatter(x=daily['date'], y=daily['avg_sentiment'],
                                 mode='lines', name='Avg Sentiment'), row=1, col=1)
        fig.add_trace(go.Scatter(x=daily['date'], y=daily['positive_ratio'],
                                 mode='lines', name='Positive %', line=dict(color='green')), row=2, col=1)
        fig.add_trace(go.Scatter(x=daily['date'], y=daily['negative_ratio'],
                                 mode='lines', name='Negative %', line=dict(color='red')), row=2, col=1)
        fig.add_trace(go.Bar(x=daily['date'], y=daily['headline_count'],
                             name='Count'), row=3, col=1)

        fig.update_layout(height=700)
        st.plotly_chart(fig, use_container_width=True)

    if company is not None:
        st.subheader("Company Sentiment Analysis")

        fig = px.bar(
            company.head(15),
            x='company_mentioned',
            y='avg_sentiment',
            color='avg_sentiment',
            color_continuous_scale='RdYlGn',
            title='Average Sentiment by Company'
        )
        st.plotly_chart(fig, use_container_width=True)

        fig = px.scatter(
            company,
            x='mention_count',
            y='avg_sentiment',
            text='company_mentioned',
            size='mention_count',
            title='Mentions vs Sentiment'
        )
        st.plotly_chart(fig, use_container_width=True)


def show_spark_word_freq():
    """Show Spark word frequency analysis"""
    words = load_spark_output("word_frequencies.csv")

    if words is None:
        st.warning("Word frequency data not found.")
        return

    st.subheader("Word Frequency Analysis (Spark NLP)")

    # Top words
    fig = px.bar(
        words.head(30),
        x='count',
        y='word',
        orientation='h',
        title='Top 30 Words in Headlines'
    )
    fig.update_layout(height=600, yaxis={'categoryorder': 'total ascending'})
    st.plotly_chart(fig, use_container_width=True)

    # Word frequencies by sentiment
    col1, col2 = st.columns(2)

    with col1:
        positive_words = load_spark_output("word_frequencies_positive.csv")
        if positive_words is not None:
            fig = px.bar(
                positive_words.head(20),
                x='count',
                y='word',
                orientation='h',
                title='Top Words in POSITIVE Headlines',
                color_discrete_sequence=['green']
            )
            fig.update_layout(yaxis={'categoryorder': 'total ascending'})
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        negative_words = load_spark_output("word_frequencies_negative.csv")
        if negative_words is not None:
            fig = px.bar(
                negative_words.head(20),
                x='count',
                y='word',
                orientation='h',
                title='Top Words in NEGATIVE Headlines',
                color_discrete_sequence=['red']
            )
            fig.update_layout(yaxis={'categoryorder': 'total ascending'})
            st.plotly_chart(fig, use_container_width=True)


def show_spark_gdp():
    """Show Spark GDP analysis"""
    rankings = load_spark_output("gdp_rankings.csv")
    trends = load_spark_output("gdp_trends.csv")

    if rankings is not None:
        st.subheader("GDP Rankings (Spark Processed)")

        fig = px.bar(
            rankings.head(20),
            x='country_name',
            y='value',
            title='Top 20 Economies by GDP',
            color='gdp_growth',
            color_continuous_scale='RdYlGn',
            labels={'value': 'GDP (USD)', 'gdp_growth': 'Growth %'}
        )
        st.plotly_chart(fig, use_container_width=True)

        st.dataframe(rankings.head(30), use_container_width=True)

    if trends is not None:
        st.subheader("GDP Growth Trends")

        # Select top countries
        top_countries = rankings['country_code'].head(10).tolist() if rankings is not None else []

        if top_countries:
            filtered = trends[trends['country_code'].isin(top_countries)]

            fig = px.line(
                filtered,
                x='year',
                y='gdp_growth',
                color='country_name',
                title='GDP Growth Rate Over Time'
            )
            st.plotly_chart(fig, use_container_width=True)


def show_spark_economic():
    """Show Spark economic trends"""
    trends = load_spark_output("economic_trends.csv")

    if trends is None:
        st.warning("Economic trends data not found.")
        return

    st.subheader("Economic Trends (Spark Processed)")

    indicators = trends['series_id'].unique()
    selected = st.multiselect("Select Indicators", indicators, default=list(indicators[:4]))

    if selected:
        for ind in selected:
            data = trends[trends['series_id'] == ind]
            name = data['series_name'].iloc[0] if 'series_name' in data.columns else ind

            fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                                subplot_titles=[f'{name} - Value', 'Year-over-Year Change (%)'])

            fig.add_trace(go.Scatter(x=data['year'], y=data['value'],
                                     mode='lines+markers', name='Value'), row=1, col=1)
            fig.add_trace(go.Bar(x=data['year'], y=data['yoy_change'],
                                 name='YoY Change'), row=2, col=1)

            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)


def show_spark_report():
    """Show Spark analytics report"""
    report = load_spark_output("analytics_report.json")

    if report is None:
        st.warning("Analytics report not found.")
        return

    st.subheader("Comprehensive Analytics Report")

    st.json(report)


def show_cross_analysis():
    """Show cross-source analysis"""
    st.header("🔗 Cross-Source Analysis")

    combined = load_spark_output("stock_sentiment_combined.csv")

    if combined is not None:
        st.subheader("Stock Returns vs News Sentiment")

        combined['date'] = pd.to_datetime(combined['date'])

        # Scatter plot
        fig = px.scatter(
            combined,
            x='avg_sentiment',
            y='spy_return',
            trendline='ols',
            title='SPY Daily Returns vs Average News Sentiment',
            labels={'avg_sentiment': 'Average Sentiment Score',
                    'spy_return': 'SPY Daily Return'}
        )
        st.plotly_chart(fig, use_container_width=True)

        # Correlation
        correlation = combined['spy_return'].corr(combined['avg_sentiment'])
        st.metric("Correlation Coefficient", f"{correlation:.4f}")

        # Time series comparison
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                            subplot_titles=['SPY Returns', 'Sentiment'])

        fig.add_trace(go.Scatter(x=combined['date'], y=combined['spy_return'],
                                 mode='lines', name='SPY Return'), row=1, col=1)
        fig.add_trace(go.Scatter(x=combined['date'], y=combined['avg_sentiment'],
                                 mode='lines', name='Sentiment'), row=2, col=1)

        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)

    else:
        st.warning("Cross-source analysis data not found. Run batch_spark_analytics.py first.")

    # Additional cross-source insights
    st.subheader("Data Pipeline Summary")

    col1, col2 = st.columns(2)

    with col1:
        st.info("""
        **Stage 1: Data Collection**
        - World Bank API (50+ years GDP data)
        - Stock prices via yfinance
        - Generated news headlines with sentiment

        **Stage 2: Spark Processing**
        - Technical indicators (SMA, RSI, Bollinger)
        - Correlation analysis
        - NLP word frequency analysis
        """)

    with col2:
        st.info("""
        **Stage 3: Storage**
        - QuestDB: Time-series data
        - MongoDB: Documents & analytics results
        - Cassandra: Wide-column analytics
        - Neo4j: Graph relationships

        **Stage 4: Visualization**
        - Streamlit interactive dashboard
        - Plotly charts
        """)


if __name__ == "__main__":
    main()
