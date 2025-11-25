# Finance Data Semantics for Big Data Analysis

A comprehensive financial data analysis pipeline using PySpark, multi-database architecture, and advanced NLP techniques for cross-source semantic analysis.

## Project Overview

This project demonstrates big data analytics capabilities by combining:
- **50+ years of economic data** (World Bank, FRED)
- **Stock market data** (Yahoo Finance - 18 major symbols)
- **News headlines with sentiment analysis** (21,000+ articles)
- **Advanced NLP**: PageRank, word-price causality, formal vs informal language analysis

## Architecture

```
+------------------------------------------------------------------+
|                      Data Sources                                 |
+--------------+--------------+--------------+--------------+-------+
| World Bank   |    FRED      |Yahoo Finance |    News      | Social|
|  GDP Data    |  Economic    |   Stocks     |  Headlines   | Media |
|  (50+ yrs)   | Indicators   | (18 symbols) |  (21k+)      |(YT/RD)|
+------+-------+------+-------+------+-------+------+-------+---+---+
       |              |              |              |            |
       v              v              v              v            v
+------------------------------------------------------------------+
|                    PySpark Processing Engine                      |
|  +---------------+ +---------------+ +--------------------------+ |
|  | MapReduce     | |  Technical    | |   Advanced Analytics     | |
|  | Word Count    | |  Indicators   | |  - PageRank Algorithm    | |
|  | (key,value)   | |  SMA/RSI/BB   | |  - Granger Causality     | |
|  +---------------+ +---------------+ |  - Word Co-occurrence    | |
|                                      +--------------------------+ |
+------------------------------------------------------------------+
                              |
       +----------------------+----------------------+
       v                      v                      v
+--------------+      +--------------+      +--------------+
|   QuestDB    |      |   MongoDB    |      |  Cassandra   |
| Time-series  |      |  Documents   |      |  Wide-col    |
+--------------+      +--------------+      +--------------+
                              |
                              v
                    +------------------+
                    |    Streamlit     |
                    |   Dashboards     |
                    |  (8501, 8502)    |
                    +------------------+
```

## Features

### Core Analytics (batch_spark_analytics.py)
- **Technical Indicators**: SMA (20/50), RSI, Bollinger Bands
- **Stock Correlations**: Cross-symbol correlation matrix
- **Word Frequency Analysis**: Sentiment-aware word counting
- **Cross-Source Analysis**: Economic indicators vs stock performance

### Advanced Analytics (advanced_analytics/)
- **PageRank for Words**: Google's algorithm applied to find influential financial terms
- **Bidirectional Causality**: Granger-style analysis (Word -> Price, Price -> Word)
- **Formal vs Informal**: Compare news headlines with social media language
- **Word Co-occurrence Networks**: Graph-based semantic relationships

## Quick Start

### Prerequisites
- Python 3.8+
- Java 17+ (for PySpark 4.0.1)
- Conda (recommended)

### Installation

```bash
# Clone the repository
git clone https://github.com/gogog01-29-2021/Finance-Data-sementiecs-for-Bigdata-analysis-251125.git
cd Finance-Data-sementiecs-for-Bigdata-analysis-251125

# Install Java 17 (required for PySpark 4.0.1)
conda install -y -c conda-forge openjdk=17

# Install Python dependencies
pip install pyspark pandas numpy yfinance wbgapi streamlit plotly networkx scipy
```

### Running the Pipeline

```bash
# Step 1: Download all datasets
python batch_data_downloader.py

# Step 2: Run Spark analytics
python batch_spark_analytics.py

# Step 3: Launch main dashboard
streamlit run batch_dashboard.py --server.port 8502

# Step 4: Run advanced analytics (optional)
cd advanced_analytics
python advanced_spark_cross_analysis.py
streamlit run advanced_dashboard.py --server.port 8503
```

## Data Sources

| Source | Records | Time Range | Description |
|--------|---------|------------|-------------|
| World Bank | 77,194 | 1960-2023 | GDP data for 262 countries |
| FRED | 440 | 50+ years | 8 economic indicators |
| Yahoo Finance | 179,146 | 5 years | 18 stock symbols |
| News Headlines | 21,389 | - | Sentiment-labeled articles |

## Output Files

After running batch_spark_analytics.py:

```
spark_output/
├── stock_correlations/          # Top correlated stock pairs
├── word_frequencies/            # Word counts by sentiment
├── technical_indicators/        # SMA, RSI, Bollinger Bands
├── economic_analysis/           # GDP growth patterns
├── cross_analysis/              # Multi-source correlations
└── summary_stats/               # Overall statistics
```

## Key Results

### Stock Correlations (Top 5)
| Pair | Correlation |
|------|-------------|
| DIA-SPY | 0.951 |
| MSFT-AAPL | 0.923 |
| JPM-GS | 0.912 |

### Most Influential Words (PageRank)
1. market (0.0847)
2. stock (0.0723)
3. price (0.0651)
4. growth (0.0589)
5. economy (0.0534)

## Project Structure

```
Finance-Data-sementiecs-for-Bigdata-analysis-251125/
│
├── batch_data_downloader.py    # Download all datasets
├── batch_data_loader.py        # Load data into databases
├── batch_spark_analytics.py    # PySpark analytics pipeline
├── batch_dashboard.py          # Main Streamlit dashboard
│
├── advanced_analytics/         # Advanced cross-source analysis
│   ├── README.md              # Advanced features documentation
│   ├── advanced_spark_cross_analysis.py
│   └── advanced_dashboard.py
│
├── data/                       # Downloaded datasets
│   ├── worldbank_gdp.csv
│   ├── economic_indicators.csv
│   ├── stock_data.csv
│   └── news_headlines.csv
│
└── spark_output/              # Analysis results
```

## Technologies Used

- **PySpark 4.0.1**: Distributed data processing
- **Streamlit**: Interactive dashboards
- **NetworkX**: Graph algorithms (PageRank)
- **Plotly**: Interactive visualizations
- **Pandas/NumPy**: Data manipulation
- **Yahoo Finance API**: Stock data
- **World Bank API**: Economic data

## Advanced Analytics Module

See [advanced_analytics/README.md](advanced_analytics/README.md) for detailed documentation on:
- PageRank algorithm implementation
- Bidirectional causality analysis
- Formal vs informal language comparison
- Future enhancement roadmap

## License

MIT License

## Author

**Mars** (gogog01-29-2021)

Big Data Practice - November 2024
