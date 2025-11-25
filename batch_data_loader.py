#!/usr/bin/env python3
"""
BATCH DATA LOADER
Loads downloaded batch data into the multi-database pipeline:
- QuestDB: Time-series data (stocks, economic indicators)
- MongoDB: Documents (news headlines, analysis results)
- Cassandra: Wide-column analytics data
- Neo4j: Graph relationships (companies, sectors, correlations)
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
import json
import time
from dotenv import load_dotenv

load_dotenv()

# Data directories
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data" / "batch"

# Database configurations
QUESTDB_HOST = os.getenv("QUESTDB_HOST", "localhost")
QUESTDB_HTTP_PORT = os.getenv("QUESTDB_HTTP_PORT", "9000")
QUESTDB_PG_PORT = os.getenv("QUESTDB_PORT", "8812")

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "financial_analytics")

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "financial_data")

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")


class QuestDBLoader:
    """Load time-series data into QuestDB"""

    def __init__(self):
        self.base_url = f"http://{QUESTDB_HOST}:{QUESTDB_HTTP_PORT}"

    def execute_query(self, query):
        """Execute a query via HTTP"""
        import requests
        try:
            response = requests.get(
                f"{self.base_url}/exec",
                params={"query": query},
                timeout=60
            )
            return response.status_code == 200
        except Exception as e:
            print(f"  QuestDB query error: {e}")
            return False

    def create_tables(self):
        """Create QuestDB tables for batch data"""
        print("\n[QuestDB] Creating tables...")

        # Stock prices table
        self.execute_query("""
            CREATE TABLE IF NOT EXISTS stock_prices (
                timestamp TIMESTAMP,
                symbol SYMBOL,
                company_name STRING,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume LONG,
                daily_return DOUBLE
            ) timestamp(timestamp) PARTITION BY MONTH
        """)

        # Economic indicators table
        self.execute_query("""
            CREATE TABLE IF NOT EXISTS economic_indicators (
                timestamp TIMESTAMP,
                series_id SYMBOL,
                series_name STRING,
                value DOUBLE,
                year INT
            ) timestamp(timestamp) PARTITION BY YEAR
        """)

        # World Bank data table
        self.execute_query("""
            CREATE TABLE IF NOT EXISTS world_bank_data (
                timestamp TIMESTAMP,
                country_code SYMBOL,
                country_name STRING,
                indicator_code SYMBOL,
                indicator_name STRING,
                year INT,
                value DOUBLE
            ) timestamp(timestamp) PARTITION BY YEAR
        """)

        # News sentiment time-series
        self.execute_query("""
            CREATE TABLE IF NOT EXISTS news_sentiment_ts (
                timestamp TIMESTAMP,
                avg_sentiment DOUBLE,
                sentiment_std DOUBLE,
                headline_count INT,
                positive_count INT,
                negative_count INT,
                neutral_count INT
            ) timestamp(timestamp) PARTITION BY MONTH
        """)

        print("  ✓ Tables created")

    def load_stock_data(self):
        """Load stock price data"""
        stock_file = DATA_DIR / "stocks" / "stock_prices.csv"
        if not stock_file.exists():
            print("  ✗ Stock data file not found")
            return

        print("\n[QuestDB] Loading stock data...")

        df = pd.read_csv(stock_file)
        df['date'] = pd.to_datetime(df['date'])

        # Calculate daily returns
        df = df.sort_values(['symbol', 'date'])
        df['daily_return'] = df.groupby('symbol')['close'].pct_change()
        df['daily_return'] = df['daily_return'].fillna(0)

        # Use ILP for fast ingestion
        try:
            from questdb.ingress import Sender, IngressError

            conf = f"http::addr={QUESTDB_HOST}:{QUESTDB_HTTP_PORT};"

            with Sender.from_conf(conf) as sender:
                batch_size = 10000
                total = len(df)

                for i in range(0, total, batch_size):
                    batch = df.iloc[i:i+batch_size]

                    for _, row in batch.iterrows():
                        sender.row(
                            'stock_prices',
                            symbols={
                                'symbol': str(row['symbol']),
                            },
                            columns={
                                'company_name': str(row.get('company_name', '')),
                                'open': float(row['open']),
                                'high': float(row['high']),
                                'low': float(row['low']),
                                'close': float(row['close']),
                                'volume': int(row['volume']),
                                'daily_return': float(row['daily_return'])
                            },
                            at=row['date']
                        )

                    sender.flush()
                    print(f"  Loaded {min(i+batch_size, total)}/{total} stock records")

            print(f"  ✓ Loaded {total} stock records")

        except ImportError:
            print("  questdb package not available, using HTTP API...")
            self._load_via_http(df, 'stock_prices')

    def load_economic_data(self):
        """Load economic indicator data"""
        econ_file = DATA_DIR / "fred" / "economic_indicators.csv"
        if not econ_file.exists():
            print("  ✗ Economic data file not found")
            return

        print("\n[QuestDB] Loading economic indicators...")

        df = pd.read_csv(econ_file)
        df['date'] = pd.to_datetime(df['date'])

        try:
            from questdb.ingress import Sender

            conf = f"http::addr={QUESTDB_HOST}:{QUESTDB_HTTP_PORT};"

            with Sender.from_conf(conf) as sender:
                for _, row in df.iterrows():
                    sender.row(
                        'economic_indicators',
                        symbols={
                            'series_id': str(row['series_id']),
                        },
                        columns={
                            'series_name': str(row['series_name']),
                            'value': float(row['value']),
                            'year': int(row['year'])
                        },
                        at=row['date']
                    )

                sender.flush()

            print(f"  ✓ Loaded {len(df)} economic indicator records")

        except ImportError:
            print("  Using HTTP API fallback...")

    def load_worldbank_data(self):
        """Load World Bank data"""
        wb_file = DATA_DIR / "worldbank" / "world_bank_indicators.csv"
        if not wb_file.exists():
            print("  ✗ World Bank data file not found")
            return

        print("\n[QuestDB] Loading World Bank data...")

        df = pd.read_csv(wb_file)
        # Create timestamp from year
        df['date'] = pd.to_datetime(df['year'].astype(str) + '-01-01')

        try:
            from questdb.ingress import Sender

            conf = f"http::addr={QUESTDB_HOST}:{QUESTDB_HTTP_PORT};"

            with Sender.from_conf(conf) as sender:
                batch_size = 5000
                total = len(df)

                for i in range(0, total, batch_size):
                    batch = df.iloc[i:i+batch_size]

                    for _, row in batch.iterrows():
                        sender.row(
                            'world_bank_data',
                            symbols={
                                'country_code': str(row['country_code']),
                                'indicator_code': str(row['indicator_code']),
                            },
                            columns={
                                'country_name': str(row['country_name']),
                                'indicator_name': str(row['indicator_name']),
                                'year': int(row['year']),
                                'value': float(row['value'])
                            },
                            at=row['date']
                        )

                    sender.flush()
                    print(f"  Loaded {min(i+batch_size, total)}/{total} World Bank records")

            print(f"  ✓ Loaded {total} World Bank records")

        except ImportError:
            print("  Using HTTP API fallback...")

    def _load_via_http(self, df, table_name):
        """Fallback: load data via HTTP API"""
        import requests
        # This is slower but works without questdb package
        print(f"  HTTP loading not implemented for {table_name}")


class MongoDBLoader:
    """Load document data into MongoDB"""

    def __init__(self):
        try:
            from pymongo import MongoClient
            self.client = MongoClient(MONGODB_URI)
            self.db = self.client[MONGODB_DATABASE]
            self.available = True
        except Exception as e:
            print(f"  MongoDB connection error: {e}")
            self.available = False

    def load_news_headlines(self):
        """Load news headlines into MongoDB"""
        if not self.available:
            return

        news_file = DATA_DIR / "news" / "financial_news_headlines.csv"
        if not news_file.exists():
            print("  ✗ News headlines file not found")
            return

        print("\n[MongoDB] Loading news headlines...")

        df = pd.read_csv(news_file)

        # Convert to documents
        collection = self.db['news_headlines']

        # Clear existing data
        collection.delete_many({})

        # Insert in batches
        batch_size = 5000
        total = len(df)

        for i in range(0, total, batch_size):
            batch = df.iloc[i:i+batch_size]
            documents = batch.to_dict('records')

            # Convert dates
            for doc in documents:
                doc['date'] = pd.to_datetime(doc['date'])
                doc['timestamp'] = pd.to_datetime(doc['timestamp'])

            collection.insert_many(documents)
            print(f"  Loaded {min(i+batch_size, total)}/{total} headlines")

        # Create indexes
        collection.create_index('date')
        collection.create_index('company_mentioned')
        collection.create_index('sentiment')
        collection.create_index([('sentiment_score', -1)])

        print(f"  ✓ Loaded {total} news headlines")

    def load_company_sentiment(self):
        """Load company sentiment analysis"""
        if not self.available:
            return

        sentiment_file = DATA_DIR / "news" / "company_sentiment_monthly.csv"
        if not sentiment_file.exists():
            return

        print("\n[MongoDB] Loading company sentiment analysis...")

        df = pd.read_csv(sentiment_file)
        collection = self.db['company_sentiment']
        collection.delete_many({})

        documents = df.to_dict('records')
        collection.insert_many(documents)

        collection.create_index('company')
        collection.create_index('month')

        print(f"  ✓ Loaded {len(documents)} company sentiment records")

    def load_analysis_results(self):
        """Create analysis summary documents"""
        if not self.available:
            return

        print("\n[MongoDB] Creating analysis summaries...")

        # Load stock data for summary
        stock_file = DATA_DIR / "stocks" / "stock_prices.csv"
        if stock_file.exists():
            df = pd.read_csv(stock_file)
            df['date'] = pd.to_datetime(df['date'])

            # Calculate summary stats per company
            summaries = []
            for symbol in df['symbol'].unique():
                stock_df = df[df['symbol'] == symbol]

                summary = {
                    'symbol': symbol,
                    'company_name': stock_df['company_name'].iloc[0],
                    'data_start': stock_df['date'].min().isoformat(),
                    'data_end': stock_df['date'].max().isoformat(),
                    'total_records': len(stock_df),
                    'price_stats': {
                        'min': float(stock_df['close'].min()),
                        'max': float(stock_df['close'].max()),
                        'mean': float(stock_df['close'].mean()),
                        'std': float(stock_df['close'].std()),
                        'latest': float(stock_df['close'].iloc[-1])
                    },
                    'volume_stats': {
                        'min': int(stock_df['volume'].min()),
                        'max': int(stock_df['volume'].max()),
                        'mean': float(stock_df['volume'].mean())
                    },
                    'analysis_timestamp': datetime.now().isoformat()
                }
                summaries.append(summary)

            collection = self.db['stock_summaries']
            collection.delete_many({})
            collection.insert_many(summaries)
            collection.create_index('symbol')

            print(f"  ✓ Created {len(summaries)} stock summary documents")


class CassandraLoader:
    """Load analytics data into Cassandra"""

    def __init__(self):
        self.available = False
        try:
            from cassandra.cluster import Cluster

            cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
            self.session = cluster.connect()
            self.available = True

            # Create keyspace if not exists
            self.session.execute(f"""
                CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
                WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
            """)
            self.session.set_keyspace(CASSANDRA_KEYSPACE)

        except Exception as e:
            print(f"  Cassandra connection error: {e}")

    def create_tables(self):
        """Create Cassandra tables"""
        if not self.available:
            return

        print("\n[Cassandra] Creating tables...")

        # Stock analytics by symbol and date
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS stock_analytics (
                symbol text,
                date date,
                open double,
                high double,
                low double,
                close double,
                volume bigint,
                daily_return double,
                volatility_20d double,
                sma_20 double,
                sma_50 double,
                rsi_14 double,
                PRIMARY KEY (symbol, date)
            ) WITH CLUSTERING ORDER BY (date DESC)
        """)

        # Economic indicators by series
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS economic_analytics (
                series_id text,
                year int,
                value double,
                yoy_change double,
                PRIMARY KEY (series_id, year)
            ) WITH CLUSTERING ORDER BY (year DESC)
        """)

        # Daily sentiment aggregates
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS daily_sentiment (
                date date,
                avg_sentiment double,
                positive_pct double,
                negative_pct double,
                neutral_pct double,
                headline_count int,
                PRIMARY KEY (date)
            )
        """)

        # Cross-asset correlations
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS asset_correlations (
                asset1 text,
                asset2 text,
                period text,
                correlation double,
                calculated_at timestamp,
                PRIMARY KEY ((asset1, asset2), period)
            )
        """)

        print("  ✓ Tables created")

    def load_stock_analytics(self):
        """Load stock analytics data"""
        if not self.available:
            return

        stock_file = DATA_DIR / "stocks" / "stock_prices.csv"
        if not stock_file.exists():
            return

        print("\n[Cassandra] Loading stock analytics...")

        df = pd.read_csv(stock_file)
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values(['symbol', 'date'])

        # Calculate technical indicators
        for symbol in df['symbol'].unique():
            mask = df['symbol'] == symbol
            df.loc[mask, 'daily_return'] = df.loc[mask, 'close'].pct_change()
            df.loc[mask, 'volatility_20d'] = df.loc[mask, 'daily_return'].rolling(20).std()
            df.loc[mask, 'sma_20'] = df.loc[mask, 'close'].rolling(20).mean()
            df.loc[mask, 'sma_50'] = df.loc[mask, 'close'].rolling(50).mean()

            # Simple RSI calculation
            delta = df.loc[mask, 'close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
            rs = gain / loss
            df.loc[mask, 'rsi_14'] = 100 - (100 / (1 + rs))

        df = df.fillna(0)

        # Prepare insert statement
        insert_stmt = self.session.prepare("""
            INSERT INTO stock_analytics
            (symbol, date, open, high, low, close, volume, daily_return,
             volatility_20d, sma_20, sma_50, rsi_14)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)

        # Insert data
        count = 0
        for _, row in df.iterrows():
            self.session.execute(insert_stmt, [
                row['symbol'],
                row['date'].date(),
                float(row['open']),
                float(row['high']),
                float(row['low']),
                float(row['close']),
                int(row['volume']),
                float(row['daily_return']),
                float(row['volatility_20d']),
                float(row['sma_20']),
                float(row['sma_50']),
                float(row['rsi_14'])
            ])
            count += 1

            if count % 10000 == 0:
                print(f"  Loaded {count}/{len(df)} records")

        print(f"  ✓ Loaded {count} stock analytics records")

    def load_sentiment_analytics(self):
        """Load sentiment analytics"""
        if not self.available:
            return

        news_file = DATA_DIR / "news" / "financial_news_headlines.csv"
        if not news_file.exists():
            return

        print("\n[Cassandra] Loading sentiment analytics...")

        df = pd.read_csv(news_file)
        df['date'] = pd.to_datetime(df['date']).dt.date

        # Aggregate by date
        daily = df.groupby('date').agg({
            'sentiment_score': 'mean',
            'headline': 'count',
            'sentiment': lambda x: (x == 'positive').sum() / len(x),
        }).reset_index()

        daily.columns = ['date', 'avg_sentiment', 'headline_count', 'positive_pct']

        # Calculate negative and neutral percentages
        sentiment_counts = df.groupby(['date', 'sentiment']).size().unstack(fill_value=0)
        sentiment_counts['total'] = sentiment_counts.sum(axis=1)
        sentiment_counts['negative_pct'] = sentiment_counts.get('negative', 0) / sentiment_counts['total']
        sentiment_counts['neutral_pct'] = sentiment_counts.get('neutral', 0) / sentiment_counts['total']

        daily = daily.merge(
            sentiment_counts[['negative_pct', 'neutral_pct']].reset_index(),
            on='date'
        )

        insert_stmt = self.session.prepare("""
            INSERT INTO daily_sentiment
            (date, avg_sentiment, positive_pct, negative_pct, neutral_pct, headline_count)
            VALUES (?, ?, ?, ?, ?, ?)
        """)

        for _, row in daily.iterrows():
            self.session.execute(insert_stmt, [
                row['date'],
                float(row['avg_sentiment']),
                float(row['positive_pct']),
                float(row['negative_pct']),
                float(row['neutral_pct']),
                int(row['headline_count'])
            ])

        print(f"  ✓ Loaded {len(daily)} daily sentiment records")

    def calculate_correlations(self):
        """Calculate and store asset correlations"""
        if not self.available:
            return

        stock_file = DATA_DIR / "stocks" / "stock_prices.csv"
        if not stock_file.exists():
            return

        print("\n[Cassandra] Calculating correlations...")

        df = pd.read_csv(stock_file)
        df['date'] = pd.to_datetime(df['date'])

        # Pivot to get returns by symbol
        returns = df.pivot_table(
            index='date',
            columns='symbol',
            values='close'
        ).pct_change().dropna()

        # Calculate correlations
        corr_matrix = returns.corr()

        insert_stmt = self.session.prepare("""
            INSERT INTO asset_correlations
            (asset1, asset2, period, correlation, calculated_at)
            VALUES (?, ?, ?, ?, ?)
        """)

        now = datetime.now()
        count = 0

        for asset1 in corr_matrix.columns:
            for asset2 in corr_matrix.columns:
                if asset1 < asset2:  # Only store unique pairs
                    self.session.execute(insert_stmt, [
                        asset1,
                        asset2,
                        'full_period',
                        float(corr_matrix.loc[asset1, asset2]),
                        now
                    ])
                    count += 1

        print(f"  ✓ Stored {count} correlation pairs")


class Neo4jLoader:
    """Load graph data into Neo4j"""

    def __init__(self):
        self.available = False
        try:
            from neo4j import GraphDatabase
            self.driver = GraphDatabase.driver(
                NEO4J_URI,
                auth=(NEO4J_USER, NEO4J_PASSWORD)
            )
            self.available = True
        except Exception as e:
            print(f"  Neo4j connection error: {e}")

    def close(self):
        if self.available:
            self.driver.close()

    def create_graph(self):
        """Create the financial knowledge graph"""
        if not self.available:
            return

        print("\n[Neo4j] Creating financial knowledge graph...")

        with self.driver.session() as session:
            # Clear existing data
            session.run("MATCH (n) DETACH DELETE n")

            # Create sectors
            sectors = {
                'Technology': ['AAPL', 'MSFT', 'GOOGL', 'META', 'NVDA', 'TSLA'],
                'E-Commerce': ['AMZN'],
                'Financial': ['JPM', 'BAC', 'GS'],
                'Healthcare': ['JNJ'],
                'Energy': ['XOM'],
                'Consumer': ['WMT', 'PG', 'KO'],
                'ETF': ['SPY', 'QQQ', 'DIA']
            }

            # Create sector nodes
            for sector in sectors:
                session.run(
                    "CREATE (s:Sector {name: $name})",
                    name=sector
                )

            print("  ✓ Created sector nodes")

            # Create company nodes and relationships
            stock_file = DATA_DIR / "stocks" / "stock_prices.csv"
            if stock_file.exists():
                df = pd.read_csv(stock_file)

                for symbol in df['symbol'].unique():
                    company_name = df[df['symbol'] == symbol]['company_name'].iloc[0]

                    # Find sector
                    company_sector = 'Other'
                    for sector, symbols in sectors.items():
                        if symbol in symbols:
                            company_sector = sector
                            break

                    # Create company node
                    session.run("""
                        CREATE (c:Company {
                            symbol: $symbol,
                            name: $name,
                            sector: $sector
                        })
                    """, symbol=symbol, name=company_name, sector=company_sector)

                    # Link to sector
                    session.run("""
                        MATCH (c:Company {symbol: $symbol})
                        MATCH (s:Sector {name: $sector})
                        CREATE (c)-[:BELONGS_TO]->(s)
                    """, symbol=symbol, sector=company_sector)

                print(f"  ✓ Created {df['symbol'].nunique()} company nodes")

            # Create economic indicator nodes
            econ_file = DATA_DIR / "fred" / "economic_indicators.csv"
            if econ_file.exists():
                df = pd.read_csv(econ_file)

                for series_id in df['series_id'].unique():
                    series_name = df[df['series_id'] == series_id]['series_name'].iloc[0]

                    session.run("""
                        CREATE (i:EconomicIndicator {
                            id: $id,
                            name: $name
                        })
                    """, id=series_id, name=series_name)

                print(f"  ✓ Created {df['series_id'].nunique()} economic indicator nodes")

            # Create country nodes from World Bank data
            wb_file = DATA_DIR / "worldbank" / "world_bank_indicators.csv"
            if wb_file.exists():
                df = pd.read_csv(wb_file)

                for country_code in df['country_code'].unique()[:50]:  # Top 50 countries
                    country_name = df[df['country_code'] == country_code]['country_name'].iloc[0]

                    session.run("""
                        CREATE (c:Country {
                            code: $code,
                            name: $name
                        })
                    """, code=country_code, name=country_name)

                print(f"  ✓ Created {min(50, df['country_code'].nunique())} country nodes")

            # Create correlation relationships between stocks
            stock_file = DATA_DIR / "stocks" / "stock_prices.csv"
            if stock_file.exists():
                df = pd.read_csv(stock_file)
                df['date'] = pd.to_datetime(df['date'])

                # Calculate correlations
                returns = df.pivot_table(
                    index='date',
                    columns='symbol',
                    values='close'
                ).pct_change().dropna()

                corr_matrix = returns.corr()

                # Create CORRELATED_WITH relationships for strong correlations
                for i, asset1 in enumerate(corr_matrix.columns):
                    for asset2 in corr_matrix.columns[i+1:]:
                        corr = corr_matrix.loc[asset1, asset2]
                        if abs(corr) > 0.5:  # Only strong correlations
                            session.run("""
                                MATCH (c1:Company {symbol: $symbol1})
                                MATCH (c2:Company {symbol: $symbol2})
                                CREATE (c1)-[:CORRELATED_WITH {
                                    correlation: $corr,
                                    strength: $strength
                                }]->(c2)
                            """, symbol1=asset1, symbol2=asset2,
                                corr=round(corr, 3),
                                strength='strong' if abs(corr) > 0.7 else 'moderate')

                print("  ✓ Created correlation relationships")

            # Create news mention relationships
            news_file = DATA_DIR / "news" / "financial_news_headlines.csv"
            if news_file.exists():
                df = pd.read_csv(news_file)

                # Count mentions per company
                mentions = df[df['company_mentioned'].notna()].groupby('company_mentioned').agg({
                    'headline': 'count',
                    'sentiment_score': 'mean'
                }).reset_index()
                mentions.columns = ['company', 'mention_count', 'avg_sentiment']

                # Map company names to symbols
                company_to_symbol = {
                    'Apple': 'AAPL', 'Microsoft': 'MSFT', 'Google': 'GOOGL',
                    'Amazon': 'AMZN', 'Tesla': 'TSLA', 'Meta': 'META',
                    'NVIDIA': 'NVDA', 'JPMorgan': 'JPM', 'Goldman Sachs': 'GS',
                    'Bank of America': 'BAC', 'Johnson & Johnson': 'JNJ',
                    'Walmart': 'WMT', 'Exxon': 'XOM', 'Coca-Cola': 'KO'
                }

                for _, row in mentions.iterrows():
                    symbol = company_to_symbol.get(row['company'])
                    if symbol:
                        session.run("""
                            MATCH (c:Company {symbol: $symbol})
                            SET c.news_mentions = $mentions,
                                c.avg_news_sentiment = $sentiment
                        """, symbol=symbol,
                            mentions=int(row['mention_count']),
                            sentiment=round(row['avg_sentiment'], 3))

                print("  ✓ Added news mention statistics")

            # Create indexes
            session.run("CREATE INDEX IF NOT EXISTS FOR (c:Company) ON (c.symbol)")
            session.run("CREATE INDEX IF NOT EXISTS FOR (s:Sector) ON (s.name)")
            session.run("CREATE INDEX IF NOT EXISTS FOR (c:Country) ON (c.code)")

            print("  ✓ Created indexes")


def main():
    """Load all batch data into databases"""
    print("="*60)
    print("BATCH DATA LOADER")
    print("="*60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"\nData source: {DATA_DIR}")
    print(f"\nTarget databases:")
    print(f"  QuestDB: {QUESTDB_HOST}:{QUESTDB_HTTP_PORT}")
    print(f"  MongoDB: {MONGODB_URI}")
    print(f"  Cassandra: {CASSANDRA_HOST}:{CASSANDRA_PORT}")
    print(f"  Neo4j: {NEO4J_URI}")

    # Check if data exists
    if not DATA_DIR.exists() or not any(DATA_DIR.rglob("*.csv")):
        print("\n✗ No data files found!")
        print("  Run batch_data_downloader.py first to download the data.")
        return

    # Load into QuestDB
    print("\n" + "-"*40)
    print("Loading into QuestDB...")
    print("-"*40)
    questdb = QuestDBLoader()
    questdb.create_tables()
    questdb.load_stock_data()
    questdb.load_economic_data()
    questdb.load_worldbank_data()

    # Load into MongoDB
    print("\n" + "-"*40)
    print("Loading into MongoDB...")
    print("-"*40)
    mongo = MongoDBLoader()
    mongo.load_news_headlines()
    mongo.load_company_sentiment()
    mongo.load_analysis_results()

    # Load into Cassandra
    print("\n" + "-"*40)
    print("Loading into Cassandra...")
    print("-"*40)
    cassandra = CassandraLoader()
    cassandra.create_tables()
    cassandra.load_stock_analytics()
    cassandra.load_sentiment_analytics()
    cassandra.calculate_correlations()

    # Load into Neo4j
    print("\n" + "-"*40)
    print("Loading into Neo4j...")
    print("-"*40)
    neo4j = Neo4jLoader()
    neo4j.create_graph()
    neo4j.close()

    print("\n" + "="*60)
    print("DATA LOADING COMPLETE!")
    print("="*60)
    print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
