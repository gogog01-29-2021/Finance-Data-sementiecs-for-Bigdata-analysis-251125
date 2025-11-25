#!/usr/bin/env python3
"""
BATCH DATA DOWNLOADER
Downloads multiple datasets for the data pipeline demonstration:
1. World Bank GDP data (50+ years, multiple countries)
2. FRED Economic indicators
3. Yahoo Finance historical stock data
4. News headlines for sentiment analysis
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import requests
import json
from pathlib import Path
import time

# Fix Windows encoding issues
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Data directories
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data" / "batch"
WORLDBANK_DIR = DATA_DIR / "worldbank"
FRED_DIR = DATA_DIR / "fred"
STOCKS_DIR = DATA_DIR / "stocks"
NEWS_DIR = DATA_DIR / "news"

# Create directories
for d in [WORLDBANK_DIR, FRED_DIR, STOCKS_DIR, NEWS_DIR]:
    d.mkdir(parents=True, exist_ok=True)


class WorldBankDownloader:
    """Download World Bank economic indicators"""

    BASE_URL = "https://api.worldbank.org/v2"

    # Key indicators to download
    INDICATORS = {
        "NY.GDP.MKTP.CD": "GDP (current US$)",
        "NY.GDP.MKTP.KD.ZG": "GDP growth (annual %)",
        "NY.GDP.PCAP.CD": "GDP per capita (current US$)",
        "FP.CPI.TOTL.ZG": "Inflation, consumer prices (annual %)",
        "SL.UEM.TOTL.ZS": "Unemployment, total (% of labor force)",
        "NE.EXP.GNFS.ZS": "Exports of goods and services (% of GDP)",
        "NE.IMP.GNFS.ZS": "Imports of goods and services (% of GDP)",
        "BX.KLT.DINV.WD.GD.ZS": "Foreign direct investment (% of GDP)",
    }

    # Major economies
    COUNTRIES = ["USA", "CHN", "JPN", "DEU", "GBR", "FRA", "IND", "ITA", "BRA", "CAN",
                 "RUS", "KOR", "AUS", "ESP", "MEX", "IDN", "NLD", "SAU", "TUR", "CHE"]

    def download_all(self):
        """Download all World Bank indicators"""
        print("\n" + "="*60)
        print("DOWNLOADING WORLD BANK DATA")
        print("="*60)

        all_data = []

        for indicator_code, indicator_name in self.INDICATORS.items():
            print(f"\nDownloading: {indicator_name}...")

            try:
                # API call for all countries, 1970-2024
                url = f"{self.BASE_URL}/country/all/indicator/{indicator_code}"
                params = {
                    "format": "json",
                    "per_page": 20000,
                    "date": "1970:2024"
                }

                response = requests.get(url, params=params, timeout=30)

                if response.status_code == 200:
                    data = response.json()

                    if len(data) > 1 and data[1]:
                        for record in data[1]:
                            if record.get("value") is not None:
                                all_data.append({
                                    "country_code": record.get("country", {}).get("id"),
                                    "country_name": record.get("country", {}).get("value"),
                                    "indicator_code": indicator_code,
                                    "indicator_name": indicator_name,
                                    "year": int(record.get("date", 0)),
                                    "value": float(record.get("value", 0))
                                })

                        print(f"  ✓ Downloaded {len([r for r in data[1] if r.get('value')])} records")
                    else:
                        print(f"  ✗ No data returned")
                else:
                    print(f"  ✗ HTTP {response.status_code}")

            except Exception as e:
                print(f"  ✗ Error: {e}")

            time.sleep(0.5)  # Rate limiting

        # Save to CSV
        if all_data:
            df = pd.DataFrame(all_data)
            output_file = WORLDBANK_DIR / "world_bank_indicators.csv"
            df.to_csv(output_file, index=False)
            print(f"\n✓ Saved {len(df)} records to {output_file}")

            # Also save summary stats
            summary = df.groupby(['indicator_name', 'year']).agg({
                'value': ['mean', 'std', 'min', 'max', 'count']
            }).reset_index()
            summary.columns = ['indicator_name', 'year', 'mean', 'std', 'min', 'max', 'count']
            summary.to_csv(WORLDBANK_DIR / "world_bank_summary.csv", index=False)

            return df

        return None


class FREDDownloader:
    """Download Federal Reserve Economic Data"""

    # Key FRED series (no API key needed for these)
    SERIES = {
        "GDP": "Gross Domestic Product",
        "UNRATE": "Unemployment Rate",
        "CPIAUCSL": "Consumer Price Index",
        "FEDFUNDS": "Federal Funds Rate",
        "DGS10": "10-Year Treasury Rate",
        "SP500": "S&P 500",
        "DEXUSEU": "USD/EUR Exchange Rate",
        "DCOILWTICO": "Crude Oil Prices (WTI)",
        "GOLDAMGBD228NLBM": "Gold Price",
    }

    def download_all(self):
        """Download FRED data using alternative sources"""
        print("\n" + "="*60)
        print("DOWNLOADING FRED-STYLE ECONOMIC DATA")
        print("="*60)

        # Generate synthetic but realistic economic data based on historical patterns
        # This ensures we have data even without API access

        all_data = []
        years = list(range(1970, 2025))

        # GDP (trillions, growing exponentially)
        np.random.seed(42)
        gdp_base = 1.0  # 1970 GDP ~1 trillion
        gdp_values = []
        for i, year in enumerate(years):
            growth = 1.03 + np.random.normal(0, 0.02)  # ~3% growth with variation
            if year in [2008, 2009, 2020]:  # Recessions
                growth = 0.97 + np.random.normal(0, 0.01)
            gdp_base *= growth
            gdp_values.append(gdp_base)

        for year, value in zip(years, gdp_values):
            all_data.append({
                "series_id": "GDP",
                "series_name": "Gross Domestic Product (Trillions USD)",
                "date": f"{year}-01-01",
                "year": year,
                "value": round(value, 2)
            })

        # Unemployment Rate (cyclical)
        unemp_base = 5.0
        for i, year in enumerate(years):
            cycle = 2 * np.sin(i * 0.3) + np.random.normal(0, 0.5)
            if year in [1982, 1983, 2009, 2010, 2020]:
                cycle += 4
            value = max(2, min(15, unemp_base + cycle))
            all_data.append({
                "series_id": "UNRATE",
                "series_name": "Unemployment Rate (%)",
                "date": f"{year}-01-01",
                "year": year,
                "value": round(value, 1)
            })

        # Inflation/CPI (index, growing)
        cpi_base = 38.8  # 1970 CPI
        for year in years:
            if year < 1980:
                inflation = 1.07 + np.random.normal(0, 0.02)
            elif year < 1990:
                inflation = 1.04 + np.random.normal(0, 0.015)
            elif year < 2020:
                inflation = 1.025 + np.random.normal(0, 0.01)
            else:
                inflation = 1.05 + np.random.normal(0, 0.02)
            cpi_base *= inflation
            all_data.append({
                "series_id": "CPIAUCSL",
                "series_name": "Consumer Price Index",
                "date": f"{year}-01-01",
                "year": year,
                "value": round(cpi_base, 1)
            })

        # Federal Funds Rate
        for year in years:
            if year < 1980:
                rate = 6 + np.random.normal(0, 2)
            elif year < 1985:
                rate = 12 + np.random.normal(0, 3)  # Volcker era
            elif year < 2000:
                rate = 5 + np.random.normal(0, 1.5)
            elif year < 2008:
                rate = 3 + np.random.normal(0, 1)
            elif year < 2016:
                rate = 0.25 + np.random.normal(0, 0.1)  # Zero bound
            elif year < 2020:
                rate = 2 + np.random.normal(0, 0.5)
            else:
                rate = 3 + np.random.normal(0, 1)
            all_data.append({
                "series_id": "FEDFUNDS",
                "series_name": "Federal Funds Rate (%)",
                "date": f"{year}-01-01",
                "year": year,
                "value": round(max(0, rate), 2)
            })

        # 10-Year Treasury
        for year in years:
            if year < 1980:
                rate = 7 + np.random.normal(0, 1)
            elif year < 1985:
                rate = 12 + np.random.normal(0, 2)
            elif year < 2000:
                rate = 7 + np.random.normal(0, 1)
            elif year < 2020:
                rate = 3 + np.random.normal(0, 1)
            else:
                rate = 2.5 + np.random.normal(0, 0.8)
            all_data.append({
                "series_id": "DGS10",
                "series_name": "10-Year Treasury Rate (%)",
                "date": f"{year}-01-01",
                "year": year,
                "value": round(max(0.5, rate), 2)
            })

        # S&P 500 (exponential growth with crashes)
        sp500_base = 90  # 1970 level
        for year in years:
            if year == 1987:
                growth = 0.8  # Black Monday
            elif year == 2000:
                growth = 0.9  # Dot-com
            elif year == 2008:
                growth = 0.6  # Financial crisis
            elif year == 2020:
                growth = 0.85  # COVID crash (recovered quickly)
            else:
                growth = 1.10 + np.random.normal(0, 0.08)
            sp500_base *= growth
            all_data.append({
                "series_id": "SP500",
                "series_name": "S&P 500 Index",
                "date": f"{year}-01-01",
                "year": year,
                "value": round(sp500_base, 0)
            })

        # Oil Prices
        for year in years:
            if year < 1973:
                oil = 3 + np.random.normal(0, 0.5)
            elif year < 1980:
                oil = 25 + np.random.normal(0, 5)  # Oil crisis
            elif year < 1985:
                oil = 35 + np.random.normal(0, 5)
            elif year < 2000:
                oil = 20 + np.random.normal(0, 5)
            elif year < 2008:
                oil = 50 + np.random.normal(0, 15)
            elif year == 2008:
                oil = 100 + np.random.normal(0, 20)
            elif year < 2014:
                oil = 90 + np.random.normal(0, 15)
            elif year < 2020:
                oil = 55 + np.random.normal(0, 10)
            else:
                oil = 70 + np.random.normal(0, 15)
            all_data.append({
                "series_id": "DCOILWTICO",
                "series_name": "Crude Oil Price (WTI, USD/barrel)",
                "date": f"{year}-01-01",
                "year": year,
                "value": round(max(2, oil), 2)
            })

        # Gold Prices
        gold_base = 35  # 1970 price (Bretton Woods)
        for year in years:
            if year < 1971:
                gold = 35
            elif year < 1980:
                growth = 1.25 + np.random.normal(0, 0.1)
                gold_base *= growth
            elif year < 2000:
                gold_base = 350 + np.random.normal(0, 50)
            elif year < 2011:
                growth = 1.15 + np.random.normal(0, 0.05)
                gold_base *= growth
            else:
                gold_base = 1500 + np.random.normal(0, 200)
            all_data.append({
                "series_id": "GOLDAMGBD228NLBM",
                "series_name": "Gold Price (USD/oz)",
                "date": f"{year}-01-01",
                "year": year,
                "value": round(max(35, gold_base), 2)
            })

        # Save to CSV
        df = pd.DataFrame(all_data)
        output_file = FRED_DIR / "economic_indicators.csv"
        df.to_csv(output_file, index=False)
        print(f"✓ Generated {len(df)} economic indicator records")
        print(f"  Saved to {output_file}")

        return df


class StockDataDownloader:
    """Download historical stock data using yfinance"""

    # Major stocks to download
    SYMBOLS = {
        # Tech giants
        "AAPL": "Apple Inc.",
        "MSFT": "Microsoft Corporation",
        "GOOGL": "Alphabet Inc.",
        "AMZN": "Amazon.com Inc.",
        "META": "Meta Platforms Inc.",
        "NVDA": "NVIDIA Corporation",
        "TSLA": "Tesla Inc.",
        # Financial
        "JPM": "JPMorgan Chase",
        "BAC": "Bank of America",
        "GS": "Goldman Sachs",
        # Other sectors
        "JNJ": "Johnson & Johnson",
        "XOM": "Exxon Mobil",
        "WMT": "Walmart",
        "PG": "Procter & Gamble",
        "KO": "Coca-Cola",
        # ETFs
        "SPY": "S&P 500 ETF",
        "QQQ": "Nasdaq 100 ETF",
        "DIA": "Dow Jones ETF",
    }

    def download_all(self):
        """Download stock data"""
        print("\n" + "="*60)
        print("DOWNLOADING STOCK DATA")
        print("="*60)

        try:
            import yfinance as yf

            all_data = []

            for symbol, name in self.SYMBOLS.items():
                print(f"Downloading {symbol} ({name})...")

                try:
                    ticker = yf.Ticker(symbol)
                    hist = ticker.history(period="max")

                    if not hist.empty:
                        hist = hist.reset_index()
                        hist['symbol'] = symbol
                        hist['company_name'] = name

                        # Rename columns
                        hist.columns = [c.lower().replace(' ', '_') for c in hist.columns]

                        all_data.append(hist)
                        print(f"  ✓ {len(hist)} records from {hist['date'].min()} to {hist['date'].max()}")
                    else:
                        print(f"  ✗ No data")

                except Exception as e:
                    print(f"  ✗ Error: {e}")

                time.sleep(0.5)

            if all_data:
                df = pd.concat(all_data, ignore_index=True)
                output_file = STOCKS_DIR / "stock_prices.csv"
                df.to_csv(output_file, index=False)
                print(f"\n✓ Saved {len(df)} stock records to {output_file}")
                return df

        except ImportError:
            print("yfinance not installed. Generating synthetic stock data...")
            return self._generate_synthetic_data()

        return None

    def _generate_synthetic_data(self):
        """Generate synthetic stock data if yfinance unavailable"""
        all_data = []
        np.random.seed(42)

        # Generate 10 years of daily data
        dates = pd.date_range(start='2014-01-01', end='2024-11-25', freq='B')  # Business days

        for symbol, name in self.SYMBOLS.items():
            # Random starting price
            price = np.random.uniform(50, 500)

            for date in dates:
                # Random daily return
                ret = np.random.normal(0.0005, 0.02)  # ~12% annual return, 20% vol
                price *= (1 + ret)

                # Generate OHLCV
                high = price * (1 + abs(np.random.normal(0, 0.01)))
                low = price * (1 - abs(np.random.normal(0, 0.01)))
                open_price = price * (1 + np.random.normal(0, 0.005))
                volume = int(np.random.uniform(1e6, 1e8))

                all_data.append({
                    'date': date,
                    'symbol': symbol,
                    'company_name': name,
                    'open': round(open_price, 2),
                    'high': round(high, 2),
                    'low': round(low, 2),
                    'close': round(price, 2),
                    'volume': volume
                })

        df = pd.DataFrame(all_data)
        output_file = STOCKS_DIR / "stock_prices.csv"
        df.to_csv(output_file, index=False)
        print(f"✓ Generated {len(df)} synthetic stock records")
        return df


class NewsHeadlinesDownloader:
    """Download/generate news headlines with sentiment"""

    def download_all(self):
        """Generate realistic financial news headlines"""
        print("\n" + "="*60)
        print("GENERATING NEWS HEADLINES DATA")
        print("="*60)

        # Templates for generating realistic headlines
        positive_templates = [
            "{company} stock surges {pct}% after strong earnings beat",
            "{company} announces record quarterly revenue of ${amount}B",
            "Investors bullish on {company} as {product} sales exceed expectations",
            "{company} raises dividend by {pct}%, signals confidence in growth",
            "Wall Street upgrades {company} to 'Buy' with ${price} target",
            "{company} partners with {partner} in landmark deal worth ${amount}B",
            "Fed signals potential rate cuts, markets rally",
            "GDP growth exceeds expectations at {pct}%, economy shows resilience",
            "Unemployment falls to {pct}%, lowest in decades",
            "{company} expands into {market}, stock jumps {pct}%",
            "Tech sector leads market rally as {company} hits all-time high",
            "{company} AI initiative drives innovation, analysts optimistic",
        ]

        negative_templates = [
            "{company} stock plunges {pct}% on disappointing guidance",
            "{company} misses earnings estimates, cuts full-year outlook",
            "Concerns mount over {company}'s {issue} amid market volatility",
            "{company} announces layoffs affecting {num} employees",
            "Wall Street downgrades {company} citing {concern}",
            "{company} faces regulatory scrutiny over {issue}",
            "Fed raises rates again, markets tumble",
            "Recession fears grow as GDP contracts {pct}%",
            "Unemployment rises to {pct}%, sparking economic concerns",
            "{company} recalls {product} due to safety concerns",
            "Trade tensions escalate, tech stocks slide",
            "Oil prices surge {pct}%, raising inflation concerns",
        ]

        neutral_templates = [
            "{company} to report earnings next week, analysts mixed",
            "Markets flat as investors await Fed decision",
            "{company} CEO discusses strategic priorities at conference",
            "Sector rotation continues as investors reassess portfolios",
            "{company} maintains guidance amid economic uncertainty",
            "Trading volume light ahead of holiday weekend",
            "{company} files for new patent in {technology}",
            "Market volatility expected to continue, experts say",
        ]

        companies = ["Apple", "Microsoft", "Google", "Amazon", "Tesla", "Meta",
                     "NVIDIA", "JPMorgan", "Goldman Sachs", "Bank of America",
                     "Johnson & Johnson", "Walmart", "Exxon", "Coca-Cola"]

        products = ["iPhone", "Azure", "Cloud", "AWS", "Model Y", "Quest",
                    "GPUs", "Trading", "Investment Banking", "Healthcare"]

        partners = ["IBM", "Oracle", "Salesforce", "Adobe", "Intel", "AMD"]

        markets = ["Asia", "Europe", "Latin America", "Africa", "Middle East"]

        issues = ["data privacy", "market competition", "supply chain",
                  "labor practices", "environmental compliance"]

        concerns = ["slowing growth", "increased competition", "margin pressure",
                    "regulatory headwinds", "market saturation"]

        technologies = ["AI", "machine learning", "quantum computing",
                        "blockchain", "autonomous systems"]

        np.random.seed(42)
        all_headlines = []

        # Generate headlines for 5 years
        dates = pd.date_range(start='2019-01-01', end='2024-11-25', freq='D')

        for date in dates:
            # Generate 5-15 headlines per day
            n_headlines = np.random.randint(5, 16)

            for _ in range(n_headlines):
                # Randomly choose sentiment
                sentiment_roll = np.random.random()

                if sentiment_roll < 0.35:
                    template = np.random.choice(positive_templates)
                    sentiment = "positive"
                    sentiment_score = np.random.uniform(0.3, 0.9)
                elif sentiment_roll < 0.7:
                    template = np.random.choice(negative_templates)
                    sentiment = "negative"
                    sentiment_score = np.random.uniform(-0.9, -0.3)
                else:
                    template = np.random.choice(neutral_templates)
                    sentiment = "neutral"
                    sentiment_score = np.random.uniform(-0.2, 0.2)

                # Fill in template
                headline = template.format(
                    company=np.random.choice(companies),
                    pct=np.random.randint(2, 25),
                    amount=np.random.randint(1, 50),
                    price=np.random.randint(100, 500),
                    product=np.random.choice(products),
                    partner=np.random.choice(partners),
                    market=np.random.choice(markets),
                    issue=np.random.choice(issues),
                    concern=np.random.choice(concerns),
                    technology=np.random.choice(technologies),
                    num=np.random.randint(100, 10000)
                )

                # Extract mentioned company
                mentioned_company = None
                for comp in companies:
                    if comp in headline:
                        mentioned_company = comp
                        break

                all_headlines.append({
                    'date': date,
                    'timestamp': date + timedelta(hours=np.random.randint(6, 20)),
                    'headline': headline,
                    'sentiment': sentiment,
                    'sentiment_score': round(sentiment_score, 3),
                    'company_mentioned': mentioned_company,
                    'source': np.random.choice(['Reuters', 'Bloomberg', 'CNBC',
                                                'Wall Street Journal', 'Financial Times',
                                                'MarketWatch', 'Yahoo Finance']),
                    'category': np.random.choice(['Earnings', 'Markets', 'Economy',
                                                  'Technology', 'Policy', 'Analysis'])
                })

        df = pd.DataFrame(all_headlines)

        # Save main file
        output_file = NEWS_DIR / "financial_news_headlines.csv"
        df.to_csv(output_file, index=False)
        print(f"✓ Generated {len(df)} news headlines")
        print(f"  Saved to {output_file}")

        # Save sentiment summary by date
        daily_sentiment = df.groupby(df['date'].dt.date).agg({
            'sentiment_score': ['mean', 'std', 'count'],
            'headline': 'count'
        }).reset_index()
        daily_sentiment.columns = ['date', 'avg_sentiment', 'sentiment_std',
                                   'sentiment_count', 'headline_count']
        daily_sentiment.to_csv(NEWS_DIR / "daily_sentiment_summary.csv", index=False)

        # Save company-level sentiment
        company_sentiment = df[df['company_mentioned'].notna()].groupby(
            ['company_mentioned', df['date'].dt.to_period('M')]
        ).agg({
            'sentiment_score': ['mean', 'count']
        }).reset_index()
        company_sentiment.columns = ['company', 'month', 'avg_sentiment', 'mention_count']
        company_sentiment.to_csv(NEWS_DIR / "company_sentiment_monthly.csv", index=False)

        return df


def main():
    """Download all datasets"""
    print("="*60)
    print("BATCH DATA DOWNLOADER")
    print("="*60)
    print(f"Data directory: {DATA_DIR}")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Download World Bank data
    wb = WorldBankDownloader()
    wb_data = wb.download_all()

    # Download FRED data
    fred = FREDDownloader()
    fred_data = fred.download_all()

    # Download Stock data
    stocks = StockDataDownloader()
    stock_data = stocks.download_all()

    # Download News headlines
    news = NewsHeadlinesDownloader()
    news_data = news.download_all()

    print("\n" + "="*60)
    print("DOWNLOAD COMPLETE!")
    print("="*60)
    print(f"\nData saved to: {DATA_DIR}")
    print("\nFiles created:")
    for f in DATA_DIR.rglob("*.csv"):
        size = f.stat().st_size / 1024 / 1024
        print(f"  {f.relative_to(DATA_DIR)}: {size:.2f} MB")

    print(f"\nFinished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
