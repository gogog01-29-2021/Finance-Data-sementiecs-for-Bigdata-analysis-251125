-- Create keyspace for financial data
CREATE KEYSPACE IF NOT EXISTS financial_data
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE financial_data;

-- Arbitrage opportunities table
CREATE TABLE IF NOT EXISTS arbitrage_opportunities (
    symbol text,
    buy_exchange text,
    sell_exchange text,
    timestamp timestamp,
    base text,
    buy_price double,
    sell_price double,
    buy_region text,
    sell_region text,
    spread_bps double,
    profit_pct double,
    PRIMARY KEY ((symbol), timestamp, buy_exchange, sell_exchange)
) WITH CLUSTERING ORDER BY (timestamp DESC);

-- Price predictions table
CREATE TABLE IF NOT EXISTS price_predictions (
    symbol text,
    exchange text,
    timestamp timestamp,
    base text,
    quote text,
    venue_type text,
    mid_price double,
    ma_10 double,
    trend text,
    prediction text,
    confidence double,
    PRIMARY KEY ((symbol, exchange), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

DESCRIBE TABLES;
