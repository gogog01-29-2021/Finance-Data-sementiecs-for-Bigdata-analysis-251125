-- ============================================================================
-- UNIFIED CASSANDRA SCHEMA - SINGLE TABLE APPROACH
-- All analytics stored in ONE table per database
-- ============================================================================

USE financial_data;

-- Drop old tables if migrating (optional - comment out if you want to keep both)
-- DROP TABLE IF EXISTS arbitrage_opportunities;
-- DROP TABLE IF EXISTS sentiment_correlations;
-- ... (drop all 15 tables)

-- ============================================================================
-- UNIFIED ANALYTICS TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS analytics_unified (
    -- Partition key: analytics type + date bucket for distribution
    analytics_type text,           -- 'arbitrage', 'sentiment_correlation', 'word_frequency', etc.
    date_bucket text,              -- 'YYYY-MM-DD' for daily partitioning

    -- Clustering key: timestamp for time-series ordering
    timestamp timestamp,

    -- Common dimensions (nullable for analytics that don't use them)
    symbol text,
    exchange text,
    subreddit text,
    base text,
    quote text,

    -- Common metrics (nullable for analytics that don't use them)
    value1 double,                 -- Primary metric (price, sentiment, frequency, etc.)
    value2 double,                 -- Secondary metric
    value3 double,                 -- Tertiary metric

    count_metric bigint,           -- Count-based metrics (post_count, tick_count, word_frequency)

    -- Classification fields
    signal_type text,              -- 'BULLISH', 'BEARISH', 'DIVERGENCE', etc.
    category text,                 -- Additional categorization

    -- Analytics-specific data stored as map (flexible schema)
    metadata map<text, text>,      -- Store any additional fields as key-value pairs

    -- Detection flags
    alert_detected boolean,        -- True if this is an alert-worthy event

    PRIMARY KEY ((analytics_type, date_bucket), timestamp, symbol, exchange, subreddit)
) WITH CLUSTERING ORDER BY (timestamp DESC, symbol ASC, exchange ASC, subreddit ASC);

-- ============================================================================
-- INDEXES FOR EFFICIENT QUERIES
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_unified_symbol ON analytics_unified (symbol);
CREATE INDEX IF NOT EXISTS idx_unified_exchange ON analytics_unified (exchange);
CREATE INDEX IF NOT EXISTS idx_unified_subreddit ON analytics_unified (subreddit);
CREATE INDEX IF NOT EXISTS idx_unified_signal_type ON analytics_unified (signal_type);
CREATE INDEX IF NOT EXISTS idx_unified_alert ON analytics_unified (alert_detected);

-- ============================================================================
-- ANALYTICS TYPE MAPPING
-- ============================================================================
-- The analytics_type field identifies which analytic this row represents:
--
-- A) ORIGINAL ANALYTICS:
--   - 'arbitrage'              : Arbitrage opportunities
--   - 'sentiment_correlation'  : Sentiment-price correlation
--   - 'price_prediction'       : Price predictions
--
-- B) VOLUME & LIQUIDITY:
--   - 'order_depth'           : Order depth analysis
--   - 'vwap'                  : Volume-weighted average price
--   - 'bid_ask_imbalance'     : Bid-ask imbalance
--
-- C) VOLATILITY & PATTERNS:
--   - 'rolling_volatility'    : Multi-timeframe volatility
--   - 'bollinger_bands'       : Bollinger bands signals
--   - 'flash_events'          : Flash crash/pump detection
--
-- D) SENTIMENT ANALYTICS:
--   - 'sentiment_momentum'    : Sentiment momentum changes
--   - 'sentiment_divergence'  : Sentiment-price divergence
--
-- E) CROSS-ASSET:
--   - 'correlation_matrix'    : Asset correlations
--
-- F) WORD ANALYTICS:
--   - 'word_frequency'        : Word usage frequency
--   - 'word_price_correlation': Word-price correlation
--   - 'word_sentiment_dist'   : Word-sentiment distribution
--
-- ============================================================================

-- ============================================================================
-- FIELD MAPPING BY ANALYTICS TYPE
-- ============================================================================
-- Different analytics use fields differently:
--
-- ARBITRAGE:
--   - value1: buy_price
--   - value2: sell_price
--   - value3: spread_bps
--   - metadata: {profit_pct, buy_region, sell_region, buy_exchange, sell_exchange}
--
-- WORD_FREQUENCY:
--   - symbol: null (not used)
--   - exchange: null (not used)
--   - subreddit: 'cryptocurrency', 'bitcoin', etc.
--   - value1: avg_sentiment_for_word
--   - value2: avg_text_length
--   - count_metric: word_frequency
--   - metadata: {word, word_rank}
--
-- WORD_PRICE_CORRELATION:
--   - subreddit: 'cryptocurrency'
--   - symbol: 'BTC-USD'
--   - value1: avg_price
--   - value2: price_change_pct
--   - value3: frequency_change_pct
--   - count_metric: word_frequency
--   - signal_type: 'STRONG_BULLISH', 'DIVERGENCE_BEARISH', etc.
--   - metadata: {word, word_trend, price_trend}
--
-- SENTIMENT_CORRELATION:
--   - value1: avg_sentiment
--   - value2: avg_price
--   - value3: price_volatility
--   - count_metric: post_count
--   - metadata: {sentiment_volatility, avg_spread}
--
-- ROLLING_VOLATILITY:
--   - value1: mid_price
--   - value2: volatility_1h
--   - value3: volatility_4h
--   - signal_type: 'INCREASING', 'DECREASING', 'STABLE'
--   - alert_detected: spike_detected
--   - metadata: {volatility_24h, volatility_trend}
--
-- ============================================================================

-- ============================================================================
-- EXAMPLE QUERIES
-- ============================================================================

-- Query 1: Get all arbitrage opportunities for today
-- SELECT * FROM analytics_unified
-- WHERE analytics_type = 'arbitrage'
--   AND date_bucket = '2025-11-21'
--   AND timestamp >= '2025-11-21 00:00:00'
-- LIMIT 100;

-- Query 2: Get word frequency for specific subreddit and date
-- SELECT timestamp, metadata['word'], count_metric as frequency, value1 as sentiment
-- FROM analytics_unified
-- WHERE analytics_type = 'word_frequency'
--   AND date_bucket = '2025-11-21'
--   AND subreddit = 'cryptocurrency'
-- LIMIT 100;

-- Query 3: Get all alerts across all analytics types
-- SELECT analytics_type, timestamp, symbol, signal_type, metadata
-- FROM analytics_unified
-- WHERE alert_detected = true
-- ALLOW FILTERING;

-- Query 4: Get strong bullish word-price correlations
-- SELECT timestamp, symbol, subreddit, metadata['word'], value2 as price_change_pct
-- FROM analytics_unified
-- WHERE analytics_type = 'word_price_correlation'
--   AND date_bucket = '2025-11-21'
--   AND signal_type = 'STRONG_BULLISH'
-- ALLOW FILTERING;

-- Query 5: Get all analytics for a specific symbol
-- SELECT analytics_type, timestamp, value1, value2, signal_type
-- FROM analytics_unified
-- WHERE symbol = 'BTC-USD'
--   AND date_bucket = '2025-11-21'
-- ALLOW FILTERING;

-- ============================================================================
-- NOTES
-- ============================================================================
-- 1. Single table design reduces operational complexity
-- 2. Partition key (analytics_type, date_bucket) ensures good distribution
-- 3. Flexible metadata map allows different analytics to store custom fields
-- 4. Indexes on common filter fields (symbol, exchange, signal_type)
-- 5. Daily date bucketing prevents large partitions
-- 6. All analytics queryable from one place
-- 7. Easier to maintain and backup (1 table vs 15 tables)
