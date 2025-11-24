-- ============================================================================
-- CASSANDRA TABLE DEFINITIONS FOR ENHANCED ANALYTICS PIPELINE
-- Run this file after creating the keyspace:
-- CREATE KEYSPACE IF NOT EXISTS financial_data
-- WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
-- ============================================================================

USE financial_data;

-- ============================================================================
-- A) ORIGINAL ANALYTICS (IMPROVED)
-- ============================================================================

-- A1: Arbitrage Opportunities
CREATE TABLE IF NOT EXISTS arbitrage_opportunities (
    timestamp timestamp,
    symbol text,
    buy_exchange text,
    sell_exchange text,
    base text,
    buy_price double,
    sell_price double,
    buy_region text,
    sell_region text,
    spread_bps double,
    profit_pct double,
    PRIMARY KEY ((symbol), timestamp, buy_exchange, sell_exchange)
) WITH CLUSTERING ORDER BY (timestamp DESC);

-- A2: Sentiment Correlations
CREATE TABLE IF NOT EXISTS sentiment_correlations (
    period_start timestamp,
    symbol text,
    subreddit text,
    period_end timestamp,
    avg_sentiment double,
    sentiment_volatility double,
    post_count bigint,
    avg_price double,
    price_volatility double,
    avg_spread double,
    PRIMARY KEY ((period_start), symbol, subreddit)
);

-- A3: Price Predictions
CREATE TABLE IF NOT EXISTS price_predictions (
    timestamp timestamp,
    exchange text,
    symbol text,
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

-- ============================================================================
-- B) VOLUME & LIQUIDITY ANALYTICS
-- ============================================================================

-- B1: Order Depth Analysis
CREATE TABLE IF NOT EXISTS order_depth (
    period_start timestamp,
    period_end timestamp,
    symbol text,
    exchange text,
    avg_spread double,
    min_spread double,
    max_spread double,
    tick_count bigint,
    liquidity_score double,
    market_depth_tier text,
    spread_stability double,
    PRIMARY KEY ((symbol, exchange), period_start)
) WITH CLUSTERING ORDER BY (period_start DESC);

-- B2: VWAP (Volume-Weighted Average Price)
CREATE TABLE IF NOT EXISTS vwap (
    period_start timestamp,
    period_end timestamp,
    symbol text,
    exchange text,
    VWAP double,
    simple_avg double,
    avg_spread double,
    tick_count bigint,
    vwap_vs_avg_pct double,
    signal text,
    PRIMARY KEY ((symbol, exchange), period_start)
) WITH CLUSTERING ORDER BY (period_start DESC);

-- B3: Bid-Ask Imbalance
CREATE TABLE IF NOT EXISTS bid_ask_imbalance (
    period_start timestamp,
    period_end timestamp,
    symbol text,
    exchange text,
    avg_imbalance double,
    avg_bid_strength double,
    avg_ask_strength double,
    tick_count bigint,
    pressure_type text,
    confidence double,
    PRIMARY KEY ((symbol, exchange), period_start)
) WITH CLUSTERING ORDER BY (period_start DESC);

-- ============================================================================
-- C) VOLATILITY & PATTERN DETECTION
-- ============================================================================

-- C1: Rolling Volatility
CREATE TABLE IF NOT EXISTS rolling_volatility (
    timestamp timestamp,
    exchange text,
    symbol text,
    base text,
    quote text,
    mid_price double,
    volatility_1h double,
    volatility_4h double,
    volatility_24h double,
    volatility_trend text,
    spike_detected boolean,
    PRIMARY KEY ((symbol, exchange), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

-- C2: Bollinger Bands
CREATE TABLE IF NOT EXISTS bollinger_bands (
    timestamp timestamp,
    exchange text,
    symbol text,
    mid_price double,
    MA_20 double,
    upper_band double,
    lower_band double,
    band_position double,
    band_width_pct double,
    signal text,
    PRIMARY KEY ((symbol, exchange), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

-- C3: Flash Events
CREATE TABLE IF NOT EXISTS flash_events (
    minute_bucket timestamp,
    symbol text,
    exchange text,
    first_timestamp timestamp,
    last_timestamp timestamp,
    first_price double,
    last_price double,
    min_price double,
    max_price double,
    tick_count bigint,
    minute_change_pct double,
    minute_range_pct double,
    flash_event_detected boolean,
    event_type text,
    recovery_within_minute boolean,
    PRIMARY KEY ((symbol, exchange), minute_bucket)
) WITH CLUSTERING ORDER BY (minute_bucket DESC);

-- ============================================================================
-- D) ENHANCED SENTIMENT ANALYTICS
-- ============================================================================

-- D1: Sentiment Momentum
CREATE TABLE IF NOT EXISTS sentiment_momentum (
    hour_start timestamp,
    hour_end timestamp,
    subreddit text,
    avg_sentiment double,
    post_count bigint,
    previous_sentiment double,
    previous_post_count bigint,
    sentiment_momentum double,
    post_count_change_pct double,
    momentum_type text,
    PRIMARY KEY ((subreddit), hour_start)
) WITH CLUSTERING ORDER BY (hour_start DESC);

-- D2: Sentiment-Price Divergence
CREATE TABLE IF NOT EXISTS sentiment_divergence (
    period_start timestamp,
    period_end timestamp,
    subreddit text,
    symbol text,
    avg_sentiment double,
    price_change_pct double,
    sentiment_direction int,
    price_direction int,
    divergence_detected boolean,
    divergence_strength double,
    divergence_type text,
    PRIMARY KEY ((period_start), subreddit, symbol)
);

-- ============================================================================
-- E) CROSS-ASSET ANALYTICS
-- ============================================================================

-- E1: Correlation Matrix
CREATE TABLE IF NOT EXISTS correlation_matrix (
    asset_1 text,
    asset_2 text,
    timestamp timestamp,
    correlation double,
    correlation_strength text,
    market_regime text,
    PRIMARY KEY ((asset_1, asset_2), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

-- ============================================================================
-- F) TEXT CONTENT ANALYTICS (Word-Level Analysis)
-- ============================================================================

-- F1: Word Frequency Analysis
CREATE TABLE IF NOT EXISTS word_frequency (
    period_start timestamp,
    period_end timestamp,
    subreddit text,
    word text,
    word_frequency bigint,
    avg_sentiment_for_word double,
    avg_text_length double,
    word_rank bigint,
    PRIMARY KEY ((subreddit, period_start), word_frequency, word)
) WITH CLUSTERING ORDER BY (word_frequency DESC, word ASC);

-- F2: Word-Price Correlation
CREATE TABLE IF NOT EXISTS word_price_correlation (
    period_start timestamp,
    period_end timestamp,
    subreddit text,
    symbol text,
    word text,
    word_frequency bigint,
    frequency_change_pct double,
    avg_price double,
    price_change_pct double,
    word_trend text,
    price_trend text,
    correlation_signal text,
    PRIMARY KEY ((period_start), subreddit, symbol, word)
);

-- F3: Word-Sentiment Distribution
CREATE TABLE IF NOT EXISTS word_sentiment_distribution (
    subreddit text,
    word text,
    timestamp timestamp,
    total_occurrences bigint,
    avg_sentiment double,
    sentiment_stddev double,
    max_sentiment double,
    min_sentiment double,
    sentiment_range double,
    sentiment_category text,
    PRIMARY KEY ((subreddit), total_occurrences, word)
) WITH CLUSTERING ORDER BY (total_occurrences DESC, word ASC);

-- ============================================================================
-- INDEXES FOR EFFICIENT QUERIES
-- ============================================================================

-- Create secondary indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_arbitrage_spread ON arbitrage_opportunities (spread_bps);
CREATE INDEX IF NOT EXISTS idx_flash_events_type ON flash_events (event_type);
CREATE INDEX IF NOT EXISTS idx_volatility_spike ON rolling_volatility (spike_detected);
CREATE INDEX IF NOT EXISTS idx_bollinger_signal ON bollinger_bands (signal);
CREATE INDEX IF NOT EXISTS idx_divergence_type ON sentiment_divergence (divergence_type);

-- Word analytics indexes
CREATE INDEX IF NOT EXISTS idx_word_frequency ON word_frequency (word);
CREATE INDEX IF NOT EXISTS idx_word_price_signal ON word_price_correlation (correlation_signal);
CREATE INDEX IF NOT EXISTS idx_word_sentiment_category ON word_sentiment_distribution (sentiment_category);

-- ============================================================================
-- NOTES
-- ============================================================================
-- 1. All tables use partition keys optimized for time-series queries
-- 2. Clustering keys are ordered DESC for recent-data-first access
-- 3. Secondary indexes created for filtering analytics results
-- 4. Replication factor set to 1 for development (increase for production)
-- 5. NEW: Word analytics tables (F1-F3) enable text-level analysis of sentiment data
--    - word_frequency: Track word usage over time windows
--    - word_price_correlation: Connect word trends with price movements
--    - word_sentiment_distribution: Analyze sentiment associated with specific words
