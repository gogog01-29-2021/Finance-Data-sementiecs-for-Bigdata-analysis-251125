CREATE TABLE IF NOT EXISTS financial_data.sentiment_correlations (
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
