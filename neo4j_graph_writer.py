#!/usr/bin/env python3
"""
NEO4J GRAPH WRITER - Creates meaningful relationships in Neo4j
Instead of just creating flat nodes, this creates a proper graph with:
- Entity nodes (RedditPost, PriceMovement, Exchange, Asset)
- Relationship edges (LEADS_TO, ARBITRAGE, CORRELATES_WITH, DIVERGES_FROM)
"""

import os
from typing import List, Dict
from neo4j import GraphDatabase
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")


class Neo4jGraphWriter:
    """Creates meaningful graph relationships in Neo4j"""

    def __init__(self):
        self.driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        print(f"[neo4j] Connected to {NEO4J_URI}")

    def close(self):
        self.driver.close()

    def clear_all(self):
        """Clear all nodes and relationships (use with caution!)"""
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            print("[neo4j] Cleared all nodes and relationships")

    # ========================================================================
    # ENTITY NODE CREATION
    # ========================================================================

    def create_reddit_post_nodes(self, sentiment_df_rows: List):
        """Create individual RedditPost nodes"""
        print(f"[neo4j] Creating {len(sentiment_df_rows)} RedditPost nodes...")

        with self.driver.session() as session:
            for row in sentiment_df_rows:
                row_dict = row.asDict()

                # Create RedditPost node
                session.run("""
                    MERGE (p:RedditPost {item_id: $item_id})
                    SET p.item_type = $item_type,
                        p.subreddit = $subreddit,
                        p.text_preview = substring($text, 0, 200),
                        p.sentiment_polarity = $sentiment_polarity,
                        p.sentiment_label = $sentiment_label,
                        p.timestamp = datetime($timestamp)
                """, {
                    "item_id": row_dict.get("item_id"),
                    "item_type": row_dict.get("item_type"),
                    "subreddit": row_dict.get("subreddit"),
                    "text": row_dict.get("text", "")[:200],  # First 200 chars
                    "sentiment_polarity": float(row_dict.get("sentiment", {}).get("polarity", 0)),
                    "sentiment_label": row_dict.get("sentiment", {}).get("label", "neutral"),
                    "timestamp": datetime.fromtimestamp(row_dict.get("timestamp", 0)).isoformat()
                })

        print(f"[neo4j] ✅ Created RedditPost nodes")

    def create_price_movement_nodes(self, orderbook_df_rows: List):
        """Create PriceMovement nodes (aggregated by hour)"""
        print(f"[neo4j] Creating PriceMovement nodes from {len(orderbook_df_rows)} records...")

        # Aggregate by hour
        price_movements = {}
        for row in orderbook_df_rows:
            row_dict = row.asDict()
            timestamp = row_dict.get("timestamp")
            symbol = row_dict.get("symbol")
            exchange = row_dict.get("exchange")

            # Create hourly key
            hour_key = f"{symbol}_{exchange}_{timestamp.strftime('%Y-%m-%d_%H')}"

            if hour_key not in price_movements:
                price_movements[hour_key] = {
                    "symbol": symbol,
                    "exchange": exchange,
                    "hour": timestamp.strftime('%Y-%m-%d %H:00:00'),
                    "prices": []
                }

            price_movements[hour_key]["prices"].append(row_dict.get("mid_price", 0))

        # Create nodes
        with self.driver.session() as session:
            for key, data in price_movements.items():
                prices = data["prices"]
                avg_price = sum(prices) / len(prices) if prices else 0
                price_change_pct = ((prices[-1] - prices[0]) / prices[0] * 100) if len(prices) > 1 and prices[0] > 0 else 0

                session.run("""
                    MERGE (pm:PriceMovement {id: $id})
                    SET pm.symbol = $symbol,
                        pm.exchange = $exchange,
                        pm.hour = datetime($hour),
                        pm.avg_price = $avg_price,
                        pm.price_change_pct = $price_change_pct,
                        pm.tick_count = $tick_count
                """, {
                    "id": key,
                    "symbol": data["symbol"],
                    "exchange": data["exchange"],
                    "hour": data["hour"],
                    "avg_price": avg_price,
                    "price_change_pct": price_change_pct,
                    "tick_count": len(prices)
                })

        print(f"[neo4j] ✅ Created {len(price_movements)} PriceMovement nodes")

    def create_exchange_nodes(self, orderbook_df_rows: List):
        """Create Exchange nodes"""
        exchanges = set()
        for row in orderbook_df_rows:
            row_dict = row.asDict()
            exchanges.add((row_dict.get("exchange"), row_dict.get("region", "UNKNOWN")))

        print(f"[neo4j] Creating {len(exchanges)} Exchange nodes...")
        with self.driver.session() as session:
            for exchange, region in exchanges:
                session.run("""
                    MERGE (e:Exchange {name: $name})
                    SET e.region = $region
                """, {"name": exchange, "region": region})

        print(f"[neo4j] ✅ Created Exchange nodes")

    def create_asset_nodes(self, orderbook_df_rows: List):
        """Create Asset nodes"""
        assets = set()
        for row in orderbook_df_rows:
            row_dict = row.asDict()
            assets.add(row_dict.get("base"))

        print(f"[neo4j] Creating {len(assets)} Asset nodes...")
        with self.driver.session() as session:
            for asset in assets:
                if asset:
                    session.run("""
                        MERGE (a:Asset {symbol: $symbol})
                    """, {"symbol": asset})

        print(f"[neo4j] ✅ Created Asset nodes")

    # ========================================================================
    # RELATIONSHIP CREATION
    # ========================================================================

    def create_sentiment_price_relationships(self, sentiment_df_rows: List, orderbook_df_rows: List):
        """
        Create BIDIRECTIONAL LEADS_TO relationships:
        1. (RedditPost)-[:LEADS_TO]->(PriceMovement) - Sentiment predicts price
        2. (PriceMovement)-[:LEADS_TO]->(RedditPost) - Price triggers discussion
        """
        print("[neo4j] Creating BIDIRECTIONAL sentiment↔price LEADS_TO relationships...")

        # Build time-indexed structures
        from datetime import timedelta

        price_by_hour = {}
        posts_by_hour = {}

        # Index prices by hour
        for row in orderbook_df_rows:
            row_dict = row.asDict()
            timestamp = row_dict.get("timestamp")
            hour_key = timestamp.strftime('%Y-%m-%d %H:00:00')
            symbol = row_dict.get("symbol")

            if hour_key not in price_by_hour:
                price_by_hour[hour_key] = {}
            if symbol not in price_by_hour[hour_key]:
                price_by_hour[hour_key][symbol] = []

            price_by_hour[hour_key][symbol].append(row_dict.get("mid_price", 0))

        # Index posts by hour
        for row in sentiment_df_rows:
            row_dict = row.asDict()
            post_time = datetime.fromtimestamp(row_dict.get("timestamp", 0))
            hour_key = post_time.strftime('%Y-%m-%d %H:00:00')

            if hour_key not in posts_by_hour:
                posts_by_hour[hour_key] = []
            posts_by_hour[hour_key].append(row_dict)

        relationships_created = 0
        with self.driver.session() as session:
            # DIRECTION 1: Post → Price (sentiment predicts price)
            print("[neo4j]   Creating Post→Price edges (sentiment leads)...")
            for sentiment_row in sentiment_df_rows:
                sentiment_dict = sentiment_row.asDict()
                post_time = datetime.fromtimestamp(sentiment_dict.get("timestamp", 0))
                post_id = sentiment_dict.get("item_id")
                sentiment_polarity = float(sentiment_dict.get("sentiment", {}).get("polarity", 0))

                # Look for price movements 1h, 2h, 4h AFTER post
                for lag_hours in [1, 2, 4, 8]:
                    future_time = post_time + timedelta(hours=lag_hours)
                    future_hour_key = future_time.strftime('%Y-%m-%d %H:00:00')

                    if future_hour_key in price_by_hour:
                        for symbol, prices in price_by_hour[future_hour_key].items():
                            if prices:
                                result = session.run("""
                                    MATCH (post:RedditPost {item_id: $post_id})
                                    MATCH (pm:PriceMovement)
                                    WHERE pm.symbol = $symbol
                                      AND pm.hour = datetime($future_hour)
                                    MERGE (post)-[r:LEADS_TO]->(pm)
                                    SET r.lag_hours = $lag_hours,
                                        r.direction = 'sentiment_first',
                                        r.sentiment_polarity = $sentiment_polarity,
                                        r.price_change = pm.price_change_pct,
                                        r.direction_match = CASE
                                            WHEN (sign($sentiment_polarity) = sign(pm.price_change_pct))
                                            THEN true ELSE false END
                                    RETURN count(r) as created
                                """, {
                                    "post_id": post_id,
                                    "symbol": symbol,
                                    "future_hour": future_hour_key,
                                    "lag_hours": lag_hours,
                                    "sentiment_polarity": sentiment_polarity
                                })

                                for record in result:
                                    relationships_created += record["created"]

            # DIRECTION 2: Price → Post (price triggers discussion)
            print("[neo4j]   Creating Price→Post edges (price leads)...")
            for row in orderbook_df_rows[:5000]:  # Limit to avoid too many relationships
                row_dict = row.asDict()
                price_time = row_dict.get("timestamp")
                symbol = row_dict.get("symbol")
                hour_key = price_time.strftime('%Y-%m-%d %H:00:00')
                mid_price = row_dict.get("mid_price", 0)

                # Look for Reddit posts 1h, 2h, 4h AFTER price movement
                for lag_hours in [1, 2, 4, 8]:
                    future_time = price_time + timedelta(hours=lag_hours)
                    future_hour_key = future_time.strftime('%Y-%m-%d %H:00:00')

                    if future_hour_key in posts_by_hour:
                        for post_dict in posts_by_hour[future_hour_key]:
                            post_id = post_dict.get("item_id")
                            sentiment_polarity = float(post_dict.get("sentiment", {}).get("polarity", 0))

                            # Find price change that might have triggered discussion
                            if hour_key in price_by_hour and symbol in price_by_hour[hour_key]:
                                prices = price_by_hour[hour_key][symbol]
                                if prices:
                                    avg_price = sum(prices) / len(prices)
                                    price_change = ((mid_price - avg_price) / avg_price * 100) if avg_price > 0 else 0

                                    result = session.run("""
                                        MATCH (pm:PriceMovement)
                                        WHERE pm.symbol = $symbol
                                          AND pm.hour = datetime($hour)
                                        MATCH (post:RedditPost {item_id: $post_id})
                                        MERGE (pm)-[r:LEADS_TO]->(post)
                                        SET r.lag_hours = $lag_hours,
                                            r.direction = 'price_first',
                                            r.price_change = $price_change,
                                            r.sentiment_polarity = $sentiment_polarity,
                                            r.direction_match = CASE
                                                WHEN (sign($price_change) = sign($sentiment_polarity))
                                                THEN true ELSE false END
                                        RETURN count(r) as created
                                    """, {
                                        "symbol": symbol,
                                        "hour": hour_key,
                                        "post_id": post_id,
                                        "lag_hours": lag_hours,
                                        "price_change": price_change,
                                        "sentiment_polarity": sentiment_polarity
                                    })

                                    for record in result:
                                        relationships_created += record["created"]

        print(f"[neo4j] ✅ Created {relationships_created} BIDIRECTIONAL LEADS_TO relationships")

    def create_arbitrage_relationships(self, arbitrage_df_rows: List):
        """
        Create BIDIRECTIONAL arbitrage relationships:
        (Buy Exchange)-[:ARBITRAGE]->(Sell Exchange)
        (Sell Exchange)-[:ARBITRAGE]->(Buy Exchange)
        Shows arbitrage opportunities in both directions
        """
        print(f"[neo4j] Creating BIDIRECTIONAL arbitrage relationships from {len(arbitrage_df_rows)} opportunities...")

        with self.driver.session() as session:
            for row in arbitrage_df_rows:
                row_dict = row.asDict()

                # Direction 1: Buy → Sell
                session.run("""
                    MATCH (ex1:Exchange {name: $buy_exchange})
                    MATCH (ex2:Exchange {name: $sell_exchange})
                    MERGE (ex1)-[r:ARBITRAGE {
                        symbol: $symbol,
                        timestamp: datetime($timestamp),
                        direction: 'buy_to_sell'
                    }]->(ex2)
                    SET r.spread_bps = $spread_bps,
                        r.profit_pct = $profit_pct,
                        r.buy_price = $buy_price,
                        r.sell_price = $sell_price,
                        r.role = 'buyer'
                """, {
                    "buy_exchange": row_dict.get("buy_exchange"),
                    "sell_exchange": row_dict.get("sell_exchange"),
                    "symbol": row_dict.get("symbol"),
                    "timestamp": row_dict.get("timestamp").isoformat() if row_dict.get("timestamp") else None,
                    "spread_bps": float(row_dict.get("spread_bps", 0)),
                    "profit_pct": float(row_dict.get("profit_pct", 0)),
                    "buy_price": float(row_dict.get("buy_price", 0)),
                    "sell_price": float(row_dict.get("sell_price", 0))
                })

                # Direction 2: Sell → Buy (reverse relationship)
                session.run("""
                    MATCH (ex1:Exchange {name: $sell_exchange})
                    MATCH (ex2:Exchange {name: $buy_exchange})
                    MERGE (ex1)-[r:ARBITRAGE {
                        symbol: $symbol,
                        timestamp: datetime($timestamp),
                        direction: 'sell_to_buy'
                    }]->(ex2)
                    SET r.spread_bps = $spread_bps,
                        r.profit_pct = $profit_pct,
                        r.buy_price = $buy_price,
                        r.sell_price = $sell_price,
                        r.role = 'seller'
                """, {
                    "sell_exchange": row_dict.get("sell_exchange"),
                    "buy_exchange": row_dict.get("buy_exchange"),
                    "symbol": row_dict.get("symbol"),
                    "timestamp": row_dict.get("timestamp").isoformat() if row_dict.get("timestamp") else None,
                    "spread_bps": float(row_dict.get("spread_bps", 0)),
                    "profit_pct": float(row_dict.get("profit_pct", 0)),
                    "buy_price": float(row_dict.get("buy_price", 0)),
                    "sell_price": float(row_dict.get("sell_price", 0))
                })

        print(f"[neo4j] ✅ Created BIDIRECTIONAL arbitrage relationships")

    def create_correlation_relationships(self, correlation_df_rows: List):
        """
        Create BIDIRECTIONAL correlation relationships:
        (Asset1)-[:CORRELATES_WITH]->(Asset2)
        (Asset2)-[:CORRELATES_WITH]->(Asset1)
        Correlation is symmetric, so both directions have same strength
        """
        print(f"[neo4j] Creating BIDIRECTIONAL asset correlation relationships from {len(correlation_df_rows)} pairs...")

        with self.driver.session() as session:
            for row in correlation_df_rows:
                row_dict = row.asDict()

                # Direction 1: Asset1 → Asset2
                session.run("""
                    MATCH (a1:Asset {symbol: $asset_1})
                    MATCH (a2:Asset {symbol: $asset_2})
                    MERGE (a1)-[r:CORRELATES_WITH]->(a2)
                    SET r.correlation = $correlation,
                        r.strength = $strength,
                        r.market_regime = $market_regime,
                        r.timestamp = datetime($timestamp)
                """, {
                    "asset_1": row_dict.get("asset_1"),
                    "asset_2": row_dict.get("asset_2"),
                    "correlation": float(row_dict.get("correlation", 0)),
                    "strength": row_dict.get("correlation_strength"),
                    "market_regime": row_dict.get("market_regime"),
                    "timestamp": row_dict.get("timestamp").isoformat() if row_dict.get("timestamp") else datetime.now().isoformat()
                })

                # Direction 2: Asset2 → Asset1 (symmetric)
                session.run("""
                    MATCH (a1:Asset {symbol: $asset_2})
                    MATCH (a2:Asset {symbol: $asset_1})
                    MERGE (a1)-[r:CORRELATES_WITH]->(a2)
                    SET r.correlation = $correlation,
                        r.strength = $strength,
                        r.market_regime = $market_regime,
                        r.timestamp = datetime($timestamp)
                """, {
                    "asset_2": row_dict.get("asset_2"),
                    "asset_1": row_dict.get("asset_1"),
                    "correlation": float(row_dict.get("correlation", 0)),
                    "strength": row_dict.get("correlation_strength"),
                    "market_regime": row_dict.get("market_regime"),
                    "timestamp": row_dict.get("timestamp").isoformat() if row_dict.get("timestamp") else datetime.now().isoformat()
                })

        print(f"[neo4j] ✅ Created BIDIRECTIONAL correlation relationships")

    def create_divergence_relationships(self, divergence_df_rows: List):
        """
        Create BIDIRECTIONAL divergence relationships:
        (RedditPost)-[:DIVERGES_FROM]->(PriceMovement)
        (PriceMovement)-[:DIVERGES_FROM]->(RedditPost)
        When sentiment and price move in opposite directions
        """
        print(f"[neo4j] Creating BIDIRECTIONAL divergence relationships from {len(divergence_df_rows)} divergences...")

        with self.driver.session() as session:
            for row in divergence_df_rows:
                row_dict = row.asDict()

                # Find Reddit posts in that time period
                period_start = row_dict.get("period_start")
                subreddit = row_dict.get("subreddit")
                symbol = row_dict.get("symbol")

                # Direction 1: Post → Price
                session.run("""
                    MATCH (post:RedditPost)
                    WHERE post.subreddit = $subreddit
                      AND post.timestamp >= datetime($period_start)
                      AND post.timestamp < datetime($period_start) + duration('PT1H')
                    MATCH (pm:PriceMovement)
                    WHERE pm.symbol = $symbol
                      AND pm.hour = datetime($period_start)
                    MERGE (post)-[r:DIVERGES_FROM]->(pm)
                    SET r.divergence_strength = $strength,
                        r.divergence_type = $divergence_type,
                        r.sentiment_direction = $sentiment_direction,
                        r.price_direction = $price_direction,
                        r.direction = 'post_to_price'
                """, {
                    "subreddit": subreddit,
                    "period_start": period_start.isoformat() if hasattr(period_start, 'isoformat') else str(period_start),
                    "symbol": symbol,
                    "strength": float(row_dict.get("divergence_strength", 0)),
                    "divergence_type": row_dict.get("divergence_type"),
                    "sentiment_direction": int(row_dict.get("sentiment_direction", 0)),
                    "price_direction": int(row_dict.get("price_direction", 0))
                })

                # Direction 2: Price → Post
                session.run("""
                    MATCH (pm:PriceMovement)
                    WHERE pm.symbol = $symbol
                      AND pm.hour = datetime($period_start)
                    MATCH (post:RedditPost)
                    WHERE post.subreddit = $subreddit
                      AND post.timestamp >= datetime($period_start)
                      AND post.timestamp < datetime($period_start) + duration('PT1H')
                    MERGE (pm)-[r:DIVERGES_FROM]->(post)
                    SET r.divergence_strength = $strength,
                        r.divergence_type = $divergence_type,
                        r.sentiment_direction = $sentiment_direction,
                        r.price_direction = $price_direction,
                        r.direction = 'price_to_post'
                """, {
                    "symbol": symbol,
                    "period_start": period_start.isoformat() if hasattr(period_start, 'isoformat') else str(period_start),
                    "subreddit": subreddit,
                    "strength": float(row_dict.get("divergence_strength", 0)),
                    "divergence_type": row_dict.get("divergence_type"),
                    "sentiment_direction": int(row_dict.get("sentiment_direction", 0)),
                    "price_direction": int(row_dict.get("price_direction", 0))
                })

        print(f"[neo4j] ✅ Created BIDIRECTIONAL divergence relationships")

    # ========================================================================
    # HELPER: Write complete graph from analytics
    # ========================================================================

    def write_complete_graph(self, sentiment_data, orderbook_data, arbitrage_data,
                            correlation_data, divergence_data):
        """
        Write complete graph structure from all analytics
        """
        print("\n" + "=" * 80)
        print("NEO4J GRAPH CREATION")
        print("=" * 80)

        try:
            # Step 1: Create entity nodes
            print("\n[1] Creating entity nodes...")
            self.create_reddit_post_nodes(sentiment_data)
            self.create_price_movement_nodes(orderbook_data)
            self.create_exchange_nodes(orderbook_data)
            self.create_asset_nodes(orderbook_data)

            # Step 2: Create relationships
            print("\n[2] Creating relationships...")
            self.create_sentiment_price_relationships(sentiment_data, orderbook_data)

            if arbitrage_data:
                self.create_arbitrage_relationships(arbitrage_data)

            if correlation_data:
                self.create_correlation_relationships(correlation_data)

            if divergence_data:
                self.create_divergence_relationships(divergence_data)

            print("\n" + "=" * 80)
            print("✅ NEO4J GRAPH CREATION COMPLETE")
            print("=" * 80)

        except Exception as e:
            print(f"\n[ERROR] Graph creation failed: {e}")
            import traceback
            traceback.print_exc()
