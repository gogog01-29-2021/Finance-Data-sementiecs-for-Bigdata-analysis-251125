# 🔗 NEO4J GRAPH QUERIES - See Your Relationships!

## 📊 **Graph Structure**

### **Entity Nodes:**
- `RedditPost` - Individual Reddit posts/comments with sentiment
- `PriceMovement` - Hourly price aggregations
- `Exchange` - Trading venues (Upbit, Binance, etc.)
- `Asset` - Cryptocurrencies (BTC, ETH, SOL, etc.)

### **Relationship Edges:**
- `LEADS_TO` - Reddit post → Price movement (with lead-lag timing)
- `ARBITRAGE` - Exchange → Exchange (price differences)
- `CORRELATES_WITH` - Asset → Asset (correlation strength)
- `DIVERGES_FROM` - Reddit post → Price movement (opposite directions)

---

## 🔍 **Essential Queries**

### **1. Which Reddit Posts Led to Price Movements?**

```cypher
// Show Reddit posts that led to significant price changes
MATCH (post:RedditPost)-[r:LEADS_TO]->(pm:PriceMovement)
WHERE abs(pm.price_change_pct) > 1.0
RETURN post.item_id,
       post.text_preview,
       post.sentiment_polarity,
       post.subreddit,
       r.lag_hours,
       pm.symbol,
       pm.price_change_pct,
       r.direction_match
ORDER BY abs(pm.price_change_pct) DESC
LIMIT 20;
```

**What you'll see:**
- Which specific Reddit post (text preview)
- Its sentiment (positive/negative)
- How many hours later the price moved
- Whether sentiment matched price direction

---

### **2. Find Lead-Lag Correlations by Subreddit**

```cypher
// Which subreddit has the best predictive power?
MATCH (post:RedditPost)-[r:LEADS_TO]->(pm:PriceMovement)
WHERE r.direction_match = true
WITH post.subreddit as subreddit,
     r.lag_hours as lag,
     count(*) as matches
RETURN subreddit,
       lag as best_lag_hours,
       matches as correct_predictions
ORDER BY matches DESC
LIMIT 10;
```

**What you'll see:**
- Which subreddit (e.g., "BitcoinKR", "SatoshiStreetBets")
- Best lead-lag timing (e.g., 4 hours)
- Number of correct predictions

---

### **3. Visualize Sentiment → Price Graph**

```cypher
// Create a graph visualization
MATCH path = (post:RedditPost)-[r:LEADS_TO]->(pm:PriceMovement)
WHERE r.lag_hours = 4
  AND abs(pm.price_change_pct) > 2.0
RETURN path
LIMIT 50;
```

**What you'll see in Neo4j Browser:**
- Visual graph of Reddit posts connecting to price movements
- Can click on nodes to see full details
- Color-coded by sentiment/price direction

---

### **4. Arbitrage Paths Between Exchanges**

```cypher
// Find profitable arbitrage opportunities
MATCH (ex1:Exchange)-[r:ARBITRAGE]->(ex2:Exchange)
WHERE r.spread_bps > 10
RETURN ex1.name as buy_exchange,
       ex2.name as sell_exchange,
       r.symbol,
       r.spread_bps,
       r.profit_pct
ORDER BY r.spread_bps DESC
LIMIT 20;
```

**What you'll see:**
- Which exchange to buy from
- Which exchange to sell to
- Profit potential in basis points

---

### **5. Asset Correlation Network**

```cypher
// See which assets move together
MATCH (a1:Asset)-[r:CORRELATES_WITH]->(a2:Asset)
WHERE r.correlation > 0.7
RETURN a1.symbol,
       a2.symbol,
       r.correlation,
       r.strength,
       r.market_regime
ORDER BY r.correlation DESC;
```

**What you'll see:**
- BTC ←→ ETH correlation
- BTC ←→ SOL correlation
- Market regime classification

---

### **6. Sentiment Divergences**

```cypher
// When sentiment and price disagree
MATCH (post:RedditPost)-[r:DIVERGES_FROM]->(pm:PriceMovement)
WHERE r.divergence_strength > 0.5
RETURN post.text_preview,
       post.sentiment_polarity,
       pm.symbol,
       pm.price_change_pct,
       r.divergence_type,
       r.divergence_strength
ORDER BY r.divergence_strength DESC
LIMIT 20;
```

**What you'll see:**
- Reddit posts with positive sentiment
- But prices went down (or vice versa)
- Divergence strength metric

---

## 🎯 **Advanced Queries**

### **7. Multi-Hop: Reddit → Price → Exchange Arbitrage**

```cypher
// Complete trading signal chain
MATCH (post:RedditPost)-[:LEADS_TO]->(pm:PriceMovement)
MATCH (pm)<-[:LISTS]-(ex1:Exchange)-[arb:ARBITRAGE]->(ex2:Exchange)
WHERE post.sentiment_polarity > 0.2
  AND arb.spread_bps > 10
RETURN post.text_preview,
       post.sentiment_polarity,
       pm.symbol,
       pm.price_change_pct,
       ex1.name as buy_from,
       ex2.name as sell_to,
       arb.profit_pct
LIMIT 10;
```

---

### **8. Time-Based Lead-Lag Analysis**

```cypher
// See how lead-lag timing affects prediction accuracy
MATCH (post:RedditPost)-[r:LEADS_TO]->(pm:PriceMovement)
WITH r.lag_hours as lag,
     count(CASE WHEN r.direction_match = true THEN 1 END) as correct,
     count(*) as total
RETURN lag as hours_ahead,
       correct,
       total,
       round(100.0 * correct / total, 2) as accuracy_pct
ORDER BY lag;
```

**What you'll see:**
- 1-hour lag: 45% accuracy
- 4-hour lag: 68% accuracy (best!)
- 8-hour lag: 52% accuracy

---

### **9. Subreddit Influence Network**

```cypher
// Which subreddits influence which assets most?
MATCH (post:RedditPost)-[r:LEADS_TO]->(pm:PriceMovement)
WHERE r.direction_match = true
WITH post.subreddit as subreddit,
     pm.symbol as symbol,
     count(*) as influence_count
RETURN subreddit,
       collect({symbol: symbol, count: influence_count}) as influences
ORDER BY influence_count DESC;
```

---

### **10. Full Context for a Specific Reddit Post**

```cypher
// See everything related to a specific Reddit post
MATCH (post:RedditPost {item_id: '1mddode'})
OPTIONAL MATCH (post)-[leads:LEADS_TO]->(pm:PriceMovement)
OPTIONAL MATCH (post)-[div:DIVERGES_FROM]->(pm2:PriceMovement)
RETURN post.text_preview as post_text,
       post.sentiment_polarity as sentiment,
       post.timestamp as posted_at,
       collect(DISTINCT {
           type: 'LEADS_TO',
           symbol: pm.symbol,
           lag_hours: leads.lag_hours,
           price_change: pm.price_change_pct,
           matched: leads.direction_match
       }) as lead_relationships,
       collect(DISTINCT {
           type: 'DIVERGENCE',
           symbol: pm2.symbol,
           divergence_type: div.divergence_type,
           strength: div.divergence_strength
       }) as divergence_relationships;
```

**What you'll see:**
- The full post text
- All price movements it led to
- Timing and accuracy of predictions
- Any divergences detected

---

## 📈 **Graph Statistics**

### **Count Everything**

```cypher
// Overview of your graph
MATCH (n)
RETURN labels(n)[0] as node_type,
       count(n) as count
ORDER BY count DESC;
```

### **Relationship Counts**

```cypher
// See how many relationships of each type
MATCH ()-[r]->()
RETURN type(r) as relationship_type,
       count(r) as count
ORDER BY count DESC;
```

---

## 🎨 **Neo4j Browser Visualization**

### **Open Neo4j Browser:**
```bash
# In your browser, go to:
http://localhost:7474

# Login with:
Username: neo4j
Password: password
```

### **Best Visualizations:**

1. **Sentiment Network:**
   ```cypher
   MATCH path = (post:RedditPost)-[:LEADS_TO]->(pm:PriceMovement)
   WHERE r.lag_hours = 4
   RETURN path LIMIT 100
   ```

2. **Arbitrage Network:**
   ```cypher
   MATCH path = (ex1:Exchange)-[:ARBITRAGE]->(ex2:Exchange)
   RETURN path
   ```

3. **Asset Correlation:**
   ```cypher
   MATCH path = (a1:Asset)-[:CORRELATES_WITH]->(a2:Asset)
   WHERE r.correlation > 0.6
   RETURN path
   ```

---

## 🔑 **Key Insights You Can Get**

### **From Your Data:**

1. **"Which Reddit post predicted BTC's 5% rise?"**
   - Query shows exact post, timing, sentiment
   - Can trace back to specific user/subreddit

2. **"Does r/BitcoinKR lead global prices by 4 hours?"**
   - Lead-lag analysis shows strongest correlations
   - Can see accuracy percentage

3. **"Which exchange pairs have consistent arbitrage?"**
   - Arbitrage graph shows recurring patterns
   - Can build automated trading strategies

4. **"When do people get bullish but prices drop?"**
   - Divergence queries show these contradictions
   - Often precedes trend reversals

5. **"Which assets always move together?"**
   - Correlation network shows clustering
   - Helps with portfolio diversification

---

## 📝 **Example Output**

### **Query: Which Reddit posts led to BTC price changes?**

```
╔═══════════════════════════════════════════════════════════════════════════╗
║ post_text                          sentiment  lag  symbol  price_change   ║
╠═══════════════════════════════════════════════════════════════════════════╣
║ "Introducing Away From Keyboard... 0.247      4h   BTC     +2.5%         ║
║ "Bitcoin to the moon! 🚀..."       0.652      2h   BTC     +3.2%         ║
║ "Major correction incoming..."     -0.428     4h   BTC     -1.8%         ║
╚═══════════════════════════════════════════════════════════════════════════╝
```

You can now:
- Click on the post to see full text
- See which subreddit it came from
- Check if sentiment matched price direction
- Find optimal lead-lag timing

---

## 🚀 **Next Steps**

1. **Run the pipeline:** `python spark_analytics.py`
2. **Open Neo4j Browser:** http://localhost:7474
3. **Run these queries** to explore your data
4. **Build visualizations** to spot patterns
5. **Export insights** for trading strategies

Your data is now a **connected graph**, not just flat tables!
