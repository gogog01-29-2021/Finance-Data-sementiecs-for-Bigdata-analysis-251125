# Enhanced Semantic Analytics Pipeline - Setup Guide

## What's New? 🚀

This upgrade transforms your pipeline from **syntactic** to **semantic** data analysis:

### Before (Syntactic):
- ❌ Basic TextBlob sentiment scores
- ❌ Overall sentiment only
- ❌ No entity recognition
- ❌ No on-chain behavior data
- ❌ Limited insights

### After (Semantic):
- ✅ **Entity-specific sentiment** (sentiment per BTC, ETH, etc.)
- ✅ **Topic detection** (bullish, bearish, regulation, technical_analysis)
- ✅ **Twitter/X data** (more real-time than Reddit)
- ✅ **On-chain data** (whale movements, exchange flows - TRUE behavior data)
- ✅ **Divergence detection** (when sentiment ≠ on-chain behavior) ⚡ KEY INSIGHT
- ✅ **Influence weighting** (weight by user followers/influence)
- ✅ **Comprehensive visualizations**

## Quick Start

### 1. Install Dependencies

```bash
# Install enhanced requirements
pip install -r requirements_enhanced.txt

# Download TextBlob corpora
python -m textblob.download_corpora
```

### 2. Ensure Services Are Running

**Required:**
- MongoDB (port 27017)
- QuestDB (port 9000, 8812)

**Check:**
```bash
# Check MongoDB
docker ps | findstr mongodb

# Check QuestDB
docker ps | findstr questdb
```

**Start if needed:**
```bash
# MongoDB
docker run -d -p 27017:27017 --name mongodb mongo:latest

# QuestDB
docker run -d -p 9000:9000 -p 8812:8812 --name questdb questdb/questdb
```

### 3. Run the Enhanced Pipeline

**Quick Test (30 seconds):**
```bash
python run_enhanced_pipeline.py --quick
```

**Full Run (60 seconds):**
```bash
python run_enhanced_pipeline.py
```

**Extended Run (5 minutes):**
```bash
python run_enhanced_pipeline.py --duration 300
```

## What It Does

### Phase 1: Data Collection (Parallel)

Runs for specified duration, collecting:

1. **Reddit Sentiment (Enhanced)**
   - Entity extraction ($BTC, $ETH mentions)
   - Topic detection (bullish, bearish, regulation, etc.)
   - Urgency scoring
   - Price prediction detection

2. **Twitter/X Sentiment**
   - Real-time crypto tweets (simulated for demo)
   - Influence scoring based on followers
   - Engagement estimation

3. **On-Chain Data**
   - Whale transactions (>100 BTC, >1000 ETH)
   - Exchange flows (accumulation vs distribution)
   - Network metrics

### Phase 2: Semantic Analytics

Analyzes collected data to extract semantic insights:

1. **Entity-Specific Sentiment**
   - Sentiment per crypto (not just overall)
   - Compare Reddit vs Twitter sentiment
   - Track urgency by crypto

2. **Topic-Price Correlation**
   - Which topics are trending per crypto?
   - Bullish/bearish ratio by symbol
   - Topic distribution analysis

3. **Sentiment vs On-Chain Divergence** ⚡ KEY INSIGHT
   - Detects when people are bullish but whales are selling
   - Detects when people are bearish but whales are buying
   - This is TRUE semantic data: comparing TALK vs BEHAVIOR

4. **Influence-Weighted Sentiment**
   - Weights sentiment by user influence
   - Shows how sentiment changes when weighted
   - Identifies influential voices

### Phase 3: Visualization

Generates 5 comprehensive visualizations:

1. **Summary Dashboard** - Complete overview
2. **Entity Sentiment Analysis** - Sentiment by crypto + source
3. **Topic Correlation** - Topics heatmap and distribution
4. **Sentiment-OnChain Divergence** - KEY INSIGHT visualization
5. **Influence-Weighted Sentiment** - Impact of weighting

## Viewing Results

### Visualizations

```bash
# Open visualizations folder
cd visualizations

# View files:
0_summary_dashboard.png           # Complete overview
1_entity_sentiment.png            # Per-crypto sentiment
2_topic_correlation.png           # Topic analysis
3_sentiment_onchain_divergence.png # Divergences (KEY!)
4_influence_weighted.png          # Influence impact
```

### MongoDB Data

```bash
# Connect to MongoDB
docker exec -it mongodb mongosh financial_analytics

# View analytics types
db.enhanced_semantic_analytics.distinct("analytics_type")

# View entity sentiment
db.enhanced_semantic_analytics.find({analytics_type: "entity_sentiment"}).limit(5)

# View divergences (KEY INSIGHTS!)
db.enhanced_semantic_analytics.find({
  analytics_type: "sentiment_onchain_divergence",
  divergence_type: "BEARISH_DIVERGENCE"
})

# Count by analytics type
db.enhanced_semantic_analytics.aggregate([
  {$group: {_id: "$analytics_type", count: {$sum: 1}}}
])
```

## Understanding the KEY Insight: Divergence Detection

### What is Divergence?

**Divergence** = When social sentiment doesn't match on-chain behavior

### Example 1: BEARISH DIVERGENCE
```
Social: "BTC to the moon! 🚀" (sentiment: +0.8)
On-chain: Whales transferring BTC to exchanges (distribution/selling)

→ BEARISH DIVERGENCE: People are bullish but whales are selling
→ Signal: Possible price drop incoming
```

### Example 2: BULLISH DIVERGENCE
```
Social: "BTC is crashing, sell everything!" (sentiment: -0.7)
On-chain: Whales withdrawing BTC from exchanges (accumulation/buying)

→ BULLISH DIVERGENCE: People are bearish but whales are buying
→ Signal: Possible price pump incoming
```

### Why This Matters

This is **TRUE SEMANTIC DATA**:
- Not just analyzing words (syntactic)
- Comparing TALK vs BEHAVIOR (semantic)
- Whales often know more than retail traders
- Divergences can be predictive signals

## Comparing Old vs New

### Old Analytics (Syntactic)

```python
# Overall sentiment only
"market sentiment: 0.65"  # What does this mean?

# Word frequency
"moon appeared 47 times"  # Which crypto?

# Basic correlation
"sentiment correlates with price"  # Why? For all cryptos?
```

### New Analytics (Semantic)

```python
# Entity-specific
{
  "symbol": "BTC",
  "sentiment": 0.8,
  "reddit_sentiment": 0.7,
  "twitter_sentiment": 0.9,
  "urgency": 0.6,
  "topics": ["bullish", "breakout"]
}

# Divergence detection
{
  "symbol": "BTC",
  "sentiment": 0.8,  # Bullish talk
  "distribution_count": 15,  # But whales are selling!
  "divergence_type": "BEARISH_DIVERGENCE"
}

# Influence-weighted
{
  "symbol": "ETH",
  "raw_sentiment": 0.3,  # Regular users: slightly positive
  "weighted_sentiment": 0.7,  # Influencers: very positive!
  "adjustment": +0.4  # Big difference!
}
```

## Troubleshooting

### MongoDB Connection Error

```bash
# Check MongoDB is running
docker ps | findstr mongodb

# Start if needed
docker start mongodb

# Or fresh start
docker run -d -p 27017:27017 --name mongodb mongo:latest
```

### QuestDB Connection Error

```bash
# Check QuestDB
docker ps | findstr questdb

# Start if needed
docker start questdb
```

### No Data Collected

- Ensure Reddit/Twitter collectors ran for sufficient time (60+ seconds)
- Check data directories: `data/reddit/`, `data/twitter/`, `data/onchain/`
- Run with longer duration: `python run_enhanced_pipeline.py --duration 120`

### Visualization Errors

```bash
# Install matplotlib/seaborn
pip install matplotlib seaborn

# If still failing, check MongoDB has data
docker exec -it mongodb mongosh --eval "db.getSiblingDB('financial_analytics').enhanced_semantic_analytics.count()"
```

## Production Deployment

For production use, replace simulated data sources:

### Twitter/X
- Option 1: Twitter API v2 ($100/month basic tier)
- Option 2: snscrape library (free but rate-limited)
- Option 3: nitter.net scraping (free)

### On-Chain Data
- **BTC**: Blockchain.info API, Blockchair API
- **ETH**: Etherscan API, Alchemy, Infura
- **Whale Alerts**: Whale Alert API (whale-alert.io)

### Reddit
- Current implementation uses public JSON (works well)
- For higher rate limits: Reddit API with credentials

## Next Steps

1. **Run the pipeline** with longer duration for more data
2. **Examine visualizations** in `visualizations/` folder
3. **Query MongoDB** for specific insights
4. **Look for divergences** - these are key trading signals
5. **Compare entity-specific sentiment** - different cryptos have different sentiment
6. **Check influence-weighted sentiment** - see if influencers differ from retail

## Files Created

**Data Collectors:**
- `reddit_sentiment.py` (enhanced with entities/topics)
- `twitter_sentiment.py` (new)
- `onchain_data.py` (new)

**Analytics:**
- `spark_enhanced_semantic_analytics.py` (new semantic analytics)

**Visualization:**
- `visualize_semantic_analytics.py` (comprehensive visualizations)

**Master Script:**
- `run_enhanced_pipeline.py` (runs everything)

**Config:**
- `requirements_enhanced.txt` (new dependencies)
- `ENHANCED_SETUP_GUIDE.md` (this file)

## Questions?

**Why am I getting 0 divergences?**
- Need more data collection time (try 5+ minutes)
- Simulated on-chain data may not have enough variance
- In production with real APIs, divergences are more common

**Why do all visualizations show similar data?**
- Short collection time means limited data diversity
- Try longer collection: `--duration 300` (5 minutes)
- In production with real data feeds, much more variance

**How do I see the difference from the old approach?**
- Old: `spark_analytics.py` - basic sentiment scores
- New: `spark_enhanced_semantic_analytics.py` - entity-specific, topics, divergences
- Compare MongoDB collections: `analytics_unified` vs `enhanced_semantic_analytics`

## Summary

You now have:
- ✅ Entity-specific sentiment (not just overall)
- ✅ Topic detection and correlation
- ✅ Multi-source data (Reddit + Twitter + On-chain)
- ✅ Divergence detection (talk vs behavior)
- ✅ Influence weighting
- ✅ Comprehensive visualizations

This is **TRUE SEMANTIC DATA** - understanding MEANING and BEHAVIOR, not just syntactic word patterns!
