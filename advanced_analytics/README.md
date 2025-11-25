# Advanced Analytics Module

Deep cross-source semantic analysis using PySpark, PageRank, and Granger causality for financial text-price relationships.

## Overview

This module extends the base analytics with Google-inspired algorithms and advanced NLP techniques to understand:
- **Which words influence markets** (PageRank)
- **Causality direction** (Do words predict prices or vice versa?)
- **Language register differences** (Formal news vs informal social media)

## Features

### 1. PageRank for Word Importance

Applies Google's PageRank algorithm to a word co-occurrence graph:

```python
class WordPageRank:
    def build_word_graph(self):
        # Words that appear together form edges
        # Edge weight = co-occurrence frequency
    
    def calculate_pagerank(self, damping=0.85, iterations=20):
        # Iterative PageRank: PR(w) = (1-d)/N + d * sum(PR(v)/L(v))
```

**Why PageRank for words?**
- Words that connect many concepts are "hubs"
- Important financial terms link different topics
- Better than raw frequency for finding influential terms

### 2. Bidirectional Granger Causality

Tests both directions of word-price causality:

```
Word -> Price: Does increased mention of "inflation" predict price drops?
Price -> Word: Do price changes drive news coverage?
```

Implementation uses lagged correlations:
```python
class BidirectionalCausalityAnalysis:
    def analyze(self, word, price_col, max_lag=10):
        # Test: word_t correlates with price_t+lag
        # Test: price_t correlates with word_t+lag
```

### 3. Formal vs Informal Language Comparison

Compares news headlines with social media (YouTube/Reddit):

| Metric | News | Social Media |
|--------|------|--------------|
| Avg word length | 6.2 | 4.8 |
| Formality score | 0.78 | 0.34 |
| Top words | market, economy, growth | stock, moon, rocket |

### 4. Word Co-occurrence Network

Graph visualization of semantic relationships:
- Nodes = words
- Edges = co-occurrence
- Node size = PageRank score
- Clusters = semantic topics

## Running the Analysis

```bash
cd advanced_analytics

# Run the Spark analysis
python advanced_spark_cross_analysis.py

# Launch the dashboard
streamlit run advanced_dashboard.py --server.port 8503
```

## Output Files

```
advanced_output/
├── pagerank_scores.csv         # Word importance rankings
├── word_cooccurrence.csv       # Word pair frequencies
├── causality_results.csv       # Granger test results
├── formal_informal_comparison/ # Language register analysis
└── social_media_generated/     # Synthetic social data
```

---

# How to Strengthen This Module

## Current Limitations

1. **Synthetic social media data** - Generated, not real
2. **Simple causality** - Correlation-based, not true Granger
3. **Limited word embeddings** - Using co-occurrence, not transformers
4. **Single language** - English only

## Roadmap for Enhancement

### Phase 1: Real Data Sources

**Goal**: Replace synthetic data with real sources

| Data Source | API/Method | Records |
|-------------|------------|---------|
| Twitter/X | Academic API | 1M+ tweets |
| Reddit | PRAW API | r/wallstreetbets, r/stocks |
| YouTube | Data API v3 | Comments on financial videos |
| StockTwits | REST API | Real-time sentiment |
| SEC Filings | EDGAR | 10-K, 8-K filings |

```python
# Example: Reddit integration
import praw

reddit = praw.Reddit(client_id='...', client_secret='...')
for submission in reddit.subreddit('wallstreetbets').hot(limit=1000):
    comments = submission.comments.list()
```

### Phase 2: True Granger Causality

**Goal**: Statistical significance testing

```python
from statsmodels.tsa.stattools import grangercausalitytests

def test_granger_causality(word_series, price_series, max_lag=5):
    data = pd.DataFrame({'word': word_series, 'price': price_series})
    results = grangercausalitytests(data, maxlag=max_lag)
    return results  # Returns F-test and p-values
```

Key improvements:
- Stationarity testing (ADF test)
- VAR model for multivariate causality
- Impulse response functions

### Phase 3: Word Embeddings

**Goal**: Semantic similarity using transformers

```python
from sentence_transformers import SentenceTransformer

model = SentenceTransformer('all-MiniLM-L6-v2')

def get_semantic_clusters(headlines):
    embeddings = model.encode(headlines)
    # Cluster with K-means or HDBSCAN
    clusters = HDBSCAN().fit(embeddings)
    return clusters
```

Benefits:
- "Market crash" and "stock plunge" recognized as similar
- Topic modeling without manual keywords
- Transfer learning from financial corpus

### Phase 4: Real-Time Processing

**Goal**: Live analysis pipeline

```
Kafka -> Spark Streaming -> Feature Store -> Dashboard
            |
            v
      Real-time NLP
      (Flair/spaCy)
```

Components:
- Kafka topics for each data source
- Spark Structured Streaming
- Redis for feature caching
- WebSocket dashboard updates

### Phase 5: Advanced Graph Analytics

**Goal**: Beyond PageRank

| Algorithm | Use Case |
|-----------|----------|
| Node2Vec | Word embeddings from graph structure |
| Community Detection | Find topic clusters |
| Temporal PageRank | How word importance changes over time |
| Knowledge Graphs | Entity relationships (COMPANY -> CEO -> STATEMENT) |

```python
from node2vec import Node2Vec

# Generate embeddings from word graph
node2vec = Node2Vec(G, dimensions=64, walk_length=30)
model = node2vec.fit(window=10, min_count=1)
word_vectors = {word: model.wv[word] for word in G.nodes()}
```

### Phase 6: Multi-Modal Analysis

**Goal**: Images + Text + Audio

- Financial charts as images (CNN feature extraction)
- YouTube video transcripts
- Earnings call audio sentiment

### Phase 7: Explainable AI

**Goal**: Understand model decisions

```python
import shap

# Explain word importance
explainer = shap.TreeExplainer(price_predictor)
shap_values = explainer.shap_values(word_features)
shap.summary_plot(shap_values, word_features)
```

## Implementation Priority

| Priority | Feature | Effort | Impact |
|----------|---------|--------|--------|
| HIGH | Real Reddit/Twitter data | Medium | High |
| HIGH | True Granger tests | Low | High |
| MEDIUM | Sentence embeddings | Medium | High |
| MEDIUM | Spark Streaming | High | Medium |
| LOW | Knowledge graphs | High | Medium |
| LOW | Multi-modal | Very High | Medium |

## Quick Wins

1. **Add SEC filings** - Free, high-quality, directly impacts prices
2. **StockTwits integration** - Purpose-built for finance
3. **Proper statistical tests** - Use statsmodels for p-values
4. **FinBERT embeddings** - Pre-trained on financial text

## Resources

- [FinBERT](https://github.com/yya518/FinBERT) - Financial NLP model
- [SEC EDGAR](https://www.sec.gov/edgar) - Corporate filings
- [WRDS](https://wrds-www.wharton.upenn.edu/) - Academic financial data
- [Granger Causality Tutorial](https://www.statsmodels.org/stable/generated/statsmodels.tsa.stattools.grangercausalitytests.html)

## Author

**Mars** (gogog01-29-2021)

Part of the Finance Data Semantics project - November 2024
