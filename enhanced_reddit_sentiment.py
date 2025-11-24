#!/usr/bin/env python3
"""
ENHANCED REDDIT SENTIMENT - with Named Entity Recognition
Quick upgrade to extract semantic features from Reddit data
"""

import spacy
from textblob import TextBlob
import re

# Load spaCy model (run: python -m spacy download en_core_web_sm)
try:
    nlp = spacy.load("en_core_web_sm")
except:
    print("Please install: python -m spacy download en_core_web_sm")
    nlp = None

class EnhancedSentimentAnalyzer:
    """Enhanced sentiment analyzer with entity extraction"""

    # Common crypto symbols to look for
    CRYPTO_SYMBOLS = {
        'bitcoin': 'BTC', 'btc': 'BTC',
        'ethereum': 'ETH', 'eth': 'ETH',
        'solana': 'SOL', 'sol': 'SOL',
        'ripple': 'XRP', 'xrp': 'XRP',
        'cardano': 'ADA', 'ada': 'ADA',
        'dogecoin': 'DOGE', 'doge': 'DOGE'
    }

    def analyze(self, text: str) -> dict:
        """
        Enhanced sentiment analysis with semantic features

        Returns:
        {
            'polarity': float,
            'subjectivity': float,
            'label': str,
            'entities': {'BTC': 3, 'ETH': 1},  # NEW: coin mentions
            'topics': ['bullish', 'breakout'],  # NEW: detected topics
            'has_price_prediction': bool,       # NEW: predictive language
            'urgency_score': float              # NEW: urgency/FOMO detection
        }
        """
        if not text:
            return self._empty_result()

        # Basic sentiment (existing)
        blob = TextBlob(text)
        polarity = blob.sentiment.polarity
        subjectivity = blob.sentiment.subjectivity

        label = "positive" if polarity > 0.1 else ("negative" if polarity < -0.1 else "neutral")

        # NEW: Extract crypto entity mentions
        entities = self._extract_crypto_entities(text)

        # NEW: Detect topics/themes
        topics = self._detect_topics(text)

        # NEW: Detect predictive language
        has_prediction = self._has_price_prediction(text)

        # NEW: Detect urgency/FOMO
        urgency = self._compute_urgency(text)

        return {
            "polarity": polarity,
            "subjectivity": subjectivity,
            "label": label,
            "entities": entities,              # NEW
            "entity_count": len(entities),     # NEW
            "topics": topics,                   # NEW
            "has_price_prediction": has_prediction,  # NEW
            "urgency_score": urgency           # NEW
        }

    def _extract_crypto_entities(self, text: str) -> dict:
        """Extract cryptocurrency mentions and count them"""
        text_lower = text.lower()
        entities = {}

        # Look for $SYMBOL pattern
        dollar_mentions = re.findall(r'\$([A-Z]{2,5})\b', text)
        for symbol in dollar_mentions:
            entities[symbol] = entities.get(symbol, 0) + 1

        # Look for coin names
        for coin_name, symbol in self.CRYPTO_SYMBOLS.items():
            # Count occurrences
            count = len(re.findall(r'\b' + coin_name + r'\b', text_lower))
            if count > 0:
                entities[symbol] = entities.get(symbol, 0) + count

        return entities

    def _detect_topics(self, text: str) -> list:
        """Detect crypto-related topics/themes"""
        topics = []
        text_lower = text.lower()

        # Bullish indicators
        if any(word in text_lower for word in ['moon', 'pump', 'bullish', 'up', 'breakout', 'rally']):
            topics.append('bullish')

        # Bearish indicators
        if any(word in text_lower for word in ['crash', 'dump', 'bearish', 'down', 'sell', 'drop']):
            topics.append('bearish')

        # Technical analysis
        if any(word in text_lower for word in ['support', 'resistance', 'pattern', 'chart', 'analysis']):
            topics.append('technical_analysis')

        # Regulation/news
        if any(word in text_lower for word in ['sec', 'regulation', 'ban', 'government', 'law']):
            topics.append('regulation')

        # HODL/long-term
        if any(word in text_lower for word in ['hodl', 'hold', 'accumulate', 'dca', 'long-term']):
            topics.append('long_term')

        # FOMO/urgency
        if any(word in text_lower for word in ['fomo', 'last chance', 'dont miss', 'hurry', 'now']):
            topics.append('fomo')

        return topics

    def _has_price_prediction(self, text: str) -> bool:
        """Detect if text contains price predictions"""
        text_lower = text.lower()

        # Look for predictive patterns
        predictive_words = [
            'will', 'going to', 'expect', 'predict', 'target',
            'could reach', 'might hit', 'heading to', 'next stop'
        ]

        # Look for price numbers (contains $ or numbers with k/K)
        has_price = bool(re.search(r'\$\d+|\d+k', text, re.IGNORECASE))

        return has_price and any(word in text_lower for word in predictive_words)

    def _compute_urgency(self, text: str) -> float:
        """Compute urgency/FOMO score (0-1)"""
        text_lower = text.lower()

        urgency_words = [
            ('!!!', 0.3),
            ('🚀', 0.2), ('💎', 0.15), ('🔥', 0.15),
            ('now', 0.1), ('urgent', 0.2), ('immediately', 0.2),
            ('fomo', 0.2), ('last chance', 0.3), ('dont miss', 0.25),
            ('breaking', 0.15), ('alert', 0.15)
        ]

        score = 0.0
        for word, weight in urgency_words:
            if word in text_lower:
                score += weight

        return min(score, 1.0)  # Cap at 1.0

    def _empty_result(self):
        """Return empty result structure"""
        return {
            "polarity": 0.0,
            "subjectivity": 0.0,
            "label": "neutral",
            "entities": {},
            "entity_count": 0,
            "topics": [],
            "has_price_prediction": False,
            "urgency_score": 0.0
        }


# Usage example
if __name__ == "__main__":
    analyzer = EnhancedSentimentAnalyzer()

    test_texts = [
        "Bitcoin is going to moon! 🚀 $BTC will hit $100k soon!",
        "Sold all my ETH, this crash is terrible",
        "HODL your BTC and ETH, don't panic sell",
        "SEC regulation might affect Ripple and XRP price"
    ]

    for text in test_texts:
        result = analyzer.analyze(text)
        print(f"\nText: {text}")
        print(f"Sentiment: {result['label']} ({result['polarity']:.2f})")
        print(f"Entities: {result['entities']}")
        print(f"Topics: {result['topics']}")
        print(f"Has prediction: {result['has_price_prediction']}")
        print(f"Urgency: {result['urgency_score']:.2f}")
