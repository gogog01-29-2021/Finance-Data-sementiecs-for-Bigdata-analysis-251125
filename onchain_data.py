#!/usr/bin/env python3
"""
ON-CHAIN DATA COLLECTOR
Collects blockchain data: whale movements, exchange flows, network metrics
This is TRUE semantic data - actual behavior, not just sentiment
"""

import asyncio
import json
import time
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional
import aiohttp
from dotenv import load_dotenv
import lmdb

load_dotenv()

# Configuration
ONCHAIN_POLL_INTERVAL = int(os.getenv("ONCHAIN_POLL_INTERVAL", "600"))  # 10 minutes
DATA_DIR = Path("data/onchain")
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Whale transaction threshold (BTC)
WHALE_THRESHOLD_BTC = 100  # 100+ BTC = whale
WHALE_THRESHOLD_ETH = 1000  # 1000+ ETH = whale

# Known exchange addresses (partial list for demo)
EXCHANGE_ADDRESSES = {
    "binance": ["bc1qm34lsc65zpw79lxes69zkqmk6ee3ewf0j77s3h", "0x28C6c06298d514Db089934071355E5743bf21d60"],
    "coinbase": ["bc1qgdjqv0av3q56jvd82tkdjpy7gdp9ut8tlqmgrpmv24sq90ecnvqqjwvw97", "0x71660c4005BA85c37ccec55d0C4493E66Fe775d3"],
    "kraken": ["bc1qj89046x7zv6pm4n00qgqp505nvljnfp6xfznyw", "0x2910543Af39abA0Cd09dBb2D50200b3E800A63D2"],
}


class OnChainAnalyzer:
    """Analyze on-chain transactions for semantic signals"""

    @staticmethod
    def analyze_transaction(tx: Dict) -> Dict:
        """
        Analyze a blockchain transaction for semantic meaning

        Returns semantic features:
        - whale_activity: bool (large transaction)
        - flow_direction: "to_exchange" | "from_exchange" | "peer_to_peer"
        - signal: "accumulation" | "distribution" | "transfer"
        - urgency: float (based on fee)
        - network_activity: float
        """

        amount = tx.get('amount', 0)
        from_addr = tx.get('from', '')
        to_addr = tx.get('to', '')
        fee = tx.get('fee', 0)
        asset = tx.get('asset', 'BTC')

        # Determine if whale transaction
        is_whale = False
        if asset == 'BTC' and amount >= WHALE_THRESHOLD_BTC:
            is_whale = True
        elif asset == 'ETH' and amount >= WHALE_THRESHOLD_ETH:
            is_whale = True

        # Determine flow direction
        from_exchange = OnChainAnalyzer._is_exchange_address(from_addr)
        to_exchange = OnChainAnalyzer._is_exchange_address(to_addr)

        if to_exchange and not from_exchange:
            flow_direction = "to_exchange"
            signal = "distribution"  # Likely selling
        elif from_exchange and not to_exchange:
            flow_direction = "from_exchange"
            signal = "accumulation"  # Likely buying/withdrawing
        else:
            flow_direction = "peer_to_peer"
            signal = "transfer"

        # Urgency based on fee (higher fee = more urgent)
        urgency = min(fee / 0.001, 1.0)  # Normalize to 0-1

        return {
            "is_whale": is_whale,
            "flow_direction": flow_direction,
            "signal": signal,
            "urgency": urgency,
            "amount": amount,
            "asset": asset,
            "confidence": 0.8 if is_whale else 0.5
        }

    @staticmethod
    def _is_exchange_address(address: str) -> bool:
        """Check if address belongs to a known exchange"""
        for exchange, addresses in EXCHANGE_ADDRESSES.items():
            if address in addresses:
                return True
        return False


class OnChainCollector:
    """Collect on-chain data from multiple blockchains"""

    def __init__(self, db_path: Path):
        self.db_path = str(db_path)
        self.env = None
        self.session = None
        self.analyzer = OnChainAnalyzer()
        self.seen_txs = set()

    def open_db(self):
        """Open LMDB database"""
        self.env = lmdb.open(self.db_path, map_size=10485760000)  # 10GB
        print(f"[lmdb] On-chain database opened at {self.db_path}")

    def close_db(self):
        """Close database"""
        if self.env:
            self.env.close()
        print("[lmdb] On-chain database closed")

    async def start(self):
        """Start on-chain collection loop"""
        self.session = aiohttp.ClientSession(
            headers={"User-Agent": "CryptoAnalytics/1.0"}
        )

        print("=" * 80)
        print("ON-CHAIN DATA COLLECTOR STARTED")
        print("=" * 80)
        print(f"Monitoring: BTC, ETH whale transactions")
        print(f"Whale Threshold: {WHALE_THRESHOLD_BTC} BTC, {WHALE_THRESHOLD_ETH} ETH")
        print(f"Poll Interval: {ONCHAIN_POLL_INTERVAL}s")
        print(f"Database Path: {self.db_path}")
        print("=" * 80)
        print("\nNOTE: Using simulated on-chain data for demo purposes")
        print("For production: integrate Blockchain.info, Etherscan, Glassnode APIs")
        print("=" * 80)

        try:
            while True:
                await self._collect_all()
                await asyncio.sleep(ONCHAIN_POLL_INTERVAL)
        finally:
            await self.session.close()

    async def _collect_all(self):
        """Collect on-chain data"""
        print(f"\n[onchain] Collecting data at {datetime.now().strftime('%H:%M:%S')}...")

        # Collect BTC transactions
        btc_txs = await self._collect_btc_transactions()

        # Collect ETH transactions
        eth_txs = await self._collect_eth_transactions()

        # Collect network metrics
        network_metrics = await self._collect_network_metrics()

        total = len(btc_txs) + len(eth_txs)
        print(f"[onchain] Collected {total} transactions ({len(btc_txs)} BTC, {len(eth_txs)} ETH)")
        print(f"[onchain] Network metrics: {network_metrics}")

    async def _collect_btc_transactions(self) -> List[Dict]:
        """
        Collect Bitcoin whale transactions

        In production, use:
        - Blockchain.info API: https://www.blockchain.com/api
        - Blockchair API: https://api.blockchair.com/bitcoin/
        - Whale Alert API: https://whale-alert.io/
        """

        # Simulated data for demo
        simulated_txs = self._generate_simulated_btc_txs()

        new_txs = []
        for tx in simulated_txs:
            tx_id = tx['hash']

            if tx_id in self.seen_txs:
                continue

            self.seen_txs.add(tx_id)

            # Analyze transaction
            analysis = self.analyzer.analyze_transaction(tx)

            # Create record
            record = {
                "tx_hash": tx_id,
                "asset": "BTC",
                "amount": tx['amount'],
                "from": tx['from'],
                "to": tx['to'],
                "fee": tx['fee'],
                "timestamp": time.time(),
                "analysis": analysis,
                "source": "onchain"
            }

            # Store in LMDB
            self._store_transaction(tx_id, record)
            new_txs.append(record)

            # Log important whale transactions
            if analysis['is_whale']:
                print(f"  [WHALE] {analysis['amount']:.2f} BTC - {analysis['signal']} ({analysis['flow_direction']})")

        return new_txs

    async def _collect_eth_transactions(self) -> List[Dict]:
        """
        Collect Ethereum whale transactions

        In production, use:
        - Etherscan API: https://api.etherscan.io/
        - Infura: https://infura.io/
        - Alchemy: https://www.alchemy.com/
        """

        # Simulated data for demo
        simulated_txs = self._generate_simulated_eth_txs()

        new_txs = []
        for tx in simulated_txs:
            tx_id = tx['hash']

            if tx_id in self.seen_txs:
                continue

            self.seen_txs.add(tx_id)

            # Analyze transaction
            analysis = self.analyzer.analyze_transaction(tx)

            # Create record
            record = {
                "tx_hash": tx_id,
                "asset": "ETH",
                "amount": tx['amount'],
                "from": tx['from'],
                "to": tx['to'],
                "fee": tx['fee'],
                "timestamp": time.time(),
                "analysis": analysis,
                "source": "onchain"
            }

            # Store in LMDB
            self._store_transaction(tx_id, record)
            new_txs.append(record)

            if analysis['is_whale']:
                print(f"  [WHALE] {analysis['amount']:.2f} ETH - {analysis['signal']} ({analysis['flow_direction']})")

        return new_txs

    async def _collect_network_metrics(self) -> Dict:
        """
        Collect network-wide metrics

        Metrics:
        - Active addresses
        - Transaction volume
        - Hash rate (BTC)
        - Gas price (ETH)
        """

        # Simulated metrics for demo
        import random

        metrics = {
            "btc_active_addresses": random.randint(800000, 1200000),
            "btc_tx_volume": random.uniform(200000, 400000),
            "eth_active_addresses": random.randint(400000, 600000),
            "eth_gas_price": random.uniform(20, 100),
            "timestamp": time.time()
        }

        # Store metrics
        self._store_metrics(metrics)

        return metrics

    def _generate_simulated_btc_txs(self) -> List[Dict]:
        """Generate simulated BTC transactions for demo"""
        import random

        templates = [
            {
                "hash": f"btc_tx_{int(time.time())}_1",
                "amount": random.uniform(150, 500),  # Whale amount
                "from": "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",  # Random address
                "to": "bc1qm34lsc65zpw79lxes69zkqmk6ee3ewf0j77s3h",  # Binance
                "fee": 0.0005
            },
            {
                "hash": f"btc_tx_{int(time.time())}_2",
                "amount": random.uniform(200, 600),
                "from": "bc1qgdjqv0av3q56jvd82tkdjpy7gdp9ut8tlqmgrpmv24sq90ecnvqqjwvw97",  # Coinbase
                "to": "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",  # Random
                "fee": 0.001
            }
        ]

        return templates[:1]  # Return 1 transaction per poll

    def _generate_simulated_eth_txs(self) -> List[Dict]:
        """Generate simulated ETH transactions for demo"""
        import random

        templates = [
            {
                "hash": f"eth_tx_{int(time.time())}_1",
                "amount": random.uniform(1500, 3000),  # Whale amount
                "from": "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb",
                "to": "0x28C6c06298d514Db089934071355E5743bf21d60",  # Binance
                "fee": 0.005
            },
            {
                "hash": f"eth_tx_{int(time.time())}_2",
                "amount": random.uniform(2000, 4000),
                "from": "0x71660c4005BA85c37ccec55d0C4493E66Fe775d3",  # Coinbase
                "to": "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb",
                "fee": 0.008
            }
        ]

        return templates[:1]  # Return 1 transaction per poll

    def _store_transaction(self, tx_id: str, data: Dict):
        """Store transaction in LMDB"""
        with self.env.begin(write=True) as txn:
            key = f"tx:{tx_id}".encode()
            value = json.dumps(data).encode()
            txn.put(key, value)

    def _store_metrics(self, metrics: Dict):
        """Store network metrics in LMDB"""
        with self.env.begin(write=True) as txn:
            key = f"metrics:{int(time.time())}".encode()
            value = json.dumps(metrics).encode()
            txn.put(key, value)


async def main():
    # Initialize LMDB database
    db_path = DATA_DIR / "onchain_data.db"
    collector = OnChainCollector(db_path)
    collector.open_db()

    try:
        await collector.start()
    except KeyboardInterrupt:
        print("\n[onchain] Collector stopped by user")
    finally:
        collector.close_db()


if __name__ == "__main__":
    asyncio.run(main())
