#!/usr/bin/env python3
"""
COMPLETE Fixed Unified Orderbook Streamer with Korean & Global Exchanges
Using the SAME QuestDB approach as the original working code
Includes ALL exchanges and DOGE support
"""

import asyncio
import json
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, Tuple, Optional, List
from datetime import datetime
import aiohttp
import websockets
from questdb.ingress import Sender, ServerTimestamp

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
OUTPUT_FILE = DATA_DIR / "orderbook_stream.jsonl"


# ================= Symbol Normalization =================
def split_symbol(exchange: str, venue_sym: str) -> Tuple[str, str, str]:
    """
    Returns (base, quote, normalized_symbol)
    
    Korean exchanges:
    - Upbit:    'KRW-BTC'      -> ('BTC', 'KRW', 'BTC-KRW')
    - Bithumb:  'BTC_KRW'      -> ('BTC', 'KRW', 'BTC-KRW')
    - Coinone:  'btc-krw'      -> ('BTC', 'KRW', 'BTC-KRW')
    - Korbit:   'btc_krw'      -> ('BTC', 'KRW', 'BTC-KRW')
    
    Global exchanges:
    - Binance:  'btcusdt'      -> ('BTC', 'USDT', 'BTC-USD')
    - OKX:      'BTC-USDT'     -> ('BTC', 'USDT', 'BTC-USD')
    - Bybit:    'BTCUSDT'      -> ('BTC', 'USDT', 'BTC-USD')
    """
    s = venue_sym.upper()
    
    # Korean exchanges
    if exchange == "upbit":
        parts = s.split("-")
        if len(parts) == 2:
            quote, base = parts  # KRW-BTC format
            return base, quote, f"{base}-{quote}"
    
    elif exchange == "bithumb":
        parts = s.split("_")
        if len(parts) == 2:
            base, quote = parts  # BTC_KRW format
            return base, quote, f"{base}-{quote}"
    
    elif exchange == "coinone":
        parts = s.split("-")
        if len(parts) == 2:
            base, quote = parts  # btc-krw format
            return base.upper(), quote.upper(), f"{base.upper()}-{quote.upper()}"
    
    elif exchange == "korbit":
        parts = s.split("_")
        if len(parts) == 2:
            base, quote = parts  # btc_krw format
            return base.upper(), quote.upper(), f"{base.upper()}-{quote.upper()}"
    
    # Global exchanges - normalize USDT to USD
    elif exchange == "binance":
        if s.endswith("USDT"):
            base = s[:-4]
            return base, "USDT", f"{base}-USD"
    
    elif exchange == "okx":
        if "-" in s:
            base, quote = s.split("-")
            if quote == "USDT":
                return base, quote, f"{base}-USD"
    
    elif exchange == "bybit":
        if s.endswith("USDT"):
            base = s[:-4]
            return base, "USDT", f"{base}-USD"
    
    # Fallback
    return venue_sym, "", venue_sym


def get_region(exchange: str, quote: str) -> str:
    """Determine if exchange/pair is Korean or Global"""
    kr_exchanges = {"upbit", "bithumb", "coinone", "korbit"}
    if exchange in kr_exchanges or quote == "KRW":
        return "KR"
    return "GLOBAL"


# ================= Common Orderbook =================
class LocalOrderBook:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.bids = defaultdict(float)
        self.asks = defaultdict(float)
        self.last_update_id = 0

    def upsert_side(self, side_dict, updates):
        for price, qty in updates:
            p, q = float(price), float(qty)
            if q == 0:
                side_dict.pop(p, None)
            else:
                side_dict[p] = q

    def snapshot(self, top_n=20):
        return {
            "symbol": self.symbol,
            "ts": time.time(),
            "bids": sorted(self.bids.items(), key=lambda x: -x[0])[:top_n],
            "asks": sorted(self.asks.items(), key=lambda x: x[0])[:top_n],
            "last_update_id": self.last_update_id,
        }


# ================= Base Streamer =================
class BaseStreamer:
    def __init__(self, exchange: str, symbol_map: dict, out_q: asyncio.Queue):
        self.exchange = exchange
        self.symbol_map = symbol_map  # unified -> venue symbol
        self.out_q = out_q
        self.books = {sym: LocalOrderBook(sym) for sym in symbol_map.keys()}

    async def run(self):
        raise NotImplementedError

    async def publish(self, *, symbol, event_ts, seq, depth, raw, venue_sym=None):
        ev = {
            "exchange": self.exchange,
            "symbol": symbol,
            "venue_symbol": venue_sym or self.symbol_map.get(symbol, symbol),
            "event_ts": event_ts,
            "recv_ts": time.time(),
            "seq": seq,
            "depth": depth,
            "raw": raw,
        }
        await self.out_q.put(ev)

    def _find_unified(self, venue: str):
        """Find unified symbol from venue symbol"""
        for unified, v in self.symbol_map.items():
            if v.upper() == venue.upper():
                return unified
        return None


# ================= Global Exchanges =================
class BinanceStreamer(BaseStreamer):
    BASE = "wss://fstream.binance.com/stream?streams="

    async def run(self):
        tasks = []
        for unified_sym, venue_sym in self.symbol_map.items():
            tasks.append(asyncio.create_task(self._one(unified_sym, venue_sym)))
        await asyncio.gather(*tasks)

    async def _one(self, unified_sym: str, venue_sym: str):
        url = f"{self.BASE}{venue_sym.lower()}@depth@100ms"
        lob = self.books[unified_sym]
        
        while True:
            try:
                async with websockets.connect(url, ping_interval=20) as ws:
                    print(f"[binance:{unified_sym}] connected")
                    async for msg in ws:
                        data = json.loads(msg)["data"]
                        
                        lob.upsert_side(lob.bids, data["b"])
                        lob.upsert_side(lob.asks, data["a"])
                        lob.last_update_id = data["u"]

                        await self.publish(
                            symbol=unified_sym,
                            venue_sym=venue_sym,
                            event_ts=data["E"] / 1000.0,
                            seq=data["u"],
                            depth=lob.snapshot(20),
                            raw=data,
                        )
            except Exception as e:
                print(f"[binance:{unified_sym}] error {e} → reconnect in 5s")
                await asyncio.sleep(5)


class OKXStreamer(BaseStreamer):
    WS = "wss://ws.okx.com:8443/ws/v5/public"

    async def run(self):
        while True:
            try:
                async with websockets.connect(self.WS, ping_interval=20) as ws:
                    sub_msg = {
                        "op": "subscribe",
                        "args": [
                            {"channel": "books5", "instId": venue}
                            for _, venue in self.symbol_map.items()
                        ],
                    }
                    await ws.send(json.dumps(sub_msg))
                    print("[okx] subscribed", list(self.symbol_map.values()))

                    async for raw in ws:
                        msg = json.loads(raw)
                        
                        if "event" in msg or msg.get("arg", {}).get("channel") != "books5":
                            continue

                        data_list = msg.get("data")
                        if not data_list:
                            continue

                        book_data = data_list[0]
                        venue = msg["arg"]["instId"]
                        unified = self._find_unified(venue)
                        
                        if not unified:
                            continue

                        lob = self.books[unified]
                        lob.bids = {float(p): float(sz) for p, sz, *_ in book_data["bids"]}
                        lob.asks = {float(p): float(sz) for p, sz, *_ in book_data["asks"]}
                        lob.last_update_id = int(book_data.get("seqId", 0))

                        await self.publish(
                            symbol=unified,
                            venue_sym=venue,
                            event_ts=float(book_data["ts"]) / 1000.0,
                            seq=lob.last_update_id,
                            depth=lob.snapshot(20),
                            raw=msg,
                        )

            except Exception as e:
                print(f"[okx] error {e} → reconnect in 5s")
                await asyncio.sleep(5)


class BybitStreamer(BaseStreamer):
    WS = "wss://stream.bybit.com/v5/public/spot"
    REST = "https://api.bybit.com/v5/market/orderbook"

    async def run(self):
        tasks = []
        for unified_sym, venue_sym in self.symbol_map.items():
            tasks.append(asyncio.create_task(self._one(unified_sym, venue_sym)))
        await asyncio.gather(*tasks)

    async def _one(self, unified_sym: str, venue_sym: str):
        lob = self.books[unified_sym]

        while True:
            try:
                # Get REST snapshot first
                async with aiohttp.ClientSession() as session:
                    params = {"category": "spot", "symbol": venue_sym, "limit": "200"}
                    async with session.get(self.REST, params=params) as resp:
                        snap = await resp.json()
                        if snap.get("retCode") == 0:
                            result = snap.get("result", {})
                            lob.bids = {float(p): float(q) for p, q in result.get("b", [])}
                            lob.asks = {float(p): float(q) for p, q in result.get("a", [])}
                            lob.last_update_id = int(result.get("u", int(time.time()*1000)))
                            print(f"[bybit:{unified_sym}] snapshot loaded")

                # Connect to WebSocket
                async with websockets.connect(self.WS, ping_interval=20) as ws:
                    sub_msg = {
                        "op": "subscribe",
                        "args": [f"orderbook.50.{venue_sym}"],
                    }
                    await ws.send(json.dumps(sub_msg))
                    print(f"[bybit:{unified_sym}] subscribed")

                    async for raw in ws:
                        msg = json.loads(raw)
                        
                        if msg.get("op") in ("ping", "pong") or "topic" not in msg:
                            continue
                        
                        if not msg["topic"].startswith("orderbook."):
                            continue

                        data = msg.get("data", {})
                        
                        # Update orderbook
                        for p, q in data.get("b", []):
                            lob.upsert_side(lob.bids, [(p, q)])
                        for p, q in data.get("a", []):
                            lob.upsert_side(lob.asks, [(p, q)])
                        
                        ts = float(data.get("t", time.time() * 1000))
                        lob.last_update_id = int(data.get("u", ts))

                        await self.publish(
                            symbol=unified_sym,
                            venue_sym=venue_sym,
                            event_ts=ts / 1000.0,
                            seq=lob.last_update_id,
                            depth=lob.snapshot(20),
                            raw=msg,
                        )

            except Exception as e:
                print(f"[bybit:{unified_sym}] error {e} → reconnect in 5s")
                await asyncio.sleep(5)


# ================= Korean Exchanges =================
class UpbitStreamer(BaseStreamer):
    WS = "wss://api.upbit.com/websocket/v1"

    async def run(self):
        syms = list(self.symbol_map.values())
        
        while True:
            try:
                async with websockets.connect(self.WS, ping_interval=20) as ws:
                    sub = [
                        {"ticket": "orderbook-stream"},
                        {"type": "orderbook", "codes": syms}
                    ]
                    await ws.send(json.dumps(sub))
                    print(f"[upbit] subscribed to {syms}")
                    
                    async for raw in ws:
                        # Upbit sends binary frames
                        if isinstance(raw, bytes):
                            msg = json.loads(raw.decode("utf-8"))
                        else:
                            msg = json.loads(raw)
                        
                        if msg.get("type") != "orderbook":
                            continue
                        
                        code = msg["code"]  # e.g. KRW-BTC
                        unified = self._find_unified(code)
                        if not unified:
                            continue
                        
                        lob = self.books[unified]
                        
                        # Upbit orderbook units contain both bid and ask data
                        units = msg.get("orderbook_units", [])
                        lob.bids = {float(u["bid_price"]): float(u["bid_size"]) 
                                   for u in units if u.get("bid_price")}
                        lob.asks = {float(u["ask_price"]): float(u["ask_size"]) 
                                   for u in units if u.get("ask_price")}
                        
                        seq = int(msg.get("timestamp", int(time.time()*1000)))
                        lob.last_update_id = seq
                        
                        await self.publish(
                            symbol=unified,
                            venue_sym=code,
                            event_ts=seq/1000.0,
                            seq=seq,
                            depth=lob.snapshot(20),
                            raw=msg
                        )
                        
            except Exception as e:
                print(f"[upbit] error {e} → reconnect in 5s")
                await asyncio.sleep(5)


class BithumbStreamer(BaseStreamer):
    WS = "wss://pubwss.bithumb.com/pub/ws"

    async def run(self):
        while True:
            try:
                async with websockets.connect(self.WS, ping_interval=20) as ws:
                    # Bithumb subscribes to each symbol separately
                    for unified, venue in self.symbol_map.items():
                        sub = {
                            "type": "orderbookdepth",
                            "symbols": [venue],
                            "tickTypes": ["30M"]  # 30-level depth
                        }
                        await ws.send(json.dumps(sub))
                    
                    print(f"[bithumb] subscribed to {list(self.symbol_map.values())}")
                    
                    async for raw in ws:
                        msg = json.loads(raw)
                        
                        if msg.get("type") != "orderbookdepth":
                            continue
                        
                        content = msg.get("content", {})
                        sym = content.get("symbol")  # BTC_KRW
                        if not sym:
                            continue
                            
                        unified = self._find_unified(sym)
                        if not unified:
                            continue
                        
                        lob = self.books[unified]
                        
                        # Bithumb sends lists of [price, quantity]
                        lob.bids = {float(p): float(q) 
                                   for p, q in content.get("bids", [])}
                        lob.asks = {float(p): float(q) 
                                   for p, q in content.get("asks", [])}
                        
                        seq = int(content.get("datetime", int(time.time()*1000)))
                        lob.last_update_id = seq
                        
                        await self.publish(
                            symbol=unified,
                            venue_sym=sym,
                            event_ts=seq/1000.0,
                            seq=seq,
                            depth=lob.snapshot(20),
                            raw=msg
                        )
                        
            except Exception as e:
                print(f"[bithumb] error {e} → reconnect in 5s")
                await asyncio.sleep(5)


class CoinoneStreamer(BaseStreamer):
    WS = "wss://stream.coinone.co.kr"

    async def run(self):
        while True:
            try:
                async with websockets.connect(self.WS, ping_interval=20) as ws:
                    # Coinone uses different subscription format
                    for unified, venue in self.symbol_map.items():
                        sub = {
                            "request_type": "SUBSCRIBE",
                            "channel": "ORDERBOOK",
                            "topic": {
                                "quote_currency": venue.split("-")[1],
                                "target_currency": venue.split("-")[0]
                            }
                        }
                        await ws.send(json.dumps(sub))
                    
                    print(f"[coinone] subscribed to {list(self.symbol_map.values())}")
                    
                    async for raw in ws:
                        msg = json.loads(raw)
                        
                        if msg.get("response_type") != "DATA":
                            continue
                        
                        data = msg.get("data", {})
                        if msg.get("channel") != "ORDERBOOK":
                            continue
                        
                        # Reconstruct symbol from topic
                        topic = msg.get("topic", {})
                        sym = f"{topic.get('target_currency')}-{topic.get('quote_currency')}"
                        
                        unified = self._find_unified(sym)
                        if not unified:
                            continue
                        
                        lob = self.books[unified]
                        
                        # Coinone sends bid/ask arrays
                        lob.bids = {float(item["price"]): float(item["qty"]) 
                                   for item in data.get("bids", [])}
                        lob.asks = {float(item["price"]): float(item["qty"]) 
                                   for item in data.get("asks", [])}
                        
                        seq = int(data.get("timestamp", int(time.time()*1000)))
                        lob.last_update_id = seq
                        
                        await self.publish(
                            symbol=unified,
                            venue_sym=sym,
                            event_ts=seq/1000.0,
                            seq=seq,
                            depth=lob.snapshot(20),
                            raw=msg
                        )
                        
            except Exception as e:
                print(f"[coinone] error {e} → reconnect in 5s")
                await asyncio.sleep(5)


class KorbitStreamer(BaseStreamer):
    WS = "wss://ws.korbit.co.kr/v1/ws"

    async def run(self):
        while True:
            try:
                async with websockets.connect(self.WS, ping_interval=20) as ws:
                    # Korbit subscription
                    sub = {
                        "type": "subscribe",
                        "channels": [f"orderbook:{venue}" for venue in self.symbol_map.values()]
                    }
                    await ws.send(json.dumps(sub))
                    print(f"[korbit] subscribed to {list(self.symbol_map.values())}")
                    
                    async for raw in ws:
                        msg = json.loads(raw)
                        
                        if not msg.get("channel", "").startswith("orderbook:"):
                            continue
                        
                        # Extract symbol from channel
                        channel = msg.get("channel", "")
                        sym = channel.replace("orderbook:", "")
                        
                        unified = self._find_unified(sym)
                        if not unified:
                            continue
                        
                        lob = self.books[unified]
                        data = msg.get("data", {})
                        
                        # Korbit sends bids/asks as arrays
                        lob.bids = {float(item[0]): float(item[1]) 
                                   for item in data.get("bids", [])}
                        lob.asks = {float(item[0]): float(item[1]) 
                                   for item in data.get("asks", [])}
                        
                        seq = int(data.get("timestamp", int(time.time()*1000)))
                        lob.last_update_id = seq
                        
                        await self.publish(
                            symbol=unified,
                            venue_sym=sym,
                            event_ts=seq/1000.0,
                            seq=seq,
                            depth=lob.snapshot(20),
                            raw=msg
                        )
                        
            except Exception as e:
                print(f"[korbit] error {e} → reconnect in 5s")
                await asyncio.sleep(5)


# ================= Data Pipeline =================
async def broadcaster(src_q: asyncio.Queue, *dst_queues):
    """Fan out from source queue to multiple destination queues"""
    while True:
        ev = await src_q.get()
        for q in dst_queues:
            await q.put(ev)
        src_q.task_done()


async def realtime_consumer(q: asyncio.Queue):
    """Print realtime updates (filtered)"""
    last_seen = {}
    
    while True:
        ev = await q.get()
        ex = ev["exchange"]
        sym = ev["symbol"]
        
        # Skip high-frequency exchanges for console output
        if ex in ["binance", "okx"]:
            q.task_done()
            continue
        
        now = ev["recv_ts"]
        prev = last_seen.get((ex, sym), now)
        dt = now - prev
        last_seen[(ex, sym)] = now
        
        depth = ev["depth"]
        bid = depth["bids"][0][0] if depth["bids"] else None
        ask = depth["asks"][0][0] if depth["asks"] else None
        
        if bid and ask:
            print(f"[{ex.upper()}:{sym}] Δt={dt:.3f}s bid={bid:.2f} ask={ask:.2f}")
        q.task_done()


async def file_consumer(q: asyncio.Queue):
    """Store raw events to JSONL file"""
    with OUTPUT_FILE.open("a", encoding="utf-8") as f:
        while True:
            ev = await q.get()
            f.write(json.dumps(ev, ensure_ascii=False) + "\n")
            f.flush()
            q.task_done()


async def batcher(src_q: asyncio.Queue, dst_q: asyncio.Queue, 
                  batch_size: int = 100, max_wait: float = 0.5):
    """Batch events for efficient DB writes"""
    buf = []
    last_flush = time.time()
    
    while True:
        try:
            ev = await asyncio.wait_for(src_q.get(), timeout=max_wait)
            buf.append(ev)
            src_q.task_done()
        except asyncio.TimeoutError:
            pass
        
        now = time.time()
        should_flush = False
        
        if len(buf) >= batch_size:
            should_flush = True
        elif buf and (now - last_flush) >= max_wait:
            should_flush = True
        
        if should_flush and buf:
            await dst_q.put(buf)
            buf = []
            last_flush = now


async def questdb_consumer(batch_q: asyncio.Queue, 
                          conf: str = "http::addr=localhost:9000;"):
    """
    Write to QuestDB with proper schema - EXACTLY LIKE ORIGINAL WORKING CODE
    - Symbols (tags): exchange, symbol, base, quote, region, venue_type, instance
    - Fields: seq, best_bid, best_ask, mid_price, spread_bps, recv_ts_s, depth_json, raw_json
    - Timestamp: event_ts in nanoseconds (using simple int conversion)
    """
    def _ns(sec: float) -> int:
        return int(sec * 1_000_000_000)
    
    with Sender.from_conf(conf) as sender:
        while True:
            batch = await batch_q.get()
            
            for ev in batch:
                try:
                    ex = ev["exchange"]
                    unified_sym = ev["symbol"]
                    venue_sym = ev.get("venue_symbol", unified_sym)
                    
                    # Parse symbol components
                    base, quote, normalized = split_symbol(ex, venue_sym)
                    region = get_region(ex, quote)
                    
                    # Extract best prices
                    depth = ev["depth"]
                    bids = depth["bids"]
                    asks = depth["asks"]
                    
                    best_bid = float(bids[0][0]) if bids else None
                    best_ask = float(asks[0][0]) if asks else None
                    
                    # Calculate derived metrics
                    mid_price = None
                    spread_bps = None
                    if best_bid and best_ask:
                        mid_price = (best_bid + best_ask) / 2
                        spread_bps = ((best_ask - best_bid) / mid_price) * 10000
                    
                    # Write row to QuestDB - EXACTLY LIKE ORIGINAL
                    sender.row(
                        "orderbook",
                        symbols={
                            "exchange": ex,
                            "symbol": normalized,
                            "base": base,
                            "quote": quote,
                            "region": region,
                            "venue_type": "SPOT",
                            "instance": "A",  # Can be A/B for redundancy
                        },
                        columns={
                            "seq": int(ev["seq"]),
                            "best_bid": best_bid,
                            "best_ask": best_ask,
                            "mid_price": mid_price,
                            "spread_bps": spread_bps,
                            "recv_ts_s": float(ev["recv_ts"]),
                            "depth_json": json.dumps(depth, ensure_ascii=False),
                            "raw_json": json.dumps(ev.get("raw", {}), ensure_ascii=False),
                        },
                        at=ServerTimestamp #_ns(ev["event_ts"])  # Use simple int conversion like original
                    )
                    
                except Exception as e:
                    print(f"[questdb] Error processing event: {e}")
                    continue
            
            sender.flush()
            batch_q.task_done()
            print(f"[questdb] Flushed {len(batch)} events")


# ================= Main =================
async def main():
    # Create queues
    central_q = asyncio.Queue()
    realtime_q = asyncio.Queue()
    file_q = asyncio.Queue()
    db_batch_q = asyncio.Queue()
    db_staging_q = asyncio.Queue()
    
    # Symbol mappings
    # Global exchanges (USDT pairs normalized to USD)
    binance_map = {
        "BTC-USD": "btcusdt",
        "ETH-USD": "ethusdt",
        "SOL-USD": "solusdt",
        "XRP-USD": "xrpusdt",
        "ADA-USD": "adausdt",
        "DOGE-USD": "dogeusdt",  # Added DOGE
    }
    
    okx_map = {
        "BTC-USD": "BTC-USDT",
        "ETH-USD": "ETH-USDT",
        "SOL-USD": "SOL-USDT",
        "XRP-USD": "XRP-USDT",
        "ADA-USD": "ADA-USDT",
        "DOGE-USD": "DOGE-USDT",  # Added DOGE
    }
    
    bybit_map = {
        "BTC-USD": "BTCUSDT",
        "ETH-USD": "ETHUSDT",
        "SOL-USD": "SOLUSDT",
        "XRP-USD": "XRPUSDT",
        "ADA-USD": "ADAUSDT",
        "DOGE-USD": "DOGEUSDT",  # Added DOGE
    }
    
    # Korean exchanges (KRW pairs)
    upbit_map = {
        "BTC-KRW": "KRW-BTC",
        "ETH-KRW": "KRW-ETH",
        "SOL-KRW": "KRW-SOL",
        "XRP-KRW": "KRW-XRP",
        "ADA-KRW": "KRW-ADA",
        "DOGE-KRW": "KRW-DOGE",  # Added DOGE
    }
    
    bithumb_map = {
        "BTC-KRW": "BTC_KRW",
        "ETH-KRW": "ETH_KRW",
        "SOL-KRW": "SOL_KRW",
        "XRP-KRW": "XRP_KRW",
        "ADA-KRW": "ADA_KRW",
        "DOGE-KRW": "DOGE_KRW",  # Added DOGE
    }
    
    coinone_map = {
        "BTC-KRW": "btc-krw",
        "ETH-KRW": "eth-krw",
        "SOL-KRW": "sol-krw",
        "XRP-KRW": "xrp-krw",
        "ADA-KRW": "ada-krw",
        "DOGE-KRW": "doge-krw",  # Added DOGE
    }
    
    korbit_map = {
        "BTC-KRW": "btc_krw",
        "ETH-KRW": "eth_krw",
        "SOL-KRW": "sol_krw",
        "XRP-KRW": "xrp_krw",
        "ADA-KRW": "ada_krw",
        "DOGE-KRW": "doge_krw",  # Added DOGE
    }
    
    # Create streamers - ALL exchanges included
    streamers = [
        # Global exchanges
        BinanceStreamer("binance", binance_map, central_q),
        OKXStreamer("okx", okx_map, central_q),
        BybitStreamer("bybit", bybit_map, central_q),
        
        # Korean exchanges - ALL included
        UpbitStreamer("upbit", upbit_map, central_q),
        BithumbStreamer("bithumb", bithumb_map, central_q),
        CoinoneStreamer("coinone", coinone_map, central_q),
        KorbitStreamer("korbit", korbit_map, central_q),
    ]
    
    # Create tasks
    tasks = [
        # Run all streamers
        *[asyncio.create_task(s.run()) for s in streamers],
        
        # Fan out to consumers
        asyncio.create_task(broadcaster(central_q, realtime_q, file_q, db_staging_q)),
        
        # Consumers
        asyncio.create_task(realtime_consumer(realtime_q)),
        asyncio.create_task(file_consumer(file_q)),
        
        # Batch and write to QuestDB
        asyncio.create_task(batcher(db_staging_q, db_batch_q)),
        asyncio.create_task(questdb_consumer(db_batch_q)),
    ]
    
    print("=" * 60)
    print("Unified Orderbook Streamer Started (FIXED VERSION)")
    print("=" * 60)
    print(f"Exchanges: {[s.exchange for s in streamers]}")
    print(f"Symbols: BTC, ETH, SOL, XRP, ADA, DOGE")
    print(f"Pairs: USD (Global) and KRW (Korean)")
    print(f"Output: {OUTPUT_FILE}")
    print("QuestDB: http://localhost:9000")
    print("=" * 60)
    
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStreamer stopped by user")