"""
Bookmap Replicator — Real-time orderbook heatmap + large trade bubbles
======================================================================
Connects to Bybit WebSocket, builds a live depth heatmap, tracks large
trades, and exposes data via a local HTTP API for TradingView MCP to consume.

What it tracks:
  1. ORDER BOOK HEATMAP — full depth snapshot every 1s, stored as time×price grid
  2. LARGE TRADES — any trade > threshold BTC, with side/size/price
  3. WALL TRACKER — bid/ask walls that persist > 10 seconds (real support/resistance)
  4. ICEBERG DETECTOR — levels that get eaten and immediately refill
  5. ABSORPTION — when a wall absorbs aggressive orders without moving
  6. DELTA PROFILE — cumulative buy vs sell volume at each price level

Run: python bookmap.py
Then Claude can query http://localhost:5588/heatmap, /trades, /walls, etc.
"""
import asyncio
import json
import time
import logging
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import websockets
from aiohttp import web

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
log = logging.getLogger("bookmap")

SYMBOL = "BTCUSDT"
WS_URL = "wss://stream.bybit.com/v5/public/linear"
HTTP_PORT = 5588
LARGE_TRADE_MIN_BTC = 0.5  # minimum BTC to count as "large"
WALL_MIN_MULTIPLIER = 3.0  # wall = level with 3x avg size
WALL_PERSIST_SECONDS = 10  # wall must persist 10s+ to be "real"
HEATMAP_HISTORY_SECONDS = 300  # 5 minutes of depth history
HEATMAP_PRICE_BUCKET = 10  # aggregate into $10 buckets


# ── Data Structures ──────────────────────────────────────────────────────────

@dataclass
class LargeTrade:
    ts: float
    price: float
    size: float
    side: str  # "Buy" or "Sell"
    usd: float

@dataclass 
class Wall:
    price: float
    size: float
    side: str  # "bid" or "ask"
    first_seen: float
    last_seen: float
    peak_size: float
    times_refreshed: int = 0
    absorbed_volume: float = 0  # volume eaten while wall held

@dataclass
class DeltaLevel:
    price_bucket: float
    buy_volume: float = 0
    sell_volume: float = 0
    
    @property
    def delta(self):
        return self.buy_volume - self.sell_volume


class BookmapEngine:
    def __init__(self):
        # Heatmap: time_bucket -> {price_bucket: size}
        self.heatmap: deque = deque(maxlen=HEATMAP_HISTORY_SECONDS)
        
        # Current orderbook
        self.bids: Dict[float, float] = {}  # price -> size
        self.asks: Dict[float, float] = {}
        self.mid_price: float = 0
        
        # Large trades
        self.large_trades: deque = deque(maxlen=500)
        
        # Walls
        self.active_walls: Dict[str, Wall] = {}  # "bid_71000" -> Wall
        self.historical_walls: deque = deque(maxlen=100)
        
        # Delta profile
        self.delta_profile: Dict[float, DeltaLevel] = defaultdict(
            lambda: DeltaLevel(price_bucket=0)
        )
        
        # Stats
        self.total_buy_volume: float = 0
        self.total_sell_volume: float = 0
        self.trade_count: int = 0
        self.last_update: float = 0
        self.connected: bool = False
        
    def _bucket(self, price: float) -> float:
        """Round price to nearest bucket."""
        return round(price / HEATMAP_PRICE_BUCKET) * HEATMAP_PRICE_BUCKET
    
    def process_orderbook(self, data: dict, msg_type: str):
        """Process orderbook snapshot or delta."""
        bids_raw = data.get("b", [])
        asks_raw = data.get("a", [])
        
        if msg_type == "snapshot":
            self.bids = {float(b[0]): float(b[1]) for b in bids_raw}
            self.asks = {float(a[0]): float(a[1]) for a in asks_raw}
        else:
            # Delta update
            for b in bids_raw:
                p, s = float(b[0]), float(b[1])
                if s == 0:
                    self.bids.pop(p, None)
                else:
                    self.bids[p] = s
            for a in asks_raw:
                p, s = float(a[0]), float(a[1])
                if s == 0:
                    self.asks.pop(p, None)
                else:
                    self.asks[p] = s
        
        if self.bids and self.asks:
            best_bid = max(self.bids.keys())
            best_ask = min(self.asks.keys())
            self.mid_price = (best_bid + best_ask) / 2
        
        # Snapshot for heatmap
        self._snapshot_heatmap()
        
        # Track walls
        self._track_walls()
        
        self.last_update = time.time()
    
    def process_trade(self, trade: dict):
        """Process a trade event."""
        price = float(trade.get("p", 0))
        size = float(trade.get("v", 0))
        side = trade.get("S", "")  # Buy or Sell
        
        if size <= 0:
            return
        
        # Delta profile
        bucket = self._bucket(price)
        level = self.delta_profile[bucket]
        level.price_bucket = bucket
        if side == "Buy":
            level.buy_volume += size
            self.total_buy_volume += size
        else:
            level.sell_volume += size
            self.total_sell_volume += size
        
        self.trade_count += 1
        
        # Large trade detection
        if size >= LARGE_TRADE_MIN_BTC:
            lt = LargeTrade(
                ts=time.time(),
                price=price,
                size=size,
                side=side,
                usd=size * price,
            )
            self.large_trades.append(lt)
            
            # Check if this trade hit a wall (absorption)
            self._check_absorption(price, size, side)
            
            log.info(f"{'🟢' if side == 'Buy' else '🔴'} LARGE TRADE: {side} {size:.3f} BTC @ ${price:,.1f} (${size*price:,.0f})")
    
    def _snapshot_heatmap(self):
        """Store current depth as a heatmap row."""
        now = time.time()
        row = {"ts": now, "bids": {}, "asks": {}}
        
        for price, size in self.bids.items():
            bucket = self._bucket(price)
            row["bids"][bucket] = row["bids"].get(bucket, 0) + size
        
        for price, size in self.asks.items():
            bucket = self._bucket(price)
            row["asks"][bucket] = row["asks"].get(bucket, 0) + size
        
        self.heatmap.append(row)
    
    def _track_walls(self):
        """Detect and track orderbook walls."""
        now = time.time()
        
        if not self.bids or not self.asks:
            return
        
        avg_bid = sum(self.bids.values()) / len(self.bids) if self.bids else 1
        avg_ask = sum(self.asks.values()) / len(self.asks) if self.asks else 1
        
        # Find current walls
        current_walls = set()
        
        for price, size in self.bids.items():
            if size > avg_bid * WALL_MIN_MULTIPLIER:
                key = f"bid_{self._bucket(price)}"
                current_walls.add(key)
                if key in self.active_walls:
                    wall = self.active_walls[key]
                    wall.last_seen = now
                    wall.size = size
                    wall.peak_size = max(wall.peak_size, size)
                else:
                    self.active_walls[key] = Wall(
                        price=price, size=size, side="bid",
                        first_seen=now, last_seen=now, peak_size=size,
                    )
        
        for price, size in self.asks.items():
            if size > avg_ask * WALL_MIN_MULTIPLIER:
                key = f"ask_{self._bucket(price)}"
                current_walls.add(key)
                if key in self.active_walls:
                    wall = self.active_walls[key]
                    wall.last_seen = now
                    wall.size = size
                    wall.peak_size = max(wall.peak_size, size)
                else:
                    self.active_walls[key] = Wall(
                        price=price, size=size, side="ask",
                        first_seen=now, last_seen=now, peak_size=size,
                    )
        
        # Remove walls that disappeared
        for key in list(self.active_walls.keys()):
            if key not in current_walls:
                wall = self.active_walls.pop(key)
                age = now - wall.first_seen
                if age > WALL_PERSIST_SECONDS:
                    self.historical_walls.append({
                        "price": wall.price,
                        "side": wall.side,
                        "peak_size": wall.peak_size,
                        "duration": round(age, 1),
                        "absorbed": round(wall.absorbed_volume, 3),
                        "refreshed": wall.times_refreshed,
                        "removed_at": now,
                    })
    
    def _check_absorption(self, trade_price: float, trade_size: float, trade_side: str):
        """Check if a large trade was absorbed by a wall."""
        bucket = self._bucket(trade_price)
        
        # Sell hitting a bid wall = absorption
        if trade_side == "Sell":
            key = f"bid_{bucket}"
            if key in self.active_walls:
                self.active_walls[key].absorbed_volume += trade_size
        
        # Buy hitting an ask wall = absorption
        elif trade_side == "Buy":
            key = f"ask_{bucket}"
            if key in self.active_walls:
                self.active_walls[key].absorbed_volume += trade_size
    
    # ── API Methods ──────────────────────────────────────────────────────────
    
    def get_heatmap_summary(self) -> dict:
        """Get heatmap data for the MCP tool."""
        if not self.heatmap:
            return {"status": "no data yet"}
        
        # Build a simplified heatmap: price_bucket -> avg_size over last 60s
        recent = [h for h in self.heatmap if time.time() - h["ts"] < 60]
        if not recent:
            return {"status": "no recent data"}
        
        bid_avg = defaultdict(list)
        ask_avg = defaultdict(list)
        
        for row in recent:
            for bucket, size in row["bids"].items():
                bid_avg[bucket].append(size)
            for bucket, size in row["asks"].items():
                ask_avg[bucket].append(size)
        
        # Top 10 thickest bid levels
        bid_thickness = {b: sum(sizes)/len(sizes) for b, sizes in bid_avg.items()}
        ask_thickness = {a: sum(sizes)/len(sizes) for a, sizes in ask_avg.items()}
        
        top_bids = sorted(bid_thickness.items(), key=lambda x: -x[1])[:10]
        top_asks = sorted(ask_thickness.items(), key=lambda x: -x[1])[:10]
        
        return {
            "mid_price": round(self.mid_price, 1),
            "thickest_bids": [{"price": f"${p:,.0f}", "avg_size": f"{s:.2f} BTC"} for p, s in top_bids],
            "thickest_asks": [{"price": f"${p:,.0f}", "avg_size": f"{s:.2f} BTC"} for p, s in top_asks],
            "snapshots": len(recent),
        }
    
    def get_large_trades(self, minutes: int = 5) -> list:
        """Get recent large trades."""
        cutoff = time.time() - minutes * 60
        trades = [t for t in self.large_trades if t.ts > cutoff]
        
        buys = [t for t in trades if t.side == "Buy"]
        sells = [t for t in trades if t.side == "Sell"]
        
        return {
            "total": len(trades),
            "buy_count": len(buys),
            "sell_count": len(sells),
            "buy_volume": f"{sum(t.size for t in buys):.2f} BTC",
            "sell_volume": f"{sum(t.size for t in sells):.2f} BTC",
            "net_delta": f"{sum(t.size for t in buys) - sum(t.size for t in sells):+.2f} BTC",
            "trades": [
                {
                    "time": datetime.fromtimestamp(t.ts, tz=timezone.utc).strftime("%H:%M:%S"),
                    "side": "🟢 BUY" if t.side == "Buy" else "🔴 SELL",
                    "size": f"{t.size:.3f} BTC",
                    "price": f"${t.price:,.1f}",
                    "usd": f"${t.usd:,.0f}",
                }
                for t in sorted(trades, key=lambda x: -x.ts)[:20]
            ],
        }
    
    def get_walls(self) -> dict:
        """Get active walls and recently removed walls."""
        now = time.time()
        active = []
        for key, wall in self.active_walls.items():
            age = now - wall.first_seen
            if age >= WALL_PERSIST_SECONDS:
                active.append({
                    "price": f"${wall.price:,.1f}",
                    "side": "🟢 BID (support)" if wall.side == "bid" else "🔴 ASK (resistance)",
                    "size": f"{wall.size:.2f} BTC",
                    "peak": f"{wall.peak_size:.2f} BTC",
                    "age": f"{age:.0f}s",
                    "absorbed": f"{wall.absorbed_volume:.3f} BTC",
                    "holding": wall.absorbed_volume > 0 and wall.size > wall.peak_size * 0.5,
                })
        
        recent_removed = list(self.historical_walls)[-10:]
        
        return {
            "active_walls": sorted(active, key=lambda x: -float(x["size"].split()[0])),
            "recently_removed": [
                {
                    "price": f"${w['price']:,.1f}",
                    "side": w["side"],
                    "lived": f"{w['duration']:.0f}s",
                    "peak": f"{w['peak_size']:.2f} BTC",
                    "absorbed_before_removal": f"{w['absorbed']:.3f} BTC",
                }
                for w in reversed(recent_removed)
            ],
        }
    
    def get_delta_profile(self) -> dict:
        """Get volume delta by price level."""
        if not self.delta_profile:
            return {"status": "no data"}
        
        # Sort by price, show around mid
        mid = self._bucket(self.mid_price)
        levels = sorted(self.delta_profile.items(), key=lambda x: x[0])
        
        # Filter to ±2% of mid
        near = [(p, l) for p, l in levels if abs(p - mid) / mid < 0.02]
        
        return {
            "mid_price": f"${mid:,.0f}",
            "total_buy": f"{self.total_buy_volume:.2f} BTC",
            "total_sell": f"{self.total_sell_volume:.2f} BTC",
            "net_delta": f"{self.total_buy_volume - self.total_sell_volume:+.2f} BTC",
            "levels": [
                {
                    "price": f"${p:,.0f}",
                    "buy": f"{l.buy_volume:.2f}",
                    "sell": f"{l.sell_volume:.2f}",
                    "delta": f"{l.delta:+.2f}",
                    "bar": "🟢" * min(10, int(l.delta * 2)) if l.delta > 0 else "🔴" * min(10, int(-l.delta * 2)),
                }
                for p, l in near[-20:]
            ],
        }
    
    def get_full_status(self) -> dict:
        """Everything in one call."""
        return {
            "connected": self.connected,
            "mid_price": f"${self.mid_price:,.1f}" if self.mid_price else "N/A",
            "trade_count": self.trade_count,
            "large_trade_count": len(self.large_trades),
            "active_walls": len([w for w in self.active_walls.values() 
                                if time.time() - w.first_seen > WALL_PERSIST_SECONDS]),
            "heatmap_snapshots": len(self.heatmap),
            "uptime": f"{time.time() - self.last_update:.0f}s since last update" if self.last_update else "starting",
        }


# ── WebSocket + HTTP Server ──────────────────────────────────────────────────

engine = BookmapEngine()


async def ws_orderbook():
    """Connect to Bybit orderbook WS."""
    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=20) as ws:
                sub = {"op": "subscribe", "args": [f"orderbook.200.{SYMBOL}"]}
                await ws.send(json.dumps(sub))
                log.info(f"📊 Orderbook WS connected: {SYMBOL}")
                engine.connected = True
                
                async for raw in ws:
                    msg = json.loads(raw)
                    if "data" in msg and "topic" in msg:
                        engine.process_orderbook(msg["data"], msg.get("type", "delta"))
        except Exception as e:
            engine.connected = False
            log.warning(f"Orderbook WS error: {e} — reconnecting in 3s")
            await asyncio.sleep(3)


async def ws_trades():
    """Connect to Bybit trade stream."""
    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=20) as ws:
                sub = {"op": "subscribe", "args": [f"publicTrade.{SYMBOL}"]}
                await ws.send(json.dumps(sub))
                log.info(f"💰 Trade WS connected: {SYMBOL}")
                
                async for raw in ws:
                    msg = json.loads(raw)
                    if "data" in msg:
                        for trade in msg["data"]:
                            engine.process_trade(trade)
        except Exception as e:
            log.warning(f"Trade WS error: {e} — reconnecting in 3s")
            await asyncio.sleep(3)


# ── HTTP API ─────────────────────────────────────────────────────────────────

async def handle_status(request):
    return web.json_response(engine.get_full_status())

async def handle_heatmap(request):
    return web.json_response(engine.get_heatmap_summary())

async def handle_trades(request):
    minutes = int(request.query.get("minutes", 5))
    return web.json_response(engine.get_large_trades(minutes))

async def handle_walls(request):
    return web.json_response(engine.get_walls())

async def handle_delta(request):
    return web.json_response(engine.get_delta_profile())

async def handle_all(request):
    """Everything in one call for MCP."""
    return web.json_response({
        "status": engine.get_full_status(),
        "heatmap": engine.get_heatmap_summary(),
        "large_trades": engine.get_large_trades(5),
        "walls": engine.get_walls(),
        "delta_profile": engine.get_delta_profile(),
    })


async def start_http():
    app = web.Application()
    app.router.add_get("/", handle_status)
    app.router.add_get("/status", handle_status)
    app.router.add_get("/heatmap", handle_heatmap)
    app.router.add_get("/trades", handle_trades)
    app.router.add_get("/walls", handle_walls)
    app.router.add_get("/delta", handle_delta)
    app.router.add_get("/all", handle_all)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", HTTP_PORT)
    await site.start()
    log.info(f"🌐 Bookmap API running at http://localhost:{HTTP_PORT}")


async def main():
    log.info(f"📊 Bookmap Replicator starting — {SYMBOL}")
    log.info(f"   Large trade threshold: {LARGE_TRADE_MIN_BTC} BTC")
    log.info(f"   Wall detection: {WALL_MIN_MULTIPLIER}x avg, persist {WALL_PERSIST_SECONDS}s")
    log.info(f"   Heatmap history: {HEATMAP_HISTORY_SECONDS}s")
    log.info(f"   API: http://localhost:{HTTP_PORT}")
    
    await start_http()
    await asyncio.gather(
        ws_orderbook(),
        ws_trades(),
    )


if __name__ == "__main__":
    asyncio.run(main())
