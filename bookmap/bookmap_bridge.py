"""
Bookmap → Claude MCP Bridge
============================
Runs as a Bookmap Python API addon. Receives real-time orderbook
depth and trade data from Bookmap, serves it via HTTP on localhost:5588
for the TradingView MCP server to consume.

Install:
  1. In Bookmap: Settings → API Plugins Configuration → Add
  2. Select this file (bookmap_bridge.py)
  3. Enable it
  4. Claude MCP reads from http://localhost:5588
"""
import sys
import os
# Ensure we import the bookmap PACKAGE, not our local bookmap.py
sys.path = [p for p in sys.path if os.path.basename(p) != 'bookmap']
import bookmap as bm
import threading
import json
import time
from collections import defaultdict, deque
from http.server import HTTPServer, BaseHTTPRequestHandler

# ── Config ────────────────────────────────────────────────────────────────────
HTTP_PORT = 5588
LARGE_TRADE_MIN = 0.5  # BTC
WALL_MULTIPLIER = 3.0  # 3x avg = wall
HISTORY_SIZE = 500

# ── Shared State ──────────────────────────────────────────────────────────────
state = {
    "bids": {},           # price -> size
    "asks": {},           # price -> size
    "mid_price": 0,
    "trades": deque(maxlen=HISTORY_SIZE),
    "large_trades": deque(maxlen=200),
    "walls": {},          # "bid_71000" -> {price, size, side, first_seen, absorbed}
    "delta_buy": 0,
    "delta_sell": 0,
    "delta_by_price": defaultdict(lambda: {"buy": 0, "sell": 0}),
    "connected": False,
    "symbol": "",
    "last_update": 0,
    "depth_snapshots": deque(maxlen=300),
}
state_lock = threading.Lock()


# ── Bookmap Callbacks ─────────────────────────────────────────────────────────

def on_subscribe(addon, alias, full_name, is_crypto, pips, size_multiplier, 
                  instrument_multiplier, supported_features):
    """Called when instrument is subscribed."""
    with state_lock:
        state["symbol"] = alias
        state["connected"] = True
        state["pips"] = pips
    print(f"[BRIDGE] Subscribed: {alias} ({full_name})")
    
    # Register event handlers
    bm.subscribe_to_depth(addon, on_depth_handler)
    bm.subscribe_to_trades(addon, on_trade_handler)


def on_unsubscribe(addon, alias):
    """Called when instrument is unsubscribed."""
    with state_lock:
        state["connected"] = False
    print(f"[BRIDGE] Unsubscribed: {alias}")


def on_depth_handler(addon, alias, is_bid, price, size):
    """Called on every orderbook update."""
    with state_lock:
        book = state["bids"] if is_bid else state["asks"]
        if size == 0:
            book.pop(price, None)
        else:
            book[price] = size
        
        if state["bids"] and state["asks"]:
            best_bid = max(state["bids"].keys())
            best_ask = min(state["asks"].keys())
            state["mid_price"] = (best_bid + best_ask) / 2
        
        state["last_update"] = time.time()
        
        # Track walls
        _update_walls()
        
        # Periodic depth snapshot (every ~1 second)
        snapshots = state["depth_snapshots"]
        if not snapshots or time.time() - snapshots[-1]["ts"] >= 1.0:
            snap = {"ts": time.time(), "bids": {}, "asks": {}}
            for p, s in list(state["bids"].items())[:50]:
                snap["bids"][p] = s
            for p, s in list(state["asks"].items())[:50]:
                snap["asks"][p] = s
            snapshots.append(snap)


def on_trade_handler(addon, alias, is_bid, price, size):
    """Called on every trade."""
    side = "Buy" if is_bid else "Sell"
    now = time.time()
    
    with state_lock:
        trade = {
            "ts": now,
            "price": price,
            "size": size,
            "side": side,
            "usd": size * price,
        }
        state["trades"].append(trade)
        
        # Delta tracking
        if is_bid:
            state["delta_buy"] += size
        else:
            state["delta_sell"] += size
        
        bucket = round(price / 10) * 10
        state["delta_by_price"][bucket][side.lower()] = (
            state["delta_by_price"][bucket].get(side.lower(), 0) + size
        )
        
        # Large trade detection
        if size >= LARGE_TRADE_MIN:
            state["large_trades"].append(trade)
            emoji = "🟢" if is_bid else "🔴"
            print(f"[BRIDGE] {emoji} LARGE: {side} {size:.3f} @ ${price:,.1f} (${size*price:,.0f})")
        
        # Check absorption
        _check_absorption(price, size, side)


def _update_walls():
    """Detect orderbook walls."""
    now = time.time()
    bids = state["bids"]
    asks = state["asks"]
    
    if not bids or not asks:
        return
    
    avg_bid = sum(bids.values()) / len(bids) if bids else 1
    avg_ask = sum(asks.values()) / len(asks) if asks else 1
    
    current = set()
    
    for p, s in bids.items():
        if s > avg_bid * WALL_MULTIPLIER:
            key = f"bid_{round(p/10)*10}"
            current.add(key)
            if key not in state["walls"]:
                state["walls"][key] = {
                    "price": p, "size": s, "side": "bid",
                    "first_seen": now, "peak": s, "absorbed": 0,
                }
            else:
                w = state["walls"][key]
                w["size"] = s
                w["peak"] = max(w["peak"], s)
    
    for p, s in asks.items():
        if s > avg_ask * WALL_MULTIPLIER:
            key = f"ask_{round(p/10)*10}"
            current.add(key)
            if key not in state["walls"]:
                state["walls"][key] = {
                    "price": p, "size": s, "side": "ask",
                    "first_seen": now, "peak": s, "absorbed": 0,
                }
            else:
                w = state["walls"][key]
                w["size"] = s
                w["peak"] = max(w["peak"], s)
    
    # Remove disappeared walls
    for key in list(state["walls"].keys()):
        if key not in current:
            del state["walls"][key]


def _check_absorption(price, size, side):
    """Check if a trade was absorbed by a wall."""
    bucket = round(price / 10) * 10
    if side == "Sell":
        key = f"bid_{bucket}"
    else:
        key = f"ask_{bucket}"
    
    if key in state["walls"]:
        state["walls"][key]["absorbed"] = state["walls"][key].get("absorbed", 0) + size


# ── HTTP Server ───────────────────────────────────────────────────────────────

class BookmapHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass  # Suppress access logs
    
    def _json(self, data):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data, default=str).encode())
    
    def do_GET(self):
        with state_lock:
            if self.path == "/status" or self.path == "/":
                self._json({
                    "connected": state["connected"],
                    "symbol": state["symbol"],
                    "mid_price": round(state["mid_price"], 1),
                    "trade_count": len(state["trades"]),
                    "large_trades": len(state["large_trades"]),
                    "active_walls": len(state["walls"]),
                    "source": "Bookmap Live (not estimated)",
                })
            
            elif self.path == "/heatmap":
                # Thickest levels from real Bookmap depth
                bids = state["bids"]
                asks = state["asks"]
                top_bids = sorted(bids.items(), key=lambda x: -x[1])[:15]
                top_asks = sorted(asks.items(), key=lambda x: -x[1])[:15]
                self._json({
                    "source": "Bookmap Live Depth",
                    "mid": round(state["mid_price"], 1),
                    "thickest_bids": [{"price": f"${p:,.1f}", "size": f"{s:.3f}"} for p, s in top_bids],
                    "thickest_asks": [{"price": f"${p:,.1f}", "size": f"{s:.3f}"} for p, s in top_asks],
                })
            
            elif self.path.startswith("/trades"):
                now = time.time()
                minutes = 5
                recent = [t for t in state["large_trades"] if now - t["ts"] < minutes * 60]
                buys = [t for t in recent if t["side"] == "Buy"]
                sells = [t for t in recent if t["side"] == "Sell"]
                self._json({
                    "source": "Bookmap Live Trades",
                    "total": len(recent),
                    "buy_count": len(buys),
                    "sell_count": len(sells),
                    "buy_volume": f"{sum(t['size'] for t in buys):.2f} BTC",
                    "sell_volume": f"{sum(t['size'] for t in sells):.2f} BTC",
                    "net_delta": f"{sum(t['size'] for t in buys) - sum(t['size'] for t in sells):+.2f} BTC",
                    "trades": [
                        {
                            "time": time.strftime("%H:%M:%S", time.gmtime(t["ts"])),
                            "side": "🟢 BUY" if t["side"] == "Buy" else "🔴 SELL",
                            "size": f"{t['size']:.3f} BTC",
                            "price": f"${t['price']:,.1f}",
                            "usd": f"${t['usd']:,.0f}",
                        }
                        for t in sorted(recent, key=lambda x: -x["ts"])[:20]
                    ],
                })
            
            elif self.path == "/walls":
                now = time.time()
                active = []
                for key, w in state["walls"].items():
                    age = now - w["first_seen"]
                    if age > 5:
                        active.append({
                            "price": f"${w['price']:,.1f}",
                            "side": "🟢 BID SUPPORT" if w["side"] == "bid" else "🔴 ASK RESISTANCE",
                            "size": f"{w['size']:.3f} BTC",
                            "peak": f"{w['peak']:.3f} BTC",
                            "age": f"{age:.0f}s",
                            "absorbed": f"{w.get('absorbed', 0):.3f} BTC",
                            "holding": w.get("absorbed", 0) > 0 and w["size"] > w["peak"] * 0.5,
                        })
                self._json({
                    "source": "Bookmap Live Walls",
                    "active": sorted(active, key=lambda x: -float(x["size"].split()[0])),
                })
            
            elif self.path == "/delta":
                mid = round(state["mid_price"] / 10) * 10
                levels = []
                for p in sorted(state["delta_by_price"].keys()):
                    if abs(p - mid) / mid < 0.02:
                        d = state["delta_by_price"][p]
                        buy = d.get("buy", 0)
                        sell = d.get("sell", 0)
                        delta = buy - sell
                        levels.append({
                            "price": f"${p:,.0f}",
                            "buy": f"{buy:.2f}",
                            "sell": f"{sell:.2f}",
                            "delta": f"{delta:+.2f}",
                        })
                self._json({
                    "source": "Bookmap Live Delta",
                    "total_buy": f"{state['delta_buy']:.2f} BTC",
                    "total_sell": f"{state['delta_sell']:.2f} BTC",
                    "net": f"{state['delta_buy'] - state['delta_sell']:+.2f} BTC",
                    "levels": levels[-20:],
                })
            
            elif self.path == "/all":
                # Everything for MCP
                self._json({
                    "source": "Bookmap Live (REAL DATA)",
                    "mid_price": round(state["mid_price"], 1),
                    "walls": len(state["walls"]),
                    "large_trades": len(state["large_trades"]),
                    "net_delta": f"{state['delta_buy'] - state['delta_sell']:+.2f} BTC",
                    "hint": "Use /heatmap, /trades, /walls, /delta for detailed views",
                })
            
            else:
                self._json({"error": "Unknown endpoint", "endpoints": ["/status", "/heatmap", "/trades", "/walls", "/delta", "/all"]})


def start_http():
    """Start HTTP server in background thread."""
    server = HTTPServer(("127.0.0.1", HTTP_PORT), BookmapHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    print(f"[BRIDGE] HTTP API running at http://localhost:{HTTP_PORT}")


# ── Bookmap Addon Entry Point ─────────────────────────────────────────────────

if __name__ == "__main__":
    print("[BRIDGE] Bookmap → Claude MCP Bridge starting...")
    start_http()
    
    addon = bm.create_addon()
    bm.start_addon(addon, on_subscribe, on_unsubscribe)
    print("[BRIDGE] Addon started — waiting for instrument subscription in Bookmap")
