"""
Bookmap → File Bridge (no threading, no HTTP)
==============================================
Runs INSIDE Bookmap's Python API. Writes live data to a JSON file
every second. The MCP server reads from the file.

No threads, no HTTP, no sockets — just file writes. 
Bookmap-compatible.

Output: ~/.bookmap_live.json (updated every second)
"""
import bookmap as bm
import json
import time
import os
from collections import defaultdict

# Output file — MCP reads this
OUTPUT_FILE = os.path.expanduser("~/.bookmap_live.json")
LARGE_TRADE_MIN = 0.5

# State
bids = {}
asks = {}
mid_price = 0
pips = 1
trades = []
large_trades = []
walls_bid = {}
walls_ask = {}
delta_buy = 0
delta_sell = 0
delta_by_price = defaultdict(lambda: {"buy": 0, "sell": 0})
symbol = ""
last_write = 0
trade_count = 0


def on_subscribe(addon, alias, full_name, is_crypto, _pips, size_multiplier,
                 instrument_multiplier, supported_features):
    global symbol, pips
    symbol = alias
    pips = _pips
    print(f"[BRIDGE] Subscribed: {alias} (pips={_pips})", flush=True)
    
    bm.subscribe_to_depth(addon, on_depth)
    bm.subscribe_to_trades(addon, on_trade)


def on_unsubscribe(addon, alias):
    global symbol
    symbol = ""
    print(f"[BRIDGE] Unsubscribed: {alias}", flush=True)


def on_depth(addon, alias, is_bid, price, size):
    global mid_price, last_write
    
    book = bids if is_bid else asks
    if size == 0:
        book.pop(price, None)
    else:
        book[price] = size
    
    if bids and asks:
        best_bid = max(bids.keys())
        best_ask = min(asks.keys())
        mid_price = (best_bid + best_ask) / 2
    
    # Write to file every 1 second
    now = time.time()
    if now - last_write >= 1.0:
        _write_state()
        last_write = now


def on_trade(addon, alias, is_bid, price, size):
    global delta_buy, delta_sell, trade_count
    
    side = "Buy" if is_bid else "Sell"
    now = time.time()
    trade_count += 1
    
    # Delta
    if is_bid:
        delta_buy += size
    else:
        delta_sell += size
    
    bucket = round(price / 10) * 10
    delta_by_price[bucket][side.lower()] = delta_by_price[bucket].get(side.lower(), 0) + size
    
    # Large trade
    if size >= LARGE_TRADE_MIN:
        large_trades.append({
            "ts": now,
            "price": price,
            "size": round(size, 4),
            "side": side,
            "usd": round(size * price, 0),
        })
        # Keep last 200
        if len(large_trades) > 200:
            del large_trades[0]
        
        emoji = "🟢" if is_bid else "🔴"
        print(f"[BRIDGE] {emoji} {side} {size:.3f} @ ${price:,.1f}", flush=True)


def _write_state():
    """Write current state to JSON file."""
    try:
        # Find walls (3x average)
        avg_bid = sum(bids.values()) / len(bids) if bids else 1
        avg_ask = sum(asks.values()) / len(asks) if asks else 1
        
        bid_walls = [{"price": p, "size": round(s, 3)} 
                     for p, s in sorted(bids.items(), key=lambda x: -x[1])[:10]
                     if s > avg_bid * 3]
        ask_walls = [{"price": p, "size": round(s, 3)}
                     for p, s in sorted(asks.items(), key=lambda x: -x[1])[:10]
                     if s > avg_ask * 3]
        
        # Top depth levels
        top_bids = [{"price": p, "size": round(s, 3)}
                    for p, s in sorted(bids.items(), key=lambda x: -x[1])[:15]]
        top_asks = [{"price": p, "size": round(s, 3)}
                    for p, s in sorted(asks.items(), key=lambda x: -x[1])[:15]]
        
        # Recent large trades (last 5 min)
        cutoff = time.time() - 300
        recent_large = [t for t in large_trades if t["ts"] > cutoff]
        buys = [t for t in recent_large if t["side"] == "Buy"]
        sells = [t for t in recent_large if t["side"] == "Sell"]
        
        # Delta profile near mid
        mid_bucket = round(mid_price / 10) * 10
        delta_levels = []
        for p in sorted(delta_by_price.keys()):
            if abs(p - mid_bucket) / max(mid_bucket, 1) < 0.02:
                d = delta_by_price[p]
                delta_levels.append({
                    "price": p,
                    "buy": round(d.get("buy", 0), 3),
                    "sell": round(d.get("sell", 0), 3),
                    "delta": round(d.get("buy", 0) - d.get("sell", 0), 3),
                })
        
        data = {
            "source": "Bookmap LIVE",
            "symbol": symbol,
            "mid_price": round(mid_price, 1),
            "updated": time.time(),
            "trade_count": trade_count,
            
            "depth": {
                "thickest_bids": top_bids[:10],
                "thickest_asks": top_asks[:10],
                "bid_levels": len(bids),
                "ask_levels": len(asks),
            },
            
            "walls": {
                "bid_walls": bid_walls[:5],
                "ask_walls": ask_walls[:5],
                "total_bid_walls": len(bid_walls),
                "total_ask_walls": len(ask_walls),
            },
            
            "large_trades": {
                "total": len(recent_large),
                "buy_count": len(buys),
                "sell_count": len(sells),
                "buy_volume": round(sum(t["size"] for t in buys), 3),
                "sell_volume": round(sum(t["size"] for t in sells), 3),
                "net_delta": round(sum(t["size"] for t in buys) - sum(t["size"] for t in sells), 3),
                "recent": recent_large[-15:],
            },
            
            "delta": {
                "total_buy": round(delta_buy, 3),
                "total_sell": round(delta_sell, 3),
                "net": round(delta_buy - delta_sell, 3),
                "levels": delta_levels[-20:],
            },
        }
        
        # Atomic write
        tmp = OUTPUT_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(data, f)
        os.replace(tmp, OUTPUT_FILE)
        
    except Exception as e:
        print(f"[BRIDGE] Write error: {e}", flush=True)


if __name__ == "__main__":
    print(f"[BRIDGE] Bookmap File Bridge starting...", flush=True)
    print(f"[BRIDGE] Output: {OUTPUT_FILE}", flush=True)
    
    addon = bm.create_addon()
    bm.start_addon(addon, on_subscribe, on_unsubscribe)
